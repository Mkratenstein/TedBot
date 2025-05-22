import os
import asyncio
import logging
from datetime import datetime, timedelta
import sys
from typing import Dict, Optional, List, Any
from functools import lru_cache
import time
import random as random_module
import json

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: List[float] = []

    async def acquire(self) -> None:
        now = time.time()
        # Remove old requests
        self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
        
        if len(self.requests) >= self.max_requests:
            # Wait until we can make another request
            sleep_time = self.requests[0] + self.time_window - now
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.requests.append(now)

class AsyncCache:
    def __init__(self, maxsize: int = 128):
        self.maxsize = maxsize
        self.cache: Dict[str, Any] = {}
        self.times: Dict[str, float] = {}

    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            return self.cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        if len(self.cache) >= self.maxsize:
            # Remove oldest item
            oldest_key = min(self.times.items(), key=lambda x: x[1])[0]
            del self.cache[oldest_key]
            del self.times[oldest_key]
        
        self.cache[key] = value
        self.times[key] = time.time()

    def clear(self) -> None:
        self.cache.clear()
        self.times.clear()

class GooseBandTracker(commands.Bot):
    def __init__(self, intents: discord.Intents):
        super().__init__(command_prefix='!', intents=intents)
        
        # Validate required environment variables
        self._validate_env_vars()
        
        # YouTube API setup with rate limiting
        self.youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))
        self.rate_limiter = RateLimiter(max_requests=100, time_window=60)  # 100 requests per minute
        
        # Initialize tracking variables
        self._init_tracking_vars()
        
        # Initialize caches
        self.playlist_cache = AsyncCache(maxsize=256)  # Increased cache size for better performance
        
        # Register commands
        self._register_commands()

    async def setup_hook(self) -> None:
        """Set up the bot's slash commands"""
        # Sync commands with Discord
        await self.tree.sync()

    def _validate_env_vars(self) -> None:
        """Validate required environment variables"""
        required_vars = [
            'YOUTUBE_API_KEY',
            'DISCORD_TOKEN',
            'YOUTUBE_CHANNEL_ID',
            'DISCORD_CHANNEL_ID',
            'DISCORD_RANDOM_CHANNEL_ID'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Validate YouTube API key
        youtube_api_key = os.getenv('YOUTUBE_API_KEY')
        if youtube_api_key == 'your_youtube_api_key_here':
            raise ValueError("YouTube API key not configured. Please set YOUTUBE_API_KEY in your environment variables.")
        
        # Validate YouTube channel ID format
        self.youtube_channel_id = os.getenv('YOUTUBE_CHANNEL_ID')
        if not self.youtube_channel_id.startswith('UC'):
            logger.warning(f"Warning: YouTube channel ID '{self.youtube_channel_id}' may be invalid. Channel IDs should start with 'UC'")
        
        # Set Discord channel IDs
        self.discord_channel_id = int(os.getenv('DISCORD_CHANNEL_ID'))  # For notifications
        self.discord_random_channel_id = int(os.getenv('DISCORD_RANDOM_CHANNEL_ID'))  # For random command

        # Test YouTube API connection with retries
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to validate YouTube API key (attempt {attempt + 1}/{max_retries})")
                self.youtube = build('youtube', 'v3', developerKey=youtube_api_key)
                
                # First, try to get channel details
                try:
                    channel_response = self.youtube.channels().list(
                        part='snippet,contentDetails',
                        id=self.youtube_channel_id
                    ).execute()
                    
                    if not channel_response.get('items'):
                        # If channel not found by ID, try searching by handle
                        logger.info("Channel not found by ID, trying to search by handle...")
                        search_response = self.youtube.search().list(
                            part='snippet',
                            q='@goosetheband',
                            type='channel',
                            maxResults=1
                        ).execute()
                        
                        if not search_response.get('items'):
                            error_msg = f"Could not find YouTube channel with ID: {self.youtube_channel_id} or handle @goosetheband"
                            logger.error(error_msg)
                            if attempt < max_retries - 1:
                                logger.info(f"Retrying in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                                continue
                            raise ValueError(error_msg)
                        
                        found_channel_id = search_response['items'][0]['id']['channelId']
                        if found_channel_id != self.youtube_channel_id:
                            logger.warning(f"Found channel ID {found_channel_id} differs from configured ID {self.youtube_channel_id}")
                            # Update the channel ID to the found one
                            self.youtube_channel_id = found_channel_id
                            logger.info(f"Updated channel ID to {found_channel_id}")
                    
                    channel_name = channel_response['items'][0]['snippet']['title']
                    logger.info(f"Successfully connected to YouTube channel: {channel_name}")
                    return  # Success, exit the function
                    
                except HttpError as e:
                    error_msg = f"YouTube API error: {str(e)}"
                    logger.error(error_msg)
                    if e.resp.status == 403:
                        logger.error("API key may be invalid or missing required permissions")
                        logger.error("Please ensure the YouTube Data API v3 is enabled in Google Cloud Console")
                    elif e.resp.status == 404:
                        logger.error("Channel not found. Please verify the channel ID")
                    elif e.resp.status == 429:
                        logger.error("Rate limit exceeded. Waiting before retry...")
                        time.sleep(retry_delay * 2)  # Double the delay for rate limits
                        continue
                    
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        continue
                    raise ValueError(error_msg)
                    
            except Exception as e:
                error_msg = f"Unexpected error validating YouTube API: {str(e)}"
                logger.error(error_msg)
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                raise ValueError(error_msg)
        
        raise ValueError("Failed to validate YouTube API after maximum retries")

    def _init_tracking_vars(self) -> None:
        """Initialize tracking variables with container-aware path handling"""
        # Use /data in Railway, local data directory otherwise
        base_path = '/data' if os.path.exists('/data') else 'data'
        os.makedirs(base_path, exist_ok=True)
        
        # Initialize paths for both tracking files
        self.current_tracking_file = os.path.join(base_path, 'current_tracking.json')
        self.posted_videos_file = os.path.join(base_path, 'posted_videos.json')
        
        try:
            # Load current tracking
            if os.path.exists(self.current_tracking_file):
                with open(self.current_tracking_file, 'r') as f:
                    data = json.load(f)
                    self.last_video_id = data.get('last_video_id', '')
                    self.last_livestream_id = data.get('last_livestream_id', '')
                    self.last_short_id = data.get('last_short_id', '')
                    self.last_check_time = datetime.fromisoformat(data.get('last_check_time', datetime.now().isoformat()))
                    logger.info(f"Loaded current tracking: last_video_id={self.last_video_id}, last_livestream_id={self.last_livestream_id}, last_short_id={self.last_short_id}")
            else:
                logger.info(f"No current tracking file found, starting fresh")
                self.last_video_id = ''
                self.last_livestream_id = ''
                self.last_short_id = ''
                self.last_check_time = datetime.now()
            
            # Load posted videos history
            if os.path.exists(self.posted_videos_file):
                with open(self.posted_videos_file, 'r') as f:
                    self.posted_videos = json.load(f)
                    logger.info(f"Loaded {len(self.posted_videos.get('videos', {}))} posted videos from history")
            else:
                logger.info("No posted videos history found, starting fresh")
                self.posted_videos = {'videos': {}}
                
        except Exception as e:
            logger.error(f"Error loading tracking variables: {e}")
            self.last_video_id = ''
            self.last_livestream_id = ''
            self.last_short_id = ''
            self.last_check_time = datetime.now()
            self.posted_videos = {'videos': {}}
        
        self.active_tasks: set = set()
        self.consecutive_errors: int = 0
        self.max_consecutive_errors: int = 3

    def _save_tracking_vars(self) -> None:
        """Save tracking variables to files"""
        try:
            # Save current tracking
            current_data = {
                'last_video_id': self.last_video_id,
                'last_livestream_id': self.last_livestream_id,
                'last_short_id': self.last_short_id,
                'last_check_time': self.last_check_time.isoformat()
            }
            
            # Create a temporary file first
            temp_file = f"{self.current_tracking_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(current_data, f)
            
            # Then rename it to the actual file (atomic operation)
            os.replace(temp_file, self.current_tracking_file)
            
            # Save posted videos history
            temp_file = f"{self.posted_videos_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.posted_videos, f)
            
            os.replace(temp_file, self.posted_videos_file)
            
            logger.info(f"Saved tracking variables and posted videos history")
            logger.info(f"Current tracking state: last_video_id={self.last_video_id}, last_livestream_id={self.last_livestream_id}, last_short_id={self.last_short_id}")
            logger.info(f"Total posted videos in history: {len(self.posted_videos.get('videos', {}))}")
            
        except Exception as e:
            logger.error(f"Error saving tracking variables: {e}")
            logger.error(f"Attempted to save to: {self.current_tracking_file} and {self.posted_videos_file}")
            logger.error(f"Current tracking state: last_video_id={self.last_video_id}, last_livestream_id={self.last_livestream_id}, last_short_id={self.last_short_id}")

    def _is_video_posted(self, video_id: str) -> bool:
        """Check if a video has already been posted to Discord"""
        return video_id in self.posted_videos.get('videos', {})

    def _add_posted_video(self, video_id: str, video_type: str, discord_message_id: str) -> None:
        """Add a video to the posted videos history"""
        self.posted_videos['videos'][video_id] = {
            'type': video_type,
            'posted_at': datetime.now().isoformat(),
            'discord_message_id': discord_message_id
        }

    def _cleanup_old_entries(self, max_age_days: int = 30) -> None:
        """Remove entries older than max_age_days from the posted videos history"""
        try:
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            old_entries = []
            
            for video_id, data in self.posted_videos.get('videos', {}).items():
                posted_at = datetime.fromisoformat(data['posted_at'])
                if posted_at < cutoff_date:
                    old_entries.append(video_id)
            
            for video_id in old_entries:
                del self.posted_videos['videos'][video_id]
            
            if old_entries:
                logger.info(f"Cleaned up {len(old_entries)} old entries from posting history")
                self._save_tracking_vars()
                
        except Exception as e:
            logger.error(f"Error cleaning up old entries: {e}")

    def _register_commands(self) -> None:
        """Register bot commands"""
        @self.tree.command(name="ping", description="Check if the bot is alive")
        async def ping(interaction: discord.Interaction) -> None:
            await interaction.response.send_message('Pong! Goose Youtube Tracker is alive!', ephemeral=True)
            
        @self.tree.command(name="randomyoutube", description="Get a random video from the channel")
        async def random_youtube(interaction: discord.Interaction) -> None:
            """Get a random video from the channel"""
            try:
                # Check if command is used in the correct channel
                if interaction.channel_id != self.discord_random_channel_id:
                    await interaction.response.send_message(f"This command can only be used in <#{self.discord_random_channel_id}>", ephemeral=True)
                    return

                # Defer the response since this might take a while
                await interaction.response.defer()
                
                # Get channel uploads playlist ID (cached)
                uploads_playlist_id = await self.get_uploads_playlist_id()
                
                # Get videos with rate limiting
                await self.rate_limiter.acquire()
                playlist_response = self.youtube.playlistItems().list(
                    part='snippet',
                    playlistId=uploads_playlist_id,
                    maxResults=150  # Get up to 150 videos for better randomization
                ).execute()
                
                if not playlist_response.get('items'):
                    logger.warning("No videos found in uploads playlist")
                    await interaction.followup.send("No videos found in the channel.")
                    return

                # Select a random video
                videos = playlist_response['items']
                random_video = random_module.choice(videos)
                video_id = random_video['snippet']['resourceId']['videoId']
                video_title = random_video['snippet']['title']
                
                # Create and send the embed
                embed = discord.Embed(
                    title="Random Goose Video",
                    description=f"**{video_title}**",
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    color=discord.Color.blue()
                )
                embed.set_thumbnail(url=random_video['snippet']['thumbnails']['high']['url'])
                
                await interaction.followup.send(embed=embed)
                
            except Exception as e:
                logger.error(f"Error in random_youtube command: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message("Sorry, there was an error getting a random video.", ephemeral=True)
                else:
                    await interaction.followup.send("Sorry, there was an error getting a random video.", ephemeral=True)

        @self.tree.command(name="status", description="Check bot and YouTube connection status")
        async def status(interaction: discord.Interaction) -> None:
            """Check bot and YouTube connection status"""
            try:
                # Test YouTube API connection
                await self.rate_limiter.acquire()
                channel_response = self.youtube.channels().list(
                    part='snippet',
                    id=self.youtube_channel_id
                ).execute()
                
                channel_name = channel_response['items'][0]['snippet']['title']
                
                embed = discord.Embed(
                    title="Bot Status",
                    color=discord.Color.green()
                )
                embed.add_field(name="YouTube Connection", value=f"✅ Connected to {channel_name}")
                embed.add_field(name="Last Check", value=f"<t:{int(time.time())}:R>")
                embed.add_field(name="Uptime", value=f"<t:{int(self.start_time)}:R>")
                
                await interaction.response.send_message(embed=embed, ephemeral=True)
                
            except Exception as e:
                logger.error(f"Error in status command: {e}")
                await interaction.response.send_message("❌ Error checking status. Check logs for details.", ephemeral=True)

        @self.tree.command(name="postinghistory", description="View recent posting history")
        async def posting_history(interaction: discord.Interaction, days: int = 7) -> None:
            """View recent posting history"""
            try:
                # Check if command is used in the correct channel
                if interaction.channel_id != self.discord_channel_id:
                    await interaction.response.send_message(f"This command can only be used in <#{self.discord_channel_id}>", ephemeral=True)
                    return

                # Defer the response since this might take a while
                await interaction.response.defer()
                
                # Calculate cutoff date
                cutoff_date = datetime.now() - timedelta(days=days)
                
                # Get recent videos
                recent_videos = []
                for video_id, data in self.posted_videos.get('videos', {}).items():
                    posted_at = datetime.fromisoformat(data['posted_at'])
                    if posted_at >= cutoff_date:
                        recent_videos.append((posted_at, video_id, data))
                
                # Sort by posted date (newest first)
                recent_videos.sort(reverse=True)
                
                if not recent_videos:
                    await interaction.followup.send(f"No videos posted in the last {days} days.", ephemeral=True)
                    return
                
                # Create embed
                embed = discord.Embed(
                    title=f"Posting History (Last {days} Days)",
                    color=discord.Color.blue()
                )
                
                # Add videos to embed
                for posted_at, video_id, data in recent_videos:
                    video_type = data['type']
                    emoji = "🔴" if video_type == "livestream" else "🎥" if video_type == "video" else "📱"
                    embed.add_field(
                        name=f"{emoji} {posted_at.strftime('%Y-%m-%d %H:%M:%S')}",
                        value=f"Type: {video_type.title()}\nVideo: https://www.youtube.com/watch?v={video_id}",
                        inline=False
                    )
                
                # Add summary
                total_videos = len(recent_videos)
                livestreams = sum(1 for _, _, data in recent_videos if data['type'] == 'livestream')
                regular_videos = sum(1 for _, _, data in recent_videos if data['type'] == 'video')
                shorts = sum(1 for _, _, data in recent_videos if data['type'] == 'short')
                
                embed.set_footer(text=f"Total: {total_videos} videos ({livestreams} livestreams, {regular_videos} videos, {shorts} shorts)")
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except Exception as e:
                logger.error(f"Error in posting_history command: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message("Sorry, there was an error getting the posting history.", ephemeral=True)
                else:
                    await interaction.followup.send("Sorry, there was an error getting the posting history.", ephemeral=True)

    async def get_uploads_playlist_id(self) -> str:
        """Cache the uploads playlist ID to reduce API calls"""
        # Check cache first
        cached_id = self.playlist_cache.get('uploads_id')
        if cached_id:
            return cached_id

        await self.rate_limiter.acquire()
        channel_response = self.youtube.channels().list(
            part='contentDetails',
            id=self.youtube_channel_id
        ).execute()
        
        if not channel_response.get('items'):
            raise ValueError(f"Could not find YouTube channel with ID: {self.youtube_channel_id}")
        
        playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        self.playlist_cache.set('uploads_id', playlist_id)
        return playlist_id

    async def handle_api_error(self, error: Exception) -> bool:
        """Handle API errors and implement backoff strategy"""
        if isinstance(error, HttpError):
            if error.resp.status in [429, 500, 503]:  # Rate limit or server errors
                self.consecutive_errors += 1
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.error("Too many consecutive errors, stopping YouTube checks")
                    self.check_youtube_updates.stop()
                    return False
                # Exponential backoff
                await asyncio.sleep(2 ** self.consecutive_errors)
            else:
                logger.error(f"YouTube API error: {error}")
        else:
            logger.error(f"Unexpected error: {error}")
        return True

    async def on_ready(self) -> None:
        """Called when the bot is ready and connected to Discord"""
        logger.info(f'Logged in as {self.user.name}')
        
        # Initialize tracking without posting
        await self.initialize_tracking()
        
        # Start background tasks
        self.check_youtube_updates.start()
        self.cleanup_old_entries.start()
        
        # Add tasks to active tasks set
        self.active_tasks.add(self.check_youtube_updates)
        self.active_tasks.add(self.cleanup_old_entries)

    async def initialize_tracking(self) -> None:
        """Initialize tracking without posting to Discord"""
        try:
            logger.info("Initializing tracking without posting...")
            
            # Get channel uploads playlist ID (cached)
            uploads_playlist_id = await self.get_uploads_playlist_id()
            
            # Get recent videos with rate limiting
            await self.rate_limiter.acquire()
            playlist_response = self.youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=50
            ).execute()
            
            if not playlist_response.get('items'):
                logger.warning("No videos found in uploads playlist during initialization")
                return
                
            # Process videos
            for item in playlist_response['items']:
                try:
                    video_id = item['snippet']['resourceId']['videoId']
                    published_at = datetime.fromisoformat(item['snippet']['publishedAt'].replace('Z', '+00:00'))
                    
                    # Skip if video is too old
                    if published_at < datetime.now(published_at.tzinfo) - timedelta(days=30):
                        continue
                        
                    # Get video details
                    await self.rate_limiter.acquire()
                    video_response = self.youtube.videos().list(
                        part='snippet,liveStreamingDetails',
                        id=video_id
                    ).execute()
                    
                    if not video_response.get('items'):
                        continue
                        
                    video = video_response['items'][0]
                    is_livestream = video.get('snippet', {}).get('liveBroadcastContent') == 'live'
                    is_short = video.get('snippet', {}).get('title', '').lower().startswith('#shorts')
                    
                    # Update tracking without posting
                    if is_livestream:
                        self.last_livestream_id = video_id
                    elif is_short:
                        self.last_short_id = video_id
                    else:
                        self.last_video_id = video_id
                        
                except Exception as e:
                    logger.error(f"Error processing video during initialization: {str(e)}")
                    continue
            
            # Save tracking variables
            self._save_tracking_vars()
            logger.info("Tracking initialization complete")
            
        except Exception as e:
            logger.error(f"Error during tracking initialization: {str(e)}")

    @tasks.loop(hours=1)
    async def cleanup_old_entries(self) -> None:
        """Clean up old entries from the posted videos history"""
        try:
            logger.info("Running scheduled cleanup of old entries...")
            self._cleanup_old_entries()
        except Exception as e:
            logger.error(f"Error in cleanup task: {e}")

    @cleanup_old_entries.before_loop
    async def before_cleanup_old_entries(self) -> None:
        """Wait for bot to be ready before starting cleanup loop"""
        await self.wait_until_ready()

    @tasks.loop(minutes=15)
    async def check_youtube_updates(self) -> None:
        """Check for new YouTube content with improved error handling and caching"""
        try:
            # Reset error counter on successful check
            self.consecutive_errors = 0
            
            # Get channel uploads playlist ID (cached)
            uploads_playlist_id = await self.get_uploads_playlist_id()
            
            # Get recent videos with rate limiting
            await self.rate_limiter.acquire()
            playlist_response = self.youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=50  # Increased from 10 to catch more videos
            ).execute()
            
            if not playlist_response.get('items'):
                logger.warning("No videos found in uploads playlist")
                return
                
            # Process videos
            for item in playlist_response['items']:
                try:
                    video_id = item['snippet']['resourceId']['videoId']
                    published_at = datetime.fromisoformat(item['snippet']['publishedAt'].replace('Z', '+00:00'))
                    
                    logger.info(f"Processing video: {video_id} published at {published_at}")
                    
                    # Skip if video is too old (increased to 30 days)
                    if published_at < datetime.now(published_at.tzinfo) - timedelta(days=30):
                        logger.info(f"Skipping video {video_id} - too old")
                        continue
                        
                    # Get video details with rate limiting
                    await self.rate_limiter.acquire()
                    video_response = self.youtube.videos().list(
                        part='snippet,liveStreamingDetails',
                        id=video_id
                    ).execute()
                    
                    if not video_response.get('items'):
                        logger.warning(f"No video details found for video ID: {video_id}")
                        continue
                        
                    video = video_response['items'][0]
                    is_livestream = video.get('snippet', {}).get('liveBroadcastContent') == 'live'
                    is_short = video.get('snippet', {}).get('title', '').lower().startswith('#shorts')
                    
                    logger.info(f"Video {video_id} - Livestream: {is_livestream}, Short: {is_short}")
                    logger.info(f"Last video ID: {self.last_video_id}, Last short ID: {self.last_short_id}, Last livestream ID: {self.last_livestream_id}")
                    
                    channel = self.get_channel(self.discord_channel_id)
                    
                    if not channel:
                        logger.error(f"Could not find Discord channel with ID: {self.discord_channel_id}")
                        return
                        
                    # Log channel permissions
                    bot_member = channel.guild.get_member(self.user.id)
                    if bot_member:
                        logger.info(f"Bot permissions in channel {channel.name}:")
                        logger.info(f"- Send Messages: {channel.permissions_for(bot_member).send_messages}")
                        logger.info(f"- Embed Links: {channel.permissions_for(bot_member).embed_links}")
                        logger.info(f"- Read Messages: {channel.permissions_for(bot_member).read_messages}")
                    else:
                        logger.error(f"Could not find bot member in guild {channel.guild.name}")
                    
                    # Send notifications for new content
                    if is_livestream and not self._is_video_posted(video_id):
                        logger.info(f"Sending livestream notification for video {video_id}")
                        try:
                            logger.info(f"Attempting to send message to channel {channel.id} in guild {channel.guild.id}")
                            message = await channel.send(f"🔴 Goose is LIVE on YouTube!\nhttps://www.youtube.com/watch?v={video_id}")
                            logger.info(f"Successfully sent message with ID: {message.id}")
                            self.last_livestream_id = video_id
                            self._add_posted_video(video_id, 'livestream', str(message.id))
                            self._save_tracking_vars()  # Save after successful notification
                        except discord.Forbidden as e:
                            logger.error(f"Forbidden error sending livestream notification: {e}")
                            logger.error(f"Channel ID: {channel.id}, Guild ID: {channel.guild.id}")
                            logger.error(f"Bot ID: {self.user.id}")
                            logger.error(f"HTTP Status: {e.status}")
                            logger.error(f"Error Code: {e.code}")
                        except discord.HTTPException as e:
                            logger.error(f"HTTP error sending livestream notification: {e}")
                            logger.error(f"Status: {e.status}")
                            logger.error(f"Response: {e.response}")
                        except Exception as e:
                            logger.error(f"Error sending livestream notification: {str(e)}")
                            logger.error(f"Error type: {type(e).__name__}")
                    elif is_short and not self._is_video_posted(video_id):
                        logger.info(f"Sending short notification for video {video_id}")
                        try:
                            logger.info(f"Attempting to send message to channel {channel.id} in guild {channel.guild.id}")
                            message = await channel.send(f"🎥 New YouTube Short!\nhttps://www.youtube.com/watch?v={video_id}")
                            logger.info(f"Successfully sent message with ID: {message.id}")
                            self.last_short_id = video_id
                            self._add_posted_video(video_id, 'short', str(message.id))
                            self._save_tracking_vars()  # Save after successful notification
                        except discord.Forbidden as e:
                            logger.error(f"Forbidden error sending short notification: {e}")
                            logger.error(f"Channel ID: {channel.id}, Guild ID: {channel.guild.id}")
                            logger.error(f"Bot ID: {self.user.id}")
                            logger.error(f"HTTP Status: {e.status}")
                            logger.error(f"Error Code: {e.code}")
                        except discord.HTTPException as e:
                            logger.error(f"HTTP error sending short notification: {e}")
                            logger.error(f"Status: {e.status}")
                            logger.error(f"Response: {e.response}")
                        except Exception as e:
                            logger.error(f"Error sending short notification: {str(e)}")
                            logger.error(f"Error type: {type(e).__name__}")
                    elif not is_livestream and not is_short and not self._is_video_posted(video_id):
                        logger.info(f"Sending video notification for video {video_id}")
                        try:
                            logger.info(f"Attempting to send message to channel {channel.id} in guild {channel.guild.id}")
                            message = await channel.send(f"🎥 New YouTube Video!\nhttps://www.youtube.com/watch?v={video_id}")
                            logger.info(f"Successfully sent message with ID: {message.id}")
                            self.last_video_id = video_id
                            self._add_posted_video(video_id, 'video', str(message.id))
                            self._save_tracking_vars()  # Save after successful notification
                        except discord.Forbidden as e:
                            logger.error(f"Forbidden error sending video notification: {e}")
                            logger.error(f"Channel ID: {channel.id}, Guild ID: {channel.guild.id}")
                            logger.error(f"Bot ID: {self.user.id}")
                            logger.error(f"HTTP Status: {e.status}")
                            logger.error(f"Error Code: {e.code}")
                        except discord.HTTPException as e:
                            logger.error(f"HTTP error sending video notification: {e}")
                            logger.error(f"Status: {e.status}")
                            logger.error(f"Response: {e.response}")
                        except Exception as e:
                            logger.error(f"Error sending video notification: {str(e)}")
                            logger.error(f"Error type: {type(e).__name__}")
                    else:
                        logger.info(f"No notification sent for video {video_id} - already processed")
                    
                except Exception as e:
                    logger.error(f"Error processing video: {str(e)}")
                    if not await self.handle_api_error(e):
                        return
                    continue
                    
            # Update last check time
            self.last_check_time = datetime.now()
            
        except Exception as e:
            logger.error(f"Error in check_youtube_updates: {str(e)}")
            if not await self.handle_api_error(e):
                return

    @check_youtube_updates.before_loop
    async def before_check_youtube_updates(self) -> None:
        """Wait for bot to be ready before starting YouTube check loop"""
        await self.wait_until_ready()

def main() -> None:
    try:
        intents = discord.Intents.default()
        intents.message_content = True
        
        bot = GooseBandTracker(intents)
        
        # Run the bot with error handling
        bot.run(os.getenv('DISCORD_TOKEN'))
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()