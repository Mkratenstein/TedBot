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
        
        try:
            # Ensure directory exists and is writable
            os.makedirs(base_path, exist_ok=True)
            test_file = os.path.join(base_path, '.test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            logger.info(f"Successfully verified write access to {base_path}")
        except Exception as e:
            logger.error(f"Error setting up data directory {base_path}: {e}")
            raise RuntimeError(f"Cannot write to data directory: {e}")
        
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
                # Create initial tracking file
                self._save_tracking_vars()
            
            # Load posted videos history
            if os.path.exists(self.posted_videos_file):
                with open(self.posted_videos_file, 'r') as f:
                    self.posted_videos = json.load(f)
                    logger.info(f"Loaded {len(self.posted_videos.get('videos', {}))} posted videos from history")
            else:
                logger.info("No posted videos history found, starting fresh")
                self.posted_videos = {'videos': {}}
                # Create initial history file
                with open(self.posted_videos_file, 'w') as f:
                    json.dump(self.posted_videos, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error loading tracking variables: {e}")
            # Initialize with empty values
            self.last_video_id = ''
            self.last_livestream_id = ''
            self.last_short_id = ''
            self.last_check_time = datetime.now()
            self.posted_videos = {'videos': {}}
            # Try to create fresh tracking files
            try:
                self._save_tracking_vars()
                with open(self.posted_videos_file, 'w') as f:
                    json.dump(self.posted_videos, f, indent=2)
            except Exception as save_error:
                logger.error(f"Failed to create fresh tracking files: {save_error}")
        
        self.active_tasks: set = set()
        self.consecutive_errors: int = 0
        self.max_consecutive_errors: int = 3

    def _save_tracking_vars(self) -> None:
        """Save tracking variables with improved error handling"""
        try:
            # Save current tracking
            current_data = {
                'last_video_id': self.last_video_id,
                'last_livestream_id': self.last_livestream_id,
                'last_short_id': self.last_short_id,
                'last_check_time': self.last_check_time.isoformat()
            }
            
            # Use atomic write for current tracking
            temp_file = f"{self.current_tracking_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(current_data, f, indent=2)
            os.replace(temp_file, self.current_tracking_file)
            
            logger.info(f"Saved current tracking: last_video_id={self.last_video_id}, last_livestream_id={self.last_livestream_id}, last_short_id={self.last_short_id}")
            
            # Save posted videos history
            temp_history_file = f"{self.posted_videos_file}.tmp"
            with open(temp_history_file, 'w') as f:
                json.dump(self.posted_videos, f, indent=2)
            os.replace(temp_history_file, self.posted_videos_file)
            
            logger.info(f"Saved {len(self.posted_videos.get('videos', {}))} posted videos to history")
            
        except Exception as e:
            logger.error(f"Error saving tracking variables: {e}")
            # Try to clean up any temporary files
            for temp_file in [f"{self.current_tracking_file}.tmp", f"{self.posted_videos_file}.tmp"]:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except Exception as cleanup_error:
                    logger.error(f"Error cleaning up temporary file {temp_file}: {cleanup_error}")
            raise  # Re-raise the original error

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
        """Initialize tracking variables and posted videos history without posting."""
        try:
            self.logger.info("Initializing tracking without posting...")
            
            # Get recent videos
            videos = self.youtube.get_recent_videos(max_results=50)
            if not videos:
                self.logger.warning("No videos found during initialization")
                return
            
            # Initialize tracking with the most recent video
            latest_video = videos[0]
            self.current_tracking = {
                'last_video_id': latest_video['id'],
                'last_livestream_id': '',
                'last_short_id': ''
            }
            
            # Add all videos to posted history without sending messages
            for video in videos:
                video_id = video['id']
                if not self._is_video_posted(video_id):
                    self.posted_videos[video_id] = {
                        'title': video['title'],
                        'url': f"https://www.youtube.com/watch?v={video_id}",
                        'published_at': video['published_at'],
                        'type': 'video',
                        'status': 'initialized'  # Mark as initialized instead of posted
                    }
            
            # Save both tracking files
            self._save_tracking_vars()
            self.logger.info("Tracking initialization complete")
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {str(e)}")
            raise

    def _merge_tracking_to_history(self) -> None:
        """Merge current tracking into posted videos history and remove duplicates"""
        try:
            # Get current tracking data
            current_data = {
                'last_video_id': self.last_video_id,
                'last_livestream_id': self.last_livestream_id,
                'last_short_id': self.last_short_id
            }
            
            # Add each video to history if it exists and isn't already there
            for video_type, video_id in [
                ('video', current_data['last_video_id']),
                ('livestream', current_data['last_livestream_id']),
                ('short', current_data['last_short_id'])
            ]:
                if video_id and video_id not in self.posted_videos.get('videos', {}):
                    self._add_posted_video(video_id, video_type, 'merged')
            
            # Remove any duplicates (keeping the most recent entry)
            seen_videos = set()
            unique_videos = {}
            
            for video_id, data in self.posted_videos.get('videos', {}).items():
                if video_id not in seen_videos:
                    seen_videos.add(video_id)
                    unique_videos[video_id] = data
            
            self.posted_videos['videos'] = unique_videos
            
            # Save the updated history
            with open(self.posted_videos_file, 'w') as f:
                json.dump(self.posted_videos, f, indent=2)
            
            logger.info(f"Merged current tracking into history. Total unique videos: {len(unique_videos)}")
            
        except Exception as e:
            logger.error(f"Error merging tracking to history: {e}")

    async def check_youtube_updates(self):
        """Check for new YouTube videos and post them to Discord."""
        while True:
            try:
                self.logger.info("Checking for new YouTube videos...")
                
                # Get recent videos
                videos = self.youtube.get_recent_videos(max_results=50)
                if not videos:
                    self.logger.warning("No videos found")
                    await asyncio.sleep(900)  # 15 minutes
                    continue
                
                # Process each video
                for video in videos:
                    try:
                        video_id = video['id']
                        published_at = video['published_at']
                        
                        # Skip if video is too old
                        if published_at < datetime.now(published_at.tzinfo) - timedelta(days=30):
                            self.logger.info(f"Skipping video {video_id} - too old")
                            continue
                        
                        # Check if video is already posted
                        if self._is_video_posted(video_id):
                            continue
                        
                        # Get video details
                        video_details = self.youtube.get_video_details(video_id)
                        if not video_details:
                            continue
                            
                        is_livestream = video_details.get('liveBroadcastContent') == 'live'
                        is_short = video_details.get('title', '').lower().startswith('#shorts')
                        
                        # Update tracking and post if needed
                        if is_livestream:
                            if video_id != self.current_tracking['last_livestream_id']:
                                await self._post_video(video_id, 'livestream')
                                self.current_tracking['last_livestream_id'] = video_id
                        elif is_short:
                            if video_id != self.current_tracking['last_short_id']:
                                await self._post_video(video_id, 'short')
                                self.current_tracking['last_short_id'] = video_id
                        else:
                            if video_id != self.current_tracking['last_video_id']:
                                await self._post_video(video_id, 'video')
                                self.current_tracking['last_video_id'] = video_id
                        
                    except Exception as e:
                        self.logger.error(f"Error processing video {video_id}: {str(e)}")
                        continue
                
                # Merge current tracking into history
                self._merge_tracking_to_history()
                
                # Save tracking variables
                self._save_tracking_vars()
                
                # Wait before next check
                await asyncio.sleep(900)  # 15 minutes
                
            except Exception as e:
                self.logger.error(f"Error in check_youtube_updates: {str(e)}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

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