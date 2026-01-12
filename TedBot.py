import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
import sys
from typing import Dict, Optional, List, Any
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

# Determine base path for data files and logs
DATA_BASE_PATH = '/data' if os.path.exists('/data') else 'data'
os.makedirs(DATA_BASE_PATH, exist_ok=True) # Ensure it exists

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(DATA_BASE_PATH, 'bot.log')) # Log to /data/bot.log or data/bot.log
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
        self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
        
        if len(self.requests) >= self.max_requests:
            sleep_time = (self.requests[0] + self.time_window) - now
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.requests.append(time.time()) # Record current time

class AsyncCache:
    def __init__(self, maxsize: int = 128):
        self.maxsize = maxsize
        self.cache: Dict[str, Any] = {}
        self.times: Dict[str, float] = {}

    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            self.times[key] = time.time() # Update access time
            return self.cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        if len(self.cache) >= self.maxsize:
            oldest_key = min(self.times, key=self.times.get)
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
        
        self.logger = logging.getLogger(__name__) # Class-level logger
        
        # New tracking file paths will be set in _init_tracking_vars
        self.posted_videos_file: Optional[str] = None
        self.current_scrape_file: Optional[str] = None
        self.ready_for_discord_file: Optional[str] = None

        # Data structures for in-memory tracking (loaded from files)
        self.posted_videos_data: Dict[str, Dict[str, Any]] = {} # Stores video_id: {details}
        # current_scrape and ready_for_discord will be transient lists of video details
        
        # Initialize YouTube API client first, as _validate_env_vars will use it.
        # Note: _validate_env_vars also checks if YOUTUBE_API_KEY is the placeholder.
        # This means YOUTUBE_API_KEY must be available from os.getenv() here.
        youtube_api_key_for_build = os.getenv('YOUTUBE_API_KEY')
        if not youtube_api_key_for_build or youtube_api_key_for_build == 'your_youtube_api_key_here':
            self.logger.critical("YouTube API key is missing or is the placeholder. Cannot initialize YouTube client.")
            # We will still proceed to _validate_env_vars which will then raise a more specific error about the key.
            # Or, we could raise an error immediately here.
            # For now, let _validate_env_vars handle the detailed error logging/raising.
            self.youtube = None # Ensure it exists as None if build fails or key is bad
        else:
            try:
                self.youtube = build('youtube', 'v3', developerKey=youtube_api_key_for_build)
            except Exception as e:
                self.logger.critical(f"Failed to build YouTube client: {e}")
                self.youtube = None # Ensure it exists as None if build fails
        
        self._validate_env_vars() # Now call validation, which uses self.youtube
        
        self.rate_limiter = RateLimiter(max_requests=90, time_window=60) # Adjusted slightly
        
        self._init_tracking_vars() # This will now initialize the three files
        
        self.playlist_cache = AsyncCache(maxsize=10) # Cache for uploads playlist ID
        self.history_cleanup_age_days = 30 # Days after which video history is cleaned up
        
        self._register_commands()
        self.first_run_after_init_complete = False # Flag for 15-min delay in processing_task
        self.initial_history_populated = False # Flag to track if full history has been pulled

    async def setup_hook(self) -> None:
        await self.tree.sync()

    def _validate_env_vars(self) -> None:
        required_vars = [
            'YOUTUBE_API_KEY', 'DISCORD_TOKEN', 'YOUTUBE_CHANNEL_ID',
            'DISCORD_CHANNEL_ID'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            self.logger.critical(f"Missing required environment variables: {', '.join(missing_vars)}")
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        self.youtube_api_key = os.getenv('YOUTUBE_API_KEY')
        self.discord_token = os.getenv('DISCORD_TOKEN')
        self.youtube_channel_id = os.getenv('YOUTUBE_CHANNEL_ID')
        self.discord_channel_id = int(os.getenv('DISCORD_CHANNEL_ID'))

        if self.youtube_api_key == 'your_youtube_api_key_here':
            self.logger.critical("Default YouTube API key detected. Please configure.")
            raise ValueError("YouTube API key not configured.")
        if not self.youtube_channel_id.startswith('UC'):
            self.logger.warning(f"YouTube channel ID '{self.youtube_channel_id}' may not be valid.")
        
        # Explicitly check if the YouTube client was initialized before trying to use it for API validation
        if self.youtube is None:
            self.logger.critical(
                "YouTube API client (self.youtube) could not be initialized. "
                "This could be due to a missing/placeholder API key (which should have been caught earlier), "
                "an invalid API key that passed the placeholder check, or other issues during client build. "
                "Check previous logs for specific errors during YouTube client build."
            )
            raise ValueError("YouTube API client is not available. Cannot validate API key/channel ID via API call.")

        try:
            self.youtube.channels().list(part='id', id=self.youtube_channel_id).execute()
            self.logger.info("Successfully validated YouTube API key and channel ID.")
        except HttpError as e:
            self.logger.critical(f"Failed to validate YouTube API key/channel ID: {e.resp.status} - {e.content}")
            raise ValueError(f"YouTube API validation failed: {e}")
        except Exception as e:
            self.logger.critical(f"Unexpected error during YouTube API validation: {e}")
            raise ValueError(f"Unexpected error during YouTube API validation: {e}")

    def _init_tracking_vars(self) -> None:
        base_path = DATA_BASE_PATH # Use the globally defined one for consistency
        # os.makedirs(base_path, exist_ok=True) # Already created when DATA_BASE_PATH is defined
        self.logger.info(f"Using data directory: {base_path}")

        self.posted_videos_file = os.path.join(base_path, 'posted_videos.json')
        self.current_scrape_file = os.path.join(base_path, 'current_scrape.json')
        self.ready_for_discord_file = os.path.join(base_path, 'ready_for_discord.json')

        # Initialize files if they don't exist
        for file_path in [self.posted_videos_file, self.current_scrape_file, self.ready_for_discord_file]:
            if not os.path.exists(file_path):
                try:
                    # posted_videos stores a dict {video_id: data}, others store a list [video_data]
                    initial_content = {} if file_path == self.posted_videos_file else []
                    with open(file_path, 'w') as f:
                        json.dump(initial_content, f, indent=2)
                    self.logger.info(f"Initialized empty tracking file: {file_path}")
                except IOError as e:
                    self.logger.error(f"Failed to initialize tracking file {file_path}: {e}")
                    raise # Critical error if we can't write tracking files
        
        # Load posted_videos_data into memory
        try:
            with open(self.posted_videos_file, 'r') as f:
                self.posted_videos_data = json.load(f)
            self.logger.info(f"Loaded {len(self.posted_videos_data)} videos from {self.posted_videos_file}")
        except (IOError, json.JSONDecodeError) as e:
            self.logger.error(f"Error loading {self.posted_videos_file}: {e}. Starting with empty history.")
            self.posted_videos_data = {} # Ensure it's an empty dict on error

        # active_tasks and consecutive_errors are for the old logic, can be re-evaluated for new tasks
        self.active_tasks: set = set() 
        self.consecutive_api_errors: int = 0
        self.max_consecutive_api_errors: int = 5

    def _save_json_data(self, file_path: str, data: Any) -> bool:
        """Atomically save JSON data to a file."""
        temp_file_path = file_path + ".tmp"
        try:
            with open(temp_file_path, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_file_path, file_path)
            self.logger.info(f"Successfully saved data to {file_path}")
            return True
        except (IOError, TypeError) as e: # Added TypeError for non-serializable data
            self.logger.error(f"Error saving data to {file_path}: {e} (Data: {str(data)[:200]}...)") # Log snippet of data
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except OSError as e_rem:
                    self.logger.error(f"Failed to remove temp file {temp_file_path}: {e_rem}")
            return False

    async def _get_all_channel_videos(self) -> List[Dict[str, Any]]:
        self.logger.info(f"Starting to fetch all videos for channel {self.youtube_channel_id}...")
        all_videos = []
        next_page_token = None
        
        uploads_playlist_id = await self.get_uploads_playlist_id()
        if not uploads_playlist_id:
            self.logger.error("Cannot fetch all videos without uploads_playlist_id.")
            return []

        max_pages = 50 # Safety break for very large channels / quota protection
        pages_processed = 0
        total_items_processed = 0
        earliest_date = None
        latest_date = None

        while pages_processed < max_pages:
            pages_processed += 1
            try:
                await self.rate_limiter.acquire()
                request = self.youtube.playlistItems().list(
                    part="snippet,contentDetails,status", # Added status for privacy check
                    playlistId=uploads_playlist_id,
                    maxResults=50, # Max allowed by API
                    pageToken=next_page_token
                )
                response = request.execute()

                items_on_page = response.get("items", [])
                total_items_processed += len(items_on_page)
                self.logger.info(f"Page {pages_processed}: Received {len(items_on_page)} items from API before filtering. Total items processed so far: {total_items_processed}")

                for item_index, item in enumerate(items_on_page):
                    video_id = item.get("contentDetails", {}).get("videoId")
                    snippet = item.get("snippet", {})
                    status = item.get("status", {})
                    upload_status = status.get("uploadStatus")
                    privacy_status = status.get("privacyStatus")
                    published_at_raw = snippet.get("publishedAt")

                    self.logger.debug(f"Page {pages_processed}, Item {item_index + 1}: Processing videoId: {video_id}, uploadStatus: {upload_status}, privacyStatus: {privacy_status}, publishedAt: {published_at_raw}")

                    # Ensure video is public
                    if privacy_status == "public":
                        if video_id and published_at_raw:
                            try:
                                published_at = datetime.fromisoformat(published_at_raw.replace('Z', '+00:00'))
                                # Update earliest/latest dates
                                if earliest_date is None or published_at < earliest_date:
                                    earliest_date = published_at
                                if latest_date is None or published_at > latest_date:
                                    latest_date = published_at
                                
                                # Skip future-dated videos
                                current_time = datetime.now(timezone.utc)
                                if published_at > current_time:
                                    self.logger.warning(f"Page {pages_processed}, Item {item_index + 1}: Skipping future-dated video {video_id} with publish date {published_at} (current time: {current_time})")
                                    continue
                            except ValueError:
                                self.logger.warning(f"Page {pages_processed}, Item {item_index + 1}: Could not parse publishedAt for video {video_id}: {published_at_raw}. Skipping item.")
                                continue # Skip this item

                            all_videos.append({
                                "id": video_id,
                                "title": snippet.get("title", "N/A"),
                                "description": snippet.get("description", ""),
                                "published_at": published_at.isoformat(), # Store as ISO string
                                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", "")
                                # Add other relevant details if needed
                            })
                            self.logger.debug(f"Page {pages_processed}, Item {item_index + 1}: Appended video {video_id}.")
                        else:
                            self.logger.warning(f"Page {pages_processed}, Item {item_index + 1}: Skipped due to missing videoId ('{video_id}') or publishedAt ('{published_at_raw}').")
                    else:
                        self.logger.info(f"Page {pages_processed}, Item {item_index + 1}: Skipped videoId {video_id} due to status (Upload: {upload_status}, Privacy: {privacy_status}).")
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    self.logger.info(f"Finished fetching all videos. Total items processed: {total_items_processed}, Total videos added: {len(all_videos)}")
                    if earliest_date and latest_date:
                        self.logger.info(f"Date range of videos: {earliest_date.isoformat()} to {latest_date.isoformat()}")
                    break 
                self.logger.info(f"Fetched page {pages_processed}, got {len(response.get('items', []))} items. Next page token: {next_page_token is not None}")
                await asyncio.sleep(1) # Small delay between pages

            except HttpError as e:
                self.logger.error(f"HttpError fetching playlist page: {e.resp.status} - {e.content}")
                # Implement backoff or break if critical error (e.g., quota exceeded)
                if e.resp.status == 403 or e.resp.status == 400: # Quota or bad request
                    self.logger.critical("Critical API error during full scrape, stopping.")
                    break
                await asyncio.sleep(5 * (self.consecutive_api_errors + 1)) # Exponential backoff
                self.consecutive_api_errors += 1
                if self.consecutive_api_errors > self.max_consecutive_api_errors:
                    self.logger.critical("Max API errors reached during full scrape.")
                    break
            except Exception as e:
                self.logger.error(f"Unexpected error fetching playlist page: {e}")
                await asyncio.sleep(5) # General error backoff
                break # Stop on unexpected errors for safety

        self.consecutive_api_errors = 0 # Reset after successful (or partially successful) scrape
        self.logger.info(f"Completed fetching all videos. Found {len(all_videos)} videos after {pages_processed} pages. Total items processed: {total_items_processed}")
        if earliest_date and latest_date:
            self.logger.info(f"Final date range of videos: {earliest_date.isoformat()} to {latest_date.isoformat()}")
        return all_videos

    async def _populate_initial_history(self) -> None:
        # Check if posted_videos_data is empty (or some other condition for first run)
        if not self.posted_videos_data and not self.initial_history_populated:
            self.logger.info("`posted_videos.json` is empty. Attempting to populate initial history...")
            all_videos_details = await self._get_all_channel_videos()
            
            if not all_videos_details:
                self.logger.warning("No videos found to populate initial history.")
                self.initial_history_populated = True # Mark as attempted even if no videos
                return

            new_history_data = {}
            for video_detail in all_videos_details:
                new_history_data[video_detail["id"]] = {
                    "title": video_detail["title"],
                    "published_at": video_detail["published_at"], # Already ISO string
                    "type": "video", # Assume 'video', can be refined later if shorts/live have different fetch path
                    "post_status": "history_initialized", # Special status (changed from "status" to "post_status")
                    "discord_message_id": None,
                    "posted_to_discord_at": None
                }
            
            if self._save_json_data(self.posted_videos_file, new_history_data):
                self.posted_videos_data = new_history_data # Update in-memory cache
                self.logger.info(f"Successfully populated initial history with {len(new_history_data)} videos.")
            else:
                self.logger.error("Failed to save populated initial history to file.")
            self.initial_history_populated = True # Mark as populated (or attempt completed)
        else:
            self.logger.info("Initial video history already populated or not empty. Skipping population.")
            self.initial_history_populated = True # Ensure this is true if data already exists

    async def on_ready(self) -> None:
        self.logger.info(f'Logged in as {self.user.name} (ID: {self.user.id})')
        self.start_time = time.time() # For uptime command
        
        # Populate initial history if needed (runs once)
        await self._populate_initial_history() 
        self.initial_history_populated = True # Explicitly set after awaiting
        
        # Start the main processing task
        self.processing_task.start()
        self.logger.info("Main processing task started.")
        
        # Start cleanup task for old entries in posted_videos.json
        self.cleanup_posted_videos_task.start()
        self.logger.info("Cleanup task for posted_videos.json started.")
        
        self.active_tasks.add(self.processing_task)
        self.active_tasks.add(self.cleanup_posted_videos_task)

    @tasks.loop(minutes=15)
    async def processing_task(self) -> None:
        try:
            if not self.initial_history_populated:
                self.logger.info("Processing task: Waiting for initial history population to complete.")
                # _populate_initial_history is now awaited in on_ready, so this check might be redundant
                # but as a safeguard, or if on_ready logic changes.
                await asyncio.sleep(30) 
                if not self.initial_history_populated: # Re-check
                    self.logger.error("Processing task: Initial history still not populated after wait. Skipping cycle.")
                    return

            current_time = time.time()
            if not self.first_run_after_init_complete and (current_time - self.start_time) < (15 * 60):
                wait_time = (15*60) - (current_time - self.start_time)
                self.logger.info(f"Processing task: First run cycle. Waiting {wait_time:.0f} more seconds before active scraping.")
                return # Skip this cycle, wait for the next iteration of the loop
            elif not self.first_run_after_init_complete:
                 self.logger.info("Processing task: Initial 15-minute delay complete. Starting active cycle.")
                 self.first_run_after_init_complete = True

            self.logger.info("Processing task: Starting new cycle.")
            
            # 1. Scrape YouTube -> List[Dict[str, Any]] (and saves to current_scrape.json)
            scraped_videos_list = await self._scrape_youtube_and_save()
            if scraped_videos_list is None: 
                self.logger.error("Processing task: Halting cycle due to error in scraping or saving current videos.")
                return
            if not scraped_videos_list:
                self.logger.info("Processing task: No videos returned from scrape. Cycle ends.")
                # Clear intermediate files as a precaution if they were somehow written to by a partial success
                await self._update_master_history_and_cleanup([]) # Pass empty list to just clear files
                return

            # 2. Compare current_scrape with posted_videos -> List[Dict[str, Any]] (and saves to ready_for_discord.json)
            videos_to_post_list = await self._compare_and_prepare_posts(scraped_videos_list)
            if videos_to_post_list is None: 
                 self.logger.error("Processing task: Halting cycle due to error in comparison or preparation.")
                 return
            
            if not videos_to_post_list:
                self.logger.info("Processing task: No new videos found to post.")
            else:
                self.logger.info(f"Processing task: Found {len(videos_to_post_list)} new videos to post.")
                # 3. Post to Discord
                processed_for_history_list = await self._post_new_videos(videos_to_post_list)
                
                # 4. Update History & Cleanup
                await self._update_master_history_and_cleanup(processed_for_history_list)

            self.logger.info("Processing task: Cycle complete.")
            self.consecutive_api_errors = 0 # Reset on successful cycle

        except Exception as e:
            self.logger.error(f"Critical error in processing_task loop: {e}", exc_info=True)
            self.consecutive_api_errors +=1 
            if self.consecutive_api_errors > self.max_consecutive_api_errors * 2: 
                self.logger.critical("Processing task failed too many times, stopping task.")
                self.processing_task.stop()

    @processing_task.before_loop
    async def before_processing_task(self) -> None:
        await self.wait_until_ready()
        # Ensure initial history is populated before the first run of the task even considers the 15min delay logic
        if not self.initial_history_populated:
            self.logger.info("Before_processing_task: Waiting for initial history population...")
            # This loop is a safeguard. _populate_initial_history in on_ready should handle it.
            while not self.initial_history_populated:
                await asyncio.sleep(10)
            self.logger.info("Before_processing_task: Initial history population confirmed.")
        self.logger.info("Processing task is ready.")

    async def _scrape_youtube_and_save(self) -> Optional[List[Dict[str, Any]]]:
        self.logger.info("STEP 1: Scraping YouTube for recent videos...")
        
        # Determine if this is the first run
        is_first_run = not self.first_run_after_init_complete
        
        if is_first_run:
            self.logger.info("First run detected - fetching all videos")
            scraped_videos_details = await self._get_all_channel_videos()
        else:
            # Get videos from the last 3 days to ensure we don't miss any
            # This is more inclusive than just "yesterday" and handles edge cases
            three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
            three_days_ago = three_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
            self.logger.info(f"Regular run - fetching videos published since {three_days_ago.isoformat()}")
            scraped_videos_details = await self.get_recent_videos(max_results=50, published_after=three_days_ago)

        if scraped_videos_details is None:
            self.logger.error("Scrape Step: Failed to fetch videos from YouTube.")
            return None

        if not scraped_videos_details:
            self.logger.info("Scrape Step: No videos found in the scrape.")
            if self._save_json_data(self.current_scrape_file, []):
                return []
            else:
                self.logger.error(f"Scrape Step: Failed to save empty list to {self.current_scrape_file}")
                return None

        if self._save_json_data(self.current_scrape_file, scraped_videos_details):
            self.logger.info(f"Scrape Step: Successfully saved {len(scraped_videos_details)} videos to {self.current_scrape_file}")
            return scraped_videos_details
        else:
            self.logger.error(f"Scrape Step: Failed to save {len(scraped_videos_details)} scraped videos to {self.current_scrape_file}")
            return None

    async def _compare_and_prepare_posts(self, current_scraped_videos: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        self.logger.info("STEP 2: Comparing scraped videos with master history...")
        videos_ready_to_post = []
        
        if self.posted_videos_data is None: 
            self.logger.error("Compare Step: Master history (posted_videos_data) is None. This should not happen.")
            # Attempt to reload as a last resort, though _init_tracking_vars should prevent this.
            try:
                with open(self.posted_videos_file, 'r') as f:
                    self.posted_videos_data = json.load(f)
                self.logger.info("Compare Step: Successfully reloaded posted_videos_data.")
            except Exception as e:
                self.logger.critical(f"Compare Step: Failed to reload posted_videos_data: {e}. Aborting comparison.")
                return None # Critical error

        for video_detail in current_scraped_videos:
            video_id = video_detail.get("id")
            if not video_id:
                self.logger.warning(f"Compare Step: Scraped video missing ID: {str(video_detail)[:100]}")
                continue

            if video_id not in self.posted_videos_data:
                self.logger.info(f"Compare Step: New video identified for posting: ID {video_id} - Title: {video_detail.get('title')}")
                videos_ready_to_post.append(video_detail) # Add the whole dict as fetched
            else:
                # Check if video was actually posted or if it needs to be reprocessed
                existing_video = self.posted_videos_data[video_id]
                post_status = existing_video.get('post_status', existing_video.get('status', 'unknown'))
                
                # Reprocess if:
                # 1. Status is "history_initialized" (from initial population, never posted)
                # 2. Status is "unknown" (legacy or error)
                # 3. posted_to_discord_at is None (never actually posted)
                if post_status == "history_initialized" or post_status == "unknown" or existing_video.get('posted_to_discord_at') is None:
                    self.logger.info(f"Compare Step: Video ID {video_id} exists in history but was never posted (status: {post_status}). Reprocessing for posting.")
                    videos_ready_to_post.append(video_detail)
                else:
                    # Video was already successfully posted or skipped for valid reason
                    self.logger.debug(f"Compare Step: Video ID {video_id} already processed with status: {post_status}")
        
        # Save the list of new videos (can be empty) to ready_for_discord.json
        if self._save_json_data(self.ready_for_discord_file, videos_ready_to_post):
            self.logger.info(f"Compare Step: Prepared {len(videos_ready_to_post)} videos in {self.ready_for_discord_file}")
            return videos_ready_to_post
        else:
            self.logger.error(f"Compare Step: Failed to save videos to {self.ready_for_discord_file}")
            return None # Indicate failure

    async def _post_new_videos(self, videos_to_post_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.logger.info(f"STEP 3: Attempting to post {len(videos_to_post_list)} new videos to Discord.")
        processed_video_details_for_history = [] 

        if not videos_to_post_list: # Should be checked by caller, but good safeguard
            return []

        channel = self.get_channel(self.discord_channel_id)
        if not channel:
            self.logger.error(f"Post Step: Cannot find Discord channel ID {self.discord_channel_id} for posting. Marking all as failed.")
            for video_data in videos_to_post_list:
                video_data["post_status"] = "failed_channel_not_found"
                video_data["posted_to_discord_at"] = datetime.now().isoformat() # Mark attempt time
                processed_video_details_for_history.append(video_data)
            return processed_video_details_for_history

        # Check permissions once
        bot_member = channel.guild.get_member(self.user.id)
        if not bot_member or not channel.permissions_for(bot_member).send_messages:
            self.logger.error(f"Post Step: Missing send permissions in channel {channel.name}. Marking all as failed.")
            for video_data in videos_to_post_list:
                video_data["post_status"] = "failed_no_permission"
                video_data["posted_to_discord_at"] = datetime.now().isoformat()
                processed_video_details_for_history.append(video_data)
            return processed_video_details_for_history

        for basic_video_data in videos_to_post_list:
            video_id = basic_video_data.get("id")
            self.logger.info(f"Post Step: Processing video ID {video_id} for Discord post.")

            # Fetch full details for accurate typing and richer messages
            full_details = await self.get_video_details(video_id) 

            if not full_details:
                self.logger.error(f"Post Step: Failed to get full details for video ID {video_id}. Skipping post.")
                # Use basic_video_data for history, mark as failed detail fetch
                basic_video_data["post_status"] = "failed_detail_fetch"
                basic_video_data["posted_to_discord_at"] = datetime.now().isoformat()
                processed_video_details_for_history.append(basic_video_data)
                continue

            # Merge full_details into basic_video_data, prioritizing full_details
            # This ensures we record the most accurate info in history
            final_video_data_for_history = {**basic_video_data, **full_details}

            video_title = final_video_data_for_history.get("title", "N/A")
            video_type = final_video_data_for_history.get("type", "video")
            scheduled_start = final_video_data_for_history.get("scheduled_start_time")
            actual_start = final_video_data_for_history.get("actual_start_time")
            actual_end = final_video_data_for_history.get("actual_end_time")

            # Log detailed premiere information
            if scheduled_start or actual_start or actual_end:
                self.logger.info(f"Post Step: Video {video_id} ({video_title}) has premiere details - "
                               f"Scheduled: {scheduled_start}, Actual Start: {actual_start}, End: {actual_end}")

            if video_type == "upcoming_live":
                self.logger.info(f"Post Step: Video {video_id} ({video_title}) is an upcoming live/premiere. Not posting notification now.")
                final_video_data_for_history["post_status"] = "skipped_upcoming"
                final_video_data_for_history["posted_to_discord_at"] = datetime.now().isoformat()
                processed_video_details_for_history.append(final_video_data_for_history)
                continue # Don't post upcoming, but record it in history
            
            message_content = ""
            if video_type == 'livestream':
                message_content = f"🔴 **{video_title}** is LIVE now!\nhttps://www.youtube.com/watch?v={video_id}"
            elif video_type == 'short':
                message_content = f"🎞️ New YouTube Short: **{video_title}**\nhttps://www.youtube.com/shorts/{video_id}" # Use shorts link
            else: # Default to video
                # Check if this was a premiere
                if scheduled_start and actual_start:
                    message_content = f"🎥 New YouTube Video (Premiered): **{video_title}**\nhttps://www.youtube.com/watch?v={video_id}"
                else:
                    message_content = f"🎥 New YouTube Video: **{video_title}**\nhttps://www.youtube.com/watch?v={video_id}"

            try:
                self.logger.info(f"Post Step: Sending Discord message for {video_type} ID {video_id}")
                sent_message = await channel.send(message_content)
                self.logger.info(f"Post Step: Successfully posted video {video_id}. Discord Message ID: {sent_message.id}")
                
                final_video_data_for_history["post_status"] = "success"
                final_video_data_for_history["discord_message_id"] = str(sent_message.id)
                final_video_data_for_history["posted_to_discord_at"] = datetime.now().isoformat()

            except discord.Forbidden as e:
                self.logger.error(f"Post Step: Discord Forbidden error posting video {video_id}. Code: {e.code}, Status: {e.status}, Text: {e.text}", exc_info=True)
                final_video_data_for_history["post_status"] = "failed_discord_forbidden"
                final_video_data_for_history["posted_to_discord_at"] = datetime.now().isoformat()
            except discord.HTTPException as e:
                self.logger.error(f"Post Step: Discord HTTPException posting video {video_id}: {e.status} {e.text}", exc_info=True)
                final_video_data_for_history["post_status"] = "failed_discord_http"
                final_video_data_for_history["posted_to_discord_at"] = datetime.now().isoformat()
            except Exception as e:
                self.logger.error(f"Post Step: Unexpected error posting video {video_id} to Discord: {e}", exc_info=True)
                final_video_data_for_history["post_status"] = "failed_unexpected"
                final_video_data_for_history["posted_to_discord_at"] = datetime.now().isoformat()
            
            processed_video_details_for_history.append(final_video_data_for_history)
            await asyncio.sleep(2) # Small delay between posts to avoid hitting Discord rate limits too hard

        return processed_video_details_for_history

    async def _update_master_history_and_cleanup(self, processed_videos_for_history: List[Dict[str, Any]]) -> None:
        self.logger.info("STEP 4: Updating master history and cleaning up temporary files...")
        
        updated_count = 0
        if self.posted_videos_data is None: 
             self.logger.critical("Update History Step: posted_videos_data is None! This should not happen. Re-initializing to empty dict.")
             self.posted_videos_data = {}

        for video_detail_with_status in processed_videos_for_history:
            video_id = video_detail_with_status.get("id")
            if not video_id:
                self.logger.warning("Update History Step: Video detail missing ID during history update.")
                continue

            # This will overwrite if ID already exists, effectively updating its status
            self.posted_videos_data[video_id] = video_detail_with_status
            updated_count +=1
            self.logger.info(f"Update History Step: Added/Updated video ID {video_id} in master history with status '{video_detail_with_status.get('post_status')}'.")
        
        if updated_count > 0 or not processed_videos_for_history: # Save even if only to clear (if processed_videos_for_history is empty but was called)
            if not self._save_json_data(self.posted_videos_file, self.posted_videos_data):
                self.logger.error("Update History Step: CRITICAL - Failed to save updated master history to posted_videos.json!")
            else:
                 self.logger.info(f"Update History Step: Successfully saved master history with {len(self.posted_videos_data)} total entries.")
        
        # Clear current_scrape.json and ready_for_discord.json by saving empty lists
        if not self._save_json_data(self.current_scrape_file, []):
            self.logger.error(f"Update History Step: Failed to clear {self.current_scrape_file}")
        else:
            self.logger.info(f"Update History Step: Cleared {self.current_scrape_file}")
            
        if not self._save_json_data(self.ready_for_discord_file, []):
            self.logger.error(f"Update History Step: Failed to clear {self.ready_for_discord_file}")
        else:
            self.logger.info(f"Update History Step: Cleared {self.ready_for_discord_file}")
            
        self.logger.info(f"Update History Step: Cycle cleanup complete. Master history has {len(self.posted_videos_data)} entries.")

    @tasks.loop(hours=24)
    async def cleanup_posted_videos_task(self) -> None:
        try:
            self.logger.info(f"Running size check for {self.posted_videos_file}.")
            if not self.posted_videos_data: # Ensure data is loaded
                self.logger.warning("posted_videos_data not loaded for cleanup task. Skipping.")
                return

            # Check file size (in MB)
            try:
                file_size_mb = os.path.getsize(self.posted_videos_file) / (1024 * 1024)
                self.logger.info(f"Current size of {self.posted_videos_file}: {file_size_mb:.2f} MB")
                
                # If file is larger than 10MB, archive and trim
                if file_size_mb > 10:
                    self.logger.info(f"File size exceeds 10MB. Creating archive and trimming...")
                    
                    # Create archive with timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    archive_path = os.path.join(DATA_BASE_PATH, f'posted_videos_archive_{timestamp}.json')
                    
                    # Save current data to archive
                    if self._save_json_data(archive_path, self.posted_videos_data):
                        self.logger.info(f"Successfully created archive at {archive_path}")
                        
                        # Keep only the most recent 1000 entries
                        sorted_entries = sorted(
                            self.posted_videos_data.items(),
                            key=lambda x: x[1].get('posted_to_discord_at') or x[1].get('published_at', ''),
                            reverse=True
                        )
                        trimmed_data = dict(sorted_entries[:1000])
                        
                        # Save trimmed data
                        if self._save_json_data(self.posted_videos_file, trimmed_data):
                            self.posted_videos_data = trimmed_data
                            self.logger.info(f"Successfully trimmed history to {len(trimmed_data)} entries")
                        else:
                            self.logger.error("Failed to save trimmed history")
                    else:
                        self.logger.error("Failed to create archive")
                else:
                    self.logger.info("File size is within acceptable limits. No cleanup needed.")
                    
            except OSError as e:
                self.logger.error(f"Error checking file size: {e}")
            
        except Exception as e:
            self.logger.error(f"Error in cleanup_posted_videos_task: {e}", exc_info=True)

    @cleanup_posted_videos_task.before_loop
    async def before_cleanup_posted_videos_task(self) -> None:
        await self.wait_until_ready()
        self.logger.info("Size check task for posted_videos.json is ready.")

    async def get_recent_videos(self, max_results: int = 25, published_after: Optional[datetime] = None) -> Optional[List[Dict[str, Any]]]:
        self.logger.info(f"Fetching up to {max_results} recent videos{' since ' + published_after.isoformat() if published_after else ''}...")
        try:
            uploads_playlist_id = await self.get_uploads_playlist_id()
            if not uploads_playlist_id:
                return None
            
            await self.rate_limiter.acquire()
            request = self.youtube.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=uploads_playlist_id,
                maxResults=max_results
            )
            response = request.execute()

            videos_details = []
            for item in response.get("items", []):
                if item.get("status", {}).get("privacyStatus") == "public":
                    video_id = item.get("contentDetails", {}).get("videoId")
                    snippet = item.get("snippet", {})
                    published_at_raw = snippet.get("publishedAt")
                    
                    if video_id and published_at_raw:
                        try:
                            published_at = datetime.fromisoformat(published_at_raw.replace('Z', '+00:00'))
                            
                            # Skip if before published_after date
                            if published_after and published_at < published_after:
                                self.logger.debug(f"Skipping video {video_id} published at {published_at} (before {published_after})")
                                continue

                            # Skip future-dated videos
                            current_time = datetime.now(timezone.utc)
                            if published_at > current_time:
                                self.logger.warning(f"Skipping future-dated video {video_id} with publish date {published_at} (current time: {current_time})")
                                continue

                            video_info = {
                                "id": video_id,
                                "title": snippet.get("title", "N/A"),
                                "description": snippet.get("description", ""),
                                "published_at": published_at.isoformat(),
                                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", "")
                            }
                            video_info["type"] = "video"
                            videos_details.append(video_info)
                        except ValueError:
                            self.logger.warning(f"Could not parse publishedAt for recent video {video_id}: {published_at_raw}")
                            continue

            self.logger.info(f"Successfully fetched {len(videos_details)} recent videos.")
            self.consecutive_api_errors = 0
            return videos_details

        except HttpError as e:
            self.logger.error(f"HttpError in get_recent_videos: {e.resp.status} - {e.content}", exc_info=True)
            self.consecutive_api_errors += 1
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in get_recent_videos: {e}", exc_info=True)
            self.consecutive_api_errors += 1
            return None
    
    async def get_video_details(self, video_id: str) -> Optional[Dict[str, Any]]:
        """Gets detailed information for a single video, including live status."""
        self.logger.info(f"Fetching details for video ID: {video_id}")
        try:
            await self.rate_limiter.acquire()
            request = self.youtube.videos().list(
                part="snippet,contentDetails,liveStreamingDetails,status",
                id=video_id
            )
            response = request.execute()

            if not response.get("items"):
                self.logger.warning(f"No items found for video ID {video_id} in get_video_details.")
                return None
            
            item = response["items"][0]
            
            snippet = item.get("snippet", {})
            published_at_raw = snippet.get("publishedAt")
            try:
                published_at = datetime.fromisoformat(published_at_raw.replace('Z', '+00:00')).isoformat()
            except:
                # Fallback if parsing fails, though this should be rare with YouTube API
                published_at = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
                self.logger.warning(f"Could not parse publishedAt '{published_at_raw}' for video {video_id}. Using current UTC time.")

            video_details = {
                "id": item["id"],
                "title": snippet.get("title", "N/A"),
                "description": snippet.get("description", ""),
                "published_at": published_at,
                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "tags": snippet.get("tags", []),
                "duration": item.get("contentDetails", {}).get("duration", ""), 
                "live_broadcast_content": snippet.get("liveBroadcastContent", "none"), 
                "actual_start_time": item.get("liveStreamingDetails", {}).get("actualStartTime"),
                "actual_end_time": item.get("liveStreamingDetails", {}).get("actualEndTime"),
                "scheduled_start_time": item.get("liveStreamingDetails", {}).get("scheduledStartTime"),
                "type": "unknown" # Default, will be overridden below
            }
            
            # Determine specific type
            lbc = video_details["live_broadcast_content"]
            title_lower = video_details["title"].lower()
            duration_seconds = self.parse_iso8601_duration(video_details["duration"])
            current_time = datetime.now(timezone.utc)

            # Log detailed information about the video's status
            self.logger.info(f"Video {video_id} details - Title: {video_details['title']}, "
                           f"LiveBroadcastContent: {lbc}, "
                           f"ScheduledStart: {video_details['scheduled_start_time']}, "
                           f"ActualStart: {video_details['actual_start_time']}, "
                           f"ActualEnd: {video_details['actual_end_time']}")

            # Handle premiered videos
            if lbc == "none" and video_details["scheduled_start_time"]:
                # This was a premiere that has ended
                scheduled_start = datetime.fromisoformat(video_details["scheduled_start_time"].replace('Z', '+00:00'))
                if current_time > scheduled_start:
                    self.logger.info(f"Video {video_id} was a premiere that has ended. Treating as regular video.")
                    video_details["type"] = "video"
                else:
                    self.logger.info(f"Video {video_id} is an upcoming premiere. Will be posted when it starts.")
                    video_details["type"] = "upcoming_live"
            elif lbc == "live":
                video_details["type"] = "livestream"
            elif lbc == "upcoming" and video_details["scheduled_start_time"]:
                video_details["type"] = "upcoming_live"
            elif "#short" in title_lower or "#shorts" in title_lower or (video_details["duration"] and duration_seconds > 0 and duration_seconds <= 65):
                video_details["type"] = "short"
            else:
                video_details["type"] = "video"

            self.logger.info(f"Successfully fetched details for video {video_id}, determined type: {video_details['type']}")
            return video_details

        except HttpError as e:
            self.logger.error(f"HttpError in get_video_details for {video_id}: {e.resp.status} - {e.content}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in get_video_details for {video_id}: {e}", exc_info=True)
            return None

    def parse_iso8601_duration(self, duration_str: Optional[str]) -> int:
        """Parses ISO 8601 duration string (e.g., PT1M30S) to seconds."""
        if not duration_str or not duration_str.startswith("PT"):
            return 0
        
        duration_str = duration_str[2:]
        total_seconds = 0
        number_buffer = ""

        for char in duration_str:
            if char.isdigit():
                number_buffer += char
            elif char == 'H' and number_buffer:
                total_seconds += int(number_buffer) * 3600
                number_buffer = ""
            elif char == 'M' and number_buffer:
                total_seconds += int(number_buffer) * 60
                number_buffer = ""
            elif char == 'S' and number_buffer:
                total_seconds += int(number_buffer)
                number_buffer = ""
        return total_seconds

    async def get_uploads_playlist_id(self) -> Optional[str]:
        cached_id = self.playlist_cache.get('uploads_playlist_id')
        if cached_id:
            return cached_id
        try:
            await self.rate_limiter.acquire()
            request = self.youtube.channels().list(
                part='contentDetails', id=self.youtube_channel_id
            )
            response = request.execute()
            if response.get("items"):
                playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                self.playlist_cache.set('uploads_playlist_id', playlist_id)
                return playlist_id
            else:
                self.logger.error(f"No items found for channel ID {self.youtube_channel_id} when fetching uploads playlist ID.")
                return None
        except HttpError as e:
            self.logger.error(f"HttpError fetching uploads playlist ID: {e.resp.status} - {e.content}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching uploads playlist ID: {e}", exc_info=True)
            return None

    def _register_commands(self) -> None:
        @self.tree.command(name="ping", description="Check if the bot is alive")
        async def ping(interaction: discord.Interaction) -> None:
            latency = self.latency * 1000  # milliseconds
            await interaction.response.send_message(f'Pong! Goose Youtube Tracker is alive! Latency: {latency:.2f}ms', ephemeral=True)
            
        @self.tree.command(name="status", description="Check bot and YouTube connection status")
        async def status(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                youtube_status = "Unknown"
                channel_name = "N/A"
                try:
                    await self.rate_limiter.acquire()
                    channel_response = self.youtube.channels().list(part='snippet', id=self.youtube_channel_id).execute()
                    if channel_response.get("items"):
                        channel_name = channel_response['items'][0]['snippet']['title']
                        youtube_status = f"✅ Connected to {channel_name}"
                    else:
                        youtube_status = f"❌ Could not fetch details for channel ID {self.youtube_channel_id}"
                except Exception as yt_e:
                    youtube_status = f"❌ Error connecting to YouTube: {str(yt_e)[:100]}"

                embed = discord.Embed(title="Bot Status", color=discord.Color.green())
                embed.add_field(name="YouTube Connection", value=youtube_status, inline=False)
                embed.add_field(name="Uptime", value=f"<t:{int(self.start_time)}:R>", inline=True)
                embed.add_field(name="Processing Task Active", value=f"{'✅ Running' if self.processing_task.is_running() else '❌ Stopped'}", inline=True)
                embed.add_field(name="Next Processing Cycle", value=f"<t:{int(self.processing_task.next_iteration.timestamp())}:R>" if self.processing_task.is_running() and self.processing_task.next_iteration else "N/A", inline=True)
                embed.add_field(name="Total Videos in History", value=str(len(self.posted_videos_data)), inline=True)
                
                await interaction.followup.send(embed=embed)
            except Exception as e:
                self.logger.error(f"Error in status command: {e}", exc_info=True)
                await interaction.followup.send("❌ Error checking status.", ephemeral=True)

        @self.tree.command(name="scrape", description="Manually trigger a YouTube scrape and check for new videos")
        async def scrape(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                await interaction.followup.send("🔄 Starting manual scrape... This may take a moment.", ephemeral=True)
                
                # Run the full processing cycle
                scraped_videos_list = await self._scrape_youtube_and_save()
                if scraped_videos_list is None:
                    await interaction.followup.send("❌ Error during scraping. Check logs for details.", ephemeral=True)
                    return
                
                videos_to_post_list = await self._compare_and_prepare_posts(scraped_videos_list)
                if videos_to_post_list is None:
                    await interaction.followup.send("❌ Error during comparison. Check logs for details.", ephemeral=True)
                    return
                
                if not videos_to_post_list:
                    await interaction.followup.send(f"✅ Scrape complete. Found {len(scraped_videos_list)} videos, but no new videos to post.", ephemeral=True)
                else:
                    await interaction.followup.send(f"✅ Scrape complete. Found {len(videos_to_post_list)} new video(s) to post. Posting now...", ephemeral=True)
                    processed_for_history_list = await self._post_new_videos(videos_to_post_list)
                    await self._update_master_history_and_cleanup(processed_for_history_list)
                    await interaction.followup.send(f"✅ Posted {len([v for v in processed_for_history_list if v.get('post_status') == 'success'])} video(s) successfully!", ephemeral=True)
                    
            except Exception as e:
                self.logger.error(f"Error in scrape command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error during manual scrape: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="postinghistory", description="View recent posting history")
        async def postinghistory(interaction: discord.Interaction, days: int = 7) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                if not self.posted_videos_data:
                    await interaction.followup.send("📭 No videos in posting history yet.", ephemeral=True)
                    return
                
                # Filter videos by date
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
                recent_videos = []
                
                for video_id, video_data in self.posted_videos_data.items():
                    posted_at_str = video_data.get('posted_to_discord_at')
                    if posted_at_str:
                        try:
                            posted_at = datetime.fromisoformat(posted_at_str.replace('Z', '+00:00'))
                            if posted_at >= cutoff_date:
                                recent_videos.append((video_id, video_data, posted_at))
                        except (ValueError, AttributeError):
                            # Fallback to published_at if posted_to_discord_at is invalid
                            published_at_str = video_data.get('published_at')
                            if published_at_str:
                                try:
                                    published_at = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                                    if published_at >= cutoff_date:
                                        recent_videos.append((video_id, video_data, published_at))
                                except (ValueError, AttributeError):
                                    pass
                
                if not recent_videos:
                    await interaction.followup.send(f"📭 No videos found in the last {days} day(s).", ephemeral=True)
                    return
                
                # Sort by date (most recent first)
                recent_videos.sort(key=lambda x: x[2], reverse=True)
                
                # Create embed with video list
                embed = discord.Embed(
                    title=f"📋 Posting History (Last {days} day(s))",
                    description=f"Found {len(recent_videos)} video(s)",
                    color=discord.Color.blue()
                )
                
                # Discord embeds have a limit of 25 fields and 1024 chars per field
                # Show up to 20 most recent videos
                for i, (video_id, video_data, date) in enumerate(recent_videos[:20]):
                    title = video_data.get('title', 'N/A')[:50]  # Truncate long titles
                    status = video_data.get('post_status', 'unknown')
                    video_type = video_data.get('type', 'video')
                    
                    # Format status emoji
                    status_emoji = {
                        'success': '✅',
                        'skipped_upcoming': '⏭️',
                        'failed_channel_not_found': '❌',
                        'failed_no_permission': '❌',
                        'failed_detail_fetch': '❌',
                        'failed_discord_forbidden': '❌',
                        'failed_discord_http': '❌',
                        'failed_unexpected': '❌',
                        'history_initialized': '📝'
                    }.get(status, '❓')
                    
                    # Format type emoji
                    type_emoji = {
                        'video': '🎥',
                        'short': '🎞️',
                        'livestream': '🔴',
                        'upcoming_live': '⏳'
                    }.get(video_type, '📹')
                    
                    value = f"{status_emoji} {type_emoji} [{title}](https://www.youtube.com/watch?v={video_id})\n"
                    value += f"Posted: <t:{int(date.timestamp())}:R>"
                    
                    embed.add_field(
                        name=f"{i+1}. {title[:40]}",
                        value=value,
                        inline=False
                    )
                
                if len(recent_videos) > 20:
                    embed.set_footer(text=f"Showing 20 of {len(recent_videos)} videos. Use a smaller days parameter to see more.")
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except Exception as e:
                self.logger.error(f"Error in postinghistory command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error retrieving posting history: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="view_posted_videos", description="View all videos in posted_videos.json")
        async def view_posted_videos(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                if not self.posted_videos_data:
                    await interaction.followup.send("📭 `posted_videos.json` is empty.", ephemeral=True)
                    return
                
                embed = discord.Embed(
                    title="📋 Posted Videos Data",
                    description=f"Total videos: {len(self.posted_videos_data)}",
                    color=discord.Color.green()
                )
                
                # Show up to 15 videos to avoid embed size limit (6000 chars)
                # Make output more concise
                for i, (video_id, video_data) in enumerate(list(self.posted_videos_data.items())[:15]):
                    # Handle None values safely
                    if video_data is None:
                        video_data = {}
                    
                    if video_id is None:
                        video_id = 'unknown'
                    
                    title = str(video_data.get('title', 'N/A'))[:50] if video_data.get('title') else 'N/A'
                    status = video_data.get('post_status') or video_data.get('status', 'unknown')
                    video_type = video_data.get('type', 'video')
                    posted_at = video_data.get('posted_to_discord_at')
                    
                    # More concise format
                    status_emoji = '✅' if status == 'success' else '❌' if status and 'failed' in str(status) else '⏭️' if status and 'skipped' in str(status) else '📝' if status == 'history_initialized' else '❓'
                    type_emoji = '🎥' if video_type == 'video' else '🎞️' if video_type == 'short' else '🔴' if video_type == 'livestream' else '📹'
                    
                    # Shorten dates - handle None safely
                    if posted_at and posted_at != 'N/A' and isinstance(posted_at, str):
                        posted_short = posted_at[:10]
                    else:
                        posted_short = 'Never'
                    
                    # Safely truncate video_id
                    video_id_str = str(video_id)
                    video_id_short = video_id_str[:11] if len(video_id_str) > 11 else video_id_str
                    
                    value = f"{status_emoji} {type_emoji} **{status}** | Posted: {posted_short}\n"
                    value += f"[Watch](https://www.youtube.com/watch?v={video_id_str}) | ID: `{video_id_short}...`"
                    
                    embed.add_field(
                        name=f"{i+1}. {title}",
                        value=value,
                        inline=False
                    )
                
                if len(self.posted_videos_data) > 15:
                    embed.set_footer(text=f"Showing 15 of {len(self.posted_videos_data)} videos. Use /postinghistory for more details.")
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except Exception as e:
                self.logger.error(f"Error in view_posted_videos command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error viewing posted videos: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="view_current_scrape", description="View all videos in current_scrape.json")
        async def view_current_scrape(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                if not os.path.exists(self.current_scrape_file):
                    await interaction.followup.send("📭 `current_scrape.json` does not exist.", ephemeral=True)
                    return
                
                with open(self.current_scrape_file, 'r') as f:
                    current_scrape_data = json.load(f)
                
                if not current_scrape_data:
                    await interaction.followup.send("📭 `current_scrape.json` is empty.", ephemeral=True)
                    return
                
                embed = discord.Embed(
                    title="🔍 Current Scrape Data",
                    description=f"Total videos: {len(current_scrape_data)}",
                    color=discord.Color.blue()
                )
                
                # Show up to 15 videos to avoid embed size limit
                for i, video_data in enumerate(current_scrape_data[:15]):
                    video_id = video_data.get('id', 'N/A')
                    title = video_data.get('title', 'N/A')[:50]
                    video_type = video_data.get('type', 'video')
                    published_at = video_data.get('published_at', 'N/A')
                    
                    type_emoji = '🎥' if video_type == 'video' else '🎞️' if video_type == 'short' else '🔴' if video_type == 'livestream' else '📹'
                    published_short = published_at[:10] if published_at != 'N/A' else 'N/A'
                    
                    value = f"{type_emoji} **{video_type}** | Published: {published_short}\n"
                    value += f"[Watch](https://www.youtube.com/watch?v={video_id}) | ID: `{video_id[:11]}...`"
                    
                    embed.add_field(
                        name=f"{i+1}. {title}",
                        value=value,
                        inline=False
                    )
                
                if len(current_scrape_data) > 15:
                    embed.set_footer(text=f"Showing 15 of {len(current_scrape_data)} videos")
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except json.JSONDecodeError as e:
                await interaction.followup.send(f"❌ Error parsing JSON: {str(e)[:200]}", ephemeral=True)
            except Exception as e:
                self.logger.error(f"Error in view_current_scrape command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error viewing current scrape: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="view_ready_for_discord", description="View all videos in ready_for_discord.json")
        async def view_ready_for_discord(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                if not os.path.exists(self.ready_for_discord_file):
                    await interaction.followup.send("📭 `ready_for_discord.json` does not exist.", ephemeral=True)
                    return
                
                with open(self.ready_for_discord_file, 'r') as f:
                    ready_data = json.load(f)
                
                if not ready_data:
                    await interaction.followup.send("📭 `ready_for_discord.json` is empty.", ephemeral=True)
                    return
                
                embed = discord.Embed(
                    title="📤 Ready for Discord Data",
                    description=f"Total videos ready to post: {len(ready_data)}",
                    color=discord.Color.orange()
                )
                
                # Show up to 15 videos to avoid embed size limit
                for i, video_data in enumerate(ready_data[:15]):
                    video_id = video_data.get('id', 'N/A')
                    title = video_data.get('title', 'N/A')[:50]
                    video_type = video_data.get('type', 'video')
                    published_at = video_data.get('published_at', 'N/A')
                    
                    type_emoji = '🎥' if video_type == 'video' else '🎞️' if video_type == 'short' else '🔴' if video_type == 'livestream' else '📹'
                    published_short = published_at[:10] if published_at != 'N/A' else 'N/A'
                    
                    value = f"{type_emoji} **{video_type}** | Published: {published_short}\n"
                    value += f"[Watch](https://www.youtube.com/watch?v={video_id}) | ID: `{video_id[:11]}...`"
                    
                    embed.add_field(
                        name=f"{i+1}. {title}",
                        value=value,
                        inline=False
                    )
                
                if len(ready_data) > 15:
                    embed.set_footer(text=f"Showing 15 of {len(ready_data)} videos")
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except json.JSONDecodeError as e:
                await interaction.followup.send(f"❌ Error parsing JSON: {str(e)[:200]}", ephemeral=True)
            except Exception as e:
                self.logger.error(f"Error in view_ready_for_discord command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error viewing ready for discord: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="sync_commands", description="Manually sync Discord slash commands (admin only)")
        async def sync_commands(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                self.logger.info("Manual command sync requested")
                synced = await self.tree.sync()
                self.logger.info(f"Successfully synced {len(synced)} commands")
                await interaction.followup.send(f"✅ Successfully synced {len(synced)} command(s)! Commands may take a few minutes to appear globally.", ephemeral=True)
            except Exception as e:
                self.logger.error(f"Error syncing commands: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error syncing commands: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="check_video", description="Debug: Check if a specific video ID is in history or can be fetched")
        async def check_video(interaction: discord.Interaction, video_id: str) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                # Remove any URL formatting
                video_id = video_id.replace('https://www.youtube.com/watch?v=', '').replace('https://youtu.be/', '').split('?')[0].split('&')[0]
                
                embed = discord.Embed(title=f"🔍 Video Check: {video_id}", color=discord.Color.blue())
                
                # Check if in posted_videos history
                in_history = video_id in self.posted_videos_data
                if in_history:
                    video_data = self.posted_videos_data[video_id]
                    # Handle both "post_status" and legacy "status" field
                    post_status = video_data.get('post_status', video_data.get('status', 'unknown'))
                    embed.add_field(name="📋 In History", value="✅ Yes", inline=False)
                    embed.add_field(name="Title", value=video_data.get('title', 'N/A'), inline=False)
                    embed.add_field(name="Status", value=post_status, inline=True)
                    embed.add_field(name="Type", value=video_data.get('type', 'unknown'), inline=True)
                    embed.add_field(name="Posted At", value=video_data.get('posted_to_discord_at', 'N/A'), inline=False)
                else:
                    embed.add_field(name="📋 In History", value="❌ No", inline=False)
                
                # Try to fetch video details from YouTube
                try:
                    video_details = await self.get_video_details(video_id)
                    if video_details:
                        embed.add_field(name="📺 YouTube Status", value="✅ Found", inline=False)
                        embed.add_field(name="YouTube Title", value=video_details.get('title', 'N/A'), inline=False)
                        embed.add_field(name="Live Broadcast", value=video_details.get('live_broadcast_content', 'none'), inline=True)
                        embed.add_field(name="Video Type", value=video_details.get('type', 'unknown'), inline=True)
                        embed.add_field(name="Published At", value=video_details.get('published_at', 'N/A'), inline=False)
                        if video_details.get('scheduled_start_time'):
                            embed.add_field(name="Scheduled Start", value=video_details.get('scheduled_start_time', 'N/A'), inline=False)
                        embed.add_field(name="Link", value=f"[Watch on YouTube](https://www.youtube.com/watch?v={video_id})", inline=False)
                    else:
                        embed.add_field(name="📺 YouTube Status", value="❌ Not found or error fetching", inline=False)
                except Exception as e:
                    embed.add_field(name="📺 YouTube Status", value=f"❌ Error: {str(e)[:100]}", inline=False)
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except Exception as e:
                self.logger.error(f"Error in check_video command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error checking video: {str(e)[:200]}", ephemeral=True)

        @self.tree.command(name="force_post", description="Force post a specific video to Discord (bypasses history check)")
        async def force_post(interaction: discord.Interaction, video_id: str) -> None:
            await interaction.response.defer(ephemeral=True)
            try:
                # Remove any URL formatting
                video_id = video_id.replace('https://www.youtube.com/watch?v=', '').replace('https://youtu.be/', '').split('?')[0].split('&')[0]
                
                await interaction.followup.send(f"🔄 Fetching video details for {video_id}...", ephemeral=True)
                
                # Fetch video details
                video_details = await self.get_video_details(video_id)
                if not video_details:
                    await interaction.followup.send(f"❌ Could not fetch video details for {video_id}. Check if the video exists.", ephemeral=True)
                    return
                
                # Create a list with just this video
                videos_to_post = [video_details]
                
                # Use the existing post function
                await interaction.followup.send(f"📤 Posting video to Discord...", ephemeral=True)
                processed_videos = await self._post_new_videos(videos_to_post)
                
                if processed_videos and len(processed_videos) > 0:
                    result = processed_videos[0]
                    post_status = result.get('post_status', 'unknown')
                    
                    if post_status == 'success':
                        await interaction.followup.send(f"✅ Successfully posted video: **{result.get('title', 'N/A')}**\nhttps://www.youtube.com/watch?v={video_id}", ephemeral=True)
                    elif post_status == 'skipped_upcoming':
                        await interaction.followup.send(f"⏭️ Video is an upcoming premiere/live. Not posting yet.", ephemeral=True)
                    else:
                        await interaction.followup.send(f"❌ Failed to post video. Status: {post_status}", ephemeral=True)
                    
                    # Update history
                    await self._update_master_history_and_cleanup(processed_videos)
                else:
                    await interaction.followup.send(f"❌ No video was processed. Check logs for details.", ephemeral=True)
                    
            except Exception as e:
                self.logger.error(f"Error in force_post command: {e}", exc_info=True)
                await interaction.followup.send(f"❌ Error force posting video: {str(e)[:200]}", ephemeral=True)

def main() -> None:
    try:
        intents = discord.Intents.default()
        intents.message_content = True # If any text commands were to be used (though focusing on slash)
        # intents.guilds = True # For guild related events if needed
        # intents.members = True # If member information is needed beyond interaction context
        
        bot = GooseBandTracker(intents)
        bot.run(os.getenv('DISCORD_TOKEN'))
    except ValueError as e: # For missing env vars from _validate_env_vars
        # Logger might not be fully set up if __init__ fails early.
        logging.critical(f"Configuration error: {e}") 
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Fatal error during bot startup: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()