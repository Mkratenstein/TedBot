# Goose Band YouTube Discord Tracker

## Overview
This Discord bot tracks the Goose the Band YouTube channel, sending notifications when:
- The channel goes live
- A new video is uploaded
- A new short is posted

## Prerequisites
- Python 3.8+
- Discord Bot Token
- YouTube Data API v3 Key

## Setup
1. Clone the repository
2. Create a virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and fill in your details

### Required Environment Variables
- `DISCORD_TOKEN`: Your Discord bot token
- `DISCORD_CHANNEL_ID`: The Discord channel where notifications will be sent
- `YOUTUBE_CHANNEL_ID`: The YouTube channel ID to track
- `YOUTUBE_API_KEY`: Your YouTube Data API v3 key

## Deployment
This bot is configured for easy deployment on Railway:
1. Connect your GitHub repository
2. Set environment variables in Railway dashboard
3. Choose Python as the deployment environment

## Running Locally
```bash
python TedBot.py
```

## Features
- Real-time YouTube livestream notifications
- New video upload notifications
- YouTube Shorts detection
- 15-minute update interval
- Comprehensive logging for tracking and debugging
- Robust tracking system using `current_tracking.json` (for current session) and `posted_videos.json` (for historical posts) to prevent reposts.
- Automatic cleanup of video posting history (default: entries older than 30 days).
- Simple Discord slash commands:
  - `/ping`: Check if bot is alive
  - `/status`: Check bot and YouTube connection status
  - `/scrape`: Manually trigger a YouTube scrape and check for new videos
  - `/postinghistory`: View recent posting history (default: last 7 days)

## Contributing
Pull requests are welcome. For major changes, please open an issue first.