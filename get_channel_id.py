from googleapiclient.discovery import build
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_channel_id():
    # Initialize the YouTube API client
    youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))
    
    # The custom URL handle
    handle = '@goosetheband'
    
    try:
        # Search for the channel using the handle
        request = youtube.search().list(
            part="snippet",
            q=handle,
            type="channel",
            maxResults=1
        )
        response = request.execute()
        
        if response['items']:
            channel_id = response['items'][0]['id']['channelId']
            channel_title = response['items'][0]['snippet']['title']
            print(f"Channel Title: {channel_title}")
            print(f"Channel ID: {channel_id}")
            return channel_id
        else:
            print("No channel found with that handle")
            return None
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    get_channel_id() 