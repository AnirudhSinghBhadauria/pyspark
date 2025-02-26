from datetime import datetime, timezone, timedelta

def get_videos(youtube, channel_id: str, max_videos: int = 7):
    videos = []
    
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()
    
    uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)
    two_days_ago = now - timedelta(days=2)
    
    next_page_token = None
    
    while True:
        playlist_items = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        
        for item in playlist_items["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_title = item["snippet"]["title"]
            published_at = item["snippet"]["publishedAt"]
            
            video_release_date = datetime.strptime(
                published_at, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
            
            if video_release_date < seven_days_ago:
                return videos
            
            if (
                    seven_days_ago <= video_release_date <= two_days_ago and
                    get_video_duration(youtube, video_id) > 300
               ):
                    videos.append({
                         "id": video_id,
                         "title": video_title,
                         "release": published_at
                    })
                    
                    if len(videos) >= max_videos:
                        return videos
        
        next_page_token = playlist_items.get("nextPageToken")
        
        if not next_page_token:
            break
    
    return videos
