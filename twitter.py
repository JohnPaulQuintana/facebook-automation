import requests
import json

headers = {
    "x-rapidapi-key": "c5268228e2mshae211db22c994e6p119e97jsnd90c58e89ef2",
    "x-rapidapi-host": "twitter241.p.rapidapi.com"
}

url = "https://twitter241.p.rapidapi.com/user"
url_media = "https://twitter241.p.rapidapi.com/user-media"
params = {"username": "baji_bgd"}

def get_user_info():
    response = requests.get(url, headers=headers, params=params)
    raw_user_data = response.json()
    # print("Raw User Data:", raw_user_data)
    
    if response.status_code == 200 and "result" in raw_user_data:
        user_data = raw_user_data['result']['data']['user']['result']
        
        # Extract fields
        user_info = {
            "id": user_data['id'],
            "rest_id": user_data['rest_id'],
            "name": user_data['legacy']['name'],
            "followers_count": user_data['legacy']['followers_count'],
            "friends_count": user_data['legacy']['friends_count'],
            "media_count": user_data['legacy']['media_count'],
        }
        return user_info
    else:
        print("Error fetching user data:", raw_user_data.get("message", "Unknown error"))
        return None


def get_user_media(rest_id):
    """
    Fetches user media posts and reliably extracts:
    - Media ID (media_key)
    - Views count
    - Engagement metrics
    """
    params = {"user": rest_id, "count": 50}
    
    try:
        # Make the API request
        response = requests.get(url_media, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        
        # Debug: Print the API response structure
        # print("API Response Structure:", json.dumps(raw_data, indent=2)[:1000] + "...")
        
        # Navigate through the response structure
        timeline = raw_data.get('result', {}).get('timeline', {})
        instructions = timeline.get('instructions', [])
        
        # Find the correct instruction containing entries
        entries = []
        for instruction in instructions:
            if instruction.get('type') == 'TimelineAddEntries':
                entries = instruction.get('entries', [])
                break
                
        if not entries:
            print("Found Instructions:", [i.get('type') for i in instructions])
            return []
            
        media_metrics = []
        
        for entry in entries:
            try:
                # Skip non-tweet entries
                if not entry.get('entryId', '').startswith('tweet-'):
                    continue
                    
                # Safely navigate the nested structure
                tweet = (entry.get('content', {})
                        .get('itemContent', {})
                        .get('tweet_results', {})
                        .get('result', {}))
                
                if not tweet:
                    continue
                    
                legacy = tweet.get('legacy', {})
                extended_entities = legacy.get('extended_entities', {})
                media_items = extended_entities.get('media', [])
                
                if not media_items:
                    continue
                    
                # Extract metrics for each media item
                for media in media_items:
                    media_metrics.append({
                        'tweet_id': tweet.get('rest_id'),
                        'media_id': media.get('media_key'),
                        'type': media.get('type'),
                        'views': tweet.get('views', {}).get('count', 'N/A'),
                        'engagements': {
                            'replies': legacy.get('reply_count', 0),
                            'retweets': legacy.get('retweet_count', 0),
                            'likes': legacy.get('favorite_count', 0),
                            'bookmarks': legacy.get('bookmark_count', 0),
                            'quotes': legacy.get('quote_count', 0)
                        },
                        'media_url': media.get('media_url_https'),
                        'display_url': media.get('display_url'),
                        'created_at': legacy.get('created_at')
                    })
                    
            except Exception as e:
                print(f"Error processing entry {entry.get('entryId')}: {str(e)}")
                continue
                
        return media_metrics if media_metrics else None
        
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None
    
    
if __name__ == "__main__":
    user_info = get_user_info()
    print("User Info:", user_info)

    rest_id = user_info.get('rest_id') if user_info else None
    if rest_id:
        print("User Rest ID:", rest_id)
        user_media = get_user_media(rest_id)
        if user_media:
            print("User Media Metrics:", user_media)
        else:
            print("Failed to retrieve user media metrics.")
    else:
        print("Failed to retrieve user info.")
        