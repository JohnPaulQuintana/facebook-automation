import requests
import json
from datetime import datetime, timedelta
from time import sleep

headers = {
    "x-rapidapi-key": "c5268228e2mshae211db22c994e6p119e97jsnd90c58e89ef2",
    "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
}

url = "https://twitter-api45.p.rapidapi.com/screenname.php"
url_media = "https://twitter-api45.p.rapidapi.com/timeline.php"

def get_user_info(username):
    params = {"screenname": username}
    response = requests.get(url, headers=headers, params=params)
    user_data = response.json()
    # print(user_data)
    if response.status_code == 200 and "status" in user_data:
        
        user_info = {
            "id": user_data['id'],
            "rest_id": user_data['rest_id'],
            "name": user_data['name'],
            "followers_count": user_data['sub_count'],
            "friends_count": user_data['friends'],
            "media_count": user_data['media_count'],
        }
        return user_info
    else:
        print("Error fetching user data:", user_data.get("message", "Unknown error"))
        return None

# currently only supports fetching media for current year
def get_current_year_media(username, rest_id):
    """
    Fetches media posts only for the current year with progress tracking
    Stops when it encounters posts older than current year
    """
    current_year = datetime.now().year
    current_year_media = []
    cursor = None
    page_count = 0
    posts_processed = 0
    
    print(f"\nFetching {current_year} media posts...")
    
    while True:
        params = {"screenname": username,"rest_id": rest_id}
        if cursor:
            params["cursor"] = cursor
            
        try:
            response = requests.get(url_media, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
            
            # Process the current page
            page_media = process_media_response(raw_data)
            posts_processed += len(page_media)
            
            # Filter for current year and check for older posts
            older_posts_found = False
            for media in page_media:
                if media['year'] == current_year:
                    current_year_media.append(media)
                elif media['year'] < current_year:
                    older_posts_found = True
                    break
            
            # Show progress
            print(f"\rPages processed: {page_count + 1} | Current year posts found: {len(current_year_media)} | Total scanned: {posts_processed}", end="", flush=True)
            
            # Stop if we found older posts or no more pages
            cursor = extract_cursor(raw_data)
            if older_posts_found or not cursor:
                break
                
            page_count += 1
            sleep(1)  # Rate limiting
            
        except requests.exceptions.RequestException as e:
            print(f"\nAPI request failed: {str(e)}")
            break
        except Exception as e:
            print(f"\nUnexpected error: {str(e)}")
            break
            
    print(f"\nFinished fetching {len(current_year_media)} current year posts.")
    return current_year_media

#focusing on current month timeline
def get_current_month_media(username, rest_id):
    """
    Fetches ALL media posts for current month (not just most recent)
    Maintains same output format as yearly version
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_month_media = []
    cursor = None
    page_count = 0
    posts_processed = 0
    
    print(f"\nFetching ALL {current_year}-{current_month:02d} media posts...")
    
    while True:
        params = {"screenname": username, "rest_id": rest_id}
        if cursor:
            params["cursor"] = cursor
            
        try:
            response = requests.get(url_media, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
            
            page_media = process_media_response(raw_data)
            posts_processed += len(page_media)
            
            # NEW: Track if we've moved beyond current month
            month_boundary_crossed = False
            
            for media in page_media:
                if media['year'] == current_year and media['month'] == current_month:
                    current_month_media.append(media)
                elif (media['year'] < current_year or 
                     (media['year'] == current_year and media['month'] < current_month)):
                    month_boundary_crossed = True
            
            print(f"\rPages: {page_count+1} | Month Posts: {len(current_month_media)} | Scanned: {posts_processed}", end="", flush=True)
            
            # Continue until no more pages OR we've collected all current month posts
            cursor = extract_cursor(raw_data)
            if not cursor or month_boundary_crossed:
                break
                
            page_count += 1
            sleep(1)
            
        except Exception as e:
            print(f"\nError: {str(e)}")
            break
            
    print(f"\nFound {len(current_month_media)} posts for {current_year}-{current_month:02d}")
    return current_month_media

def extract_cursor(response_data):
    """Extracts the bottom cursor from API response"""
    try:
        timeline = response_data.get('next_cursor', "")
        if timeline:
            return timeline
        else:
            print("No cursor found in response.")
        
        return None
    except Exception as e:
        print(f"Error extracting cursor: {str(e)}")
        return None

def process_media_response(raw_data):
    """Processes a single page of media response"""
    media_metrics = []
    
    try:
        # Safely get timeline list
        timelines = raw_data.get('timeline', []) if isinstance(raw_data, dict) else []
        
        for timeline in timelines:
            if not isinstance(timeline, dict):
                continue
                
            if timeline.get('tweet_id'):
                # Parse date
                try:
                    dt = datetime.strptime(timeline.get('created_at', ''), '%a %b %d %H:%M:%S %z %Y')
                except (ValueError, TypeError):
                    continue
                
                # Initialize media variables
                media_id = ''
                media_url = ''
                media = timeline.get('media', {})
                
                # Handle video media
                if 'video' in media and isinstance(media['video'], dict):
                    video_data = media['video']
                    media_id = video_data.get('id', '')
                    media_url = video_data.get('media_url_https', '')
                # Handle photo media (which is a list of photos)
                elif 'photo' in media and isinstance(media['photo'], list) and len(media['photo']) > 0:
                    photo_data = media['photo'][0]
                    media_id = photo_data.get('id', '')
                    media_url = photo_data.get('media_url_https', '')
                
                # Convert views to int (it comes as string in the data)
                views_str = str(timeline.get('views', '0')).strip()
                views = int(views_str) if views_str.isdigit() else 0
                
                media_metrics.append({
                    'tweet_id': timeline.get('tweet_id', ''),
                    'media_id': media_id,
                    'views': views,
                    'engagements': {
                        'replies': timeline.get('replies', 0),
                        'retweets': timeline.get('retweets', 0),
                        'likes': timeline.get('favorites', 0),
                        'bookmarks': timeline.get('bookmarks', 0),
                        'quotes': timeline.get('quotes', 0)
                    },
                    'title': timeline.get('text', 'N/A'),
                    'media_url': media_url,
                    'created_at': dt.strftime('%Y/%m/%d'),
                    'year': dt.year,
                    'month': dt.month,
                    'day': dt.day
                })
                    
    except Exception as e:
        print(f"\nError processing response: {str(e)}")
        
    return media_metrics

def analyze_current_year_metrics(media_data):
    """Analyzes metrics specifically for current year data"""
    if not media_data:
        return {}
        
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    yesterday = now.date() - timedelta(days=1)

    results = {
        'monthly': {},
        'total': {
            'posts': 0,
            'views': 0,
            'engagements': 0
        },
        'current_month': {
            'posts': 0,
            'views': 0,
            'engagements': 0
        },
        'yesterday': {
            'posts': 0,
            'views': 0,
            'engagements': 0
        }
    }
    
    for media in media_data:
        # Aggregate totals
        views = int(media.get('views', 0) or 0)
        engagements = sum(media['engagements'].values())
        
        results['total']['posts'] += 1
        results['total']['views'] += views
        results['total']['engagements'] += engagements
        
        # Monthly breakdown
        month_key = f"{media['year']}/{media['month']:02d}"
        if month_key not in results['monthly']:
            results['monthly'][month_key] = {
                'posts': 0,
                'views': 0,
                'engagements': 0
            }
        results['monthly'][month_key]['posts'] += 1
        results['monthly'][month_key]['views'] += views
        results['monthly'][month_key]['engagements'] += engagements
        
        # Current month stats
        if media['month'] == current_month:
            results['current_month']['posts'] += 1
            results['current_month']['views'] += views
            results['current_month']['engagements'] += engagements


         # Daily (yesterday) stats
        try:
            media_date = datetime.strptime(media.get("created_at", ""), "%Y/%m/%d").date()
            if media_date == yesterday:
                results['yesterday']['posts'] += 1
                results['yesterday']['views'] += views
                results['yesterday']['engagements'] += engagements
        except Exception:
            continue  # skip malformed dates

    # Calculate averages
    if results['total']['posts'] > 0:
        results['total']['avg_views'] = results['total']['views'] / results['total']['posts']
        results['total']['avg_engagements'] = results['total']['engagements'] / results['total']['posts']
    else:
        results['total']['avg_views'] = 0
        results['total']['avg_engagements'] = 0
    
    return results

def filter_media_last_30_days(media_list):
    today = datetime.today()
    cutoff_date = today - timedelta(days=30)
    
    filtered = []
    for media in media_list:
        try:
            # Parse the media's created_at date
            media_date = datetime.strptime(media.get("created_at", ""), "%Y/%m/%d")
            # Compare to cutoff
            if media_date >= cutoff_date:
                filtered.append(media)
        except ValueError:
            # Skip media if date is invalid or not in expected format
            continue

    return filtered

if __name__ == "__main__":
    # BDT
    # username = "baji_bgd" 
    username = "HDMovie_365" 
    
    # Get user info
    user_info = get_user_info(username)
    print("User Info:", json.dumps(user_info, indent=2))
    
    if user_info:
        rest_id = user_info['rest_id']
        
        # Get current year media only
        current_year_media = get_current_year_media(username,rest_id)
        # current_year_media = get_current_month_media(username,rest_id)
        
        if current_year_media:
            # Analyze metrics
            insights = analyze_current_year_metrics(current_year_media)
            #Analyze 30days periods
            recent_media = filter_media_last_30_days(current_year_media)

            print("\nCurrent Year Insights:")
            print(f"Total Posts: {insights['total']['posts']}")
            print(f"Total Views: {insights['total']['views']}")
            print(f"Total Engagements: {insights['total']['engagements']}")
            print(f"Avg Views/Post: {insights['total']['avg_views']:.1f}")
            print(f"Avg Engagements/Post: {insights['total']['avg_engagements']:.1f}")
            
            print("\nMonthly Breakdown:")
            for month, stats in insights['monthly'].items():
                print(f"{month}: {stats['posts']} posts, {stats['views']} views")
            
            print("\nCurrent Month Stats:")
            print(json.dumps(insights['current_month'], indent=2))
            
            # Save to file
            filename = f"{username}_{datetime.now().year}_{datetime.now().month}_insights.json"
            with open(filename, "w") as f:
                json.dump({
                    "user_info": user_info,
                    "media_data": current_year_media,
                    "recent_media": recent_media,
                    "insights": insights
                }, f, indent=2)
            print(f"\nData saved to {filename}")
        else:
            print("No current year media found.")