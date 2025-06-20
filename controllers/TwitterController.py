import requests
import json
import re
import time
import pickle
import os
from time import sleep
from datetime import datetime, timedelta, timezone
from datetime import date as dt
from collections import defaultdict
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

class TwitterController:
    """Controller for YOUTUBE API interactions."""
    def __init__(self, TWITTER_BASE_API_URL:str, key:str):
        self.base_url = TWITTER_BASE_API_URL
        print("FacebookController initialized...")
        self.headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
        }
    # Fetch Channel Insights Public data on twitter
    def fetch_channel_insights(self, username):
        """Fetch channel insights for a given date range channel level."""
        params = {"screenname": username}
        response = requests.get(self.base_url+"screenname.php", headers=self.headers, params=params)
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
        
    #focusing on current month timeline
    def get_current_month_media(self,username, rest_id):
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
                response = requests.get(self.base_url+"timeline.php", headers=self.headers, params=params, timeout=10)
                response.raise_for_status()
                raw_data = response.json()
                
                page_media = self.process_media_response(raw_data)
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
                cursor = self.extract_cursor(raw_data)
                if not cursor or month_boundary_crossed:
                    break
                    
                page_count += 1
                sleep(1)
                
            except Exception as e:
                print(f"\nError: {str(e)}")
                break
                
        print(f"\nFound {len(current_month_media)} posts for {current_year}-{current_month:02d}")
        return current_month_media

    def process_media_response(self,raw_data):
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
    
    def extract_cursor(self,response_data):
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
        
    def analyze_current_year_metrics(self,media_data):
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