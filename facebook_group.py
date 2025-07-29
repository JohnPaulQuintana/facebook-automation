import requests
from datetime import datetime, timedelta
import time
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union
import re
class FacebookGroupAnalyzer:
    def __init__(self, group_url: str, api_key: str, data_dir: str = "fb_group_data"):
        """
        Initialize the Facebook Group Analyzer
        
        Args:
            group_url: URL of the Facebook group to analyze
            api_key: RapidAPI key for authentication
            data_dir: Directory to store cached data
        """
        self.group_url = group_url
        self.headers = {
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "facebook-pages-scraper2.p.rapidapi.com"
        }
        self.group_info: Dict = {}
        self.all_posts: List[Dict] = []
        self.data_dir = Path(data_dir)
        self.current_year = datetime.now().year
        
        # Ensure data directory exists
        self.data_dir.mkdir(exist_ok=True)
    
    def _get_safe_filename(self, name: str) -> str:
        """Convert a string to a safe filename"""
        return re.sub(r'[^\w\-_. ]', '_', name)
    
    def get_group_filename(self) -> Path:
        """Get filename for group data based on URL"""
        group_name = self._get_safe_filename(self.group_url.split('/')[-2])
        return self.data_dir / f"{group_name}_{self.current_year}_posts.json"
    
    def save_to_json(self, data: Union[Dict, List], filename: Path) -> None:
        """Save data to JSON file with pretty printing"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    def load_from_json(self, filename: Path) -> Optional[Union[Dict, List]]:
        """Load data from JSON file if exists"""
        try:
            if filename.exists():
                with open(filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading {filename}: {e}")
        return None
    
    def fetch_group_details(self, force_refresh: bool = False) -> Optional[Dict]:
        """
        Fetch or load group information with all available details
        
        Args:
            force_refresh: If True, ignore cached data and fetch fresh from API
            
        Returns:
            Dictionary containing all group details or None if failed
        """
        cache_file = self.data_dir / "group_details.json"
        
        if not force_refresh:
            cached_data = self.load_from_json(cache_file)
            if cached_data and cached_data.get('group_url') == self.group_url:
                self.group_info = cached_data
                return self.group_info
        
        try:
            print("Fetching group details from API...")
            response = requests.get(
                "https://facebook-pages-scraper2.p.rapidapi.com/get_facebook_group_details",
                headers=self.headers,
                params={"link": self.group_url, "timezone": "UTC"}
            )
            self.group_info = response.json().get('data', {})
            
            # Enhance group info with additional metadata
            self.group_info.update({
                'group_url': self.group_url,
                'last_updated': datetime.now().isoformat(),
                'data_source': 'API'
            })
            
            self.save_to_json(self.group_info, cache_file)
            print(f"Group details saved to {cache_file}")
            return self.group_info
        except Exception as e:
            print(f"Error fetching group details: {e}")
            return None
    
    def parse_member_count(self, member_text: str) -> int:
        """Convert member count text like '81.7K' to integer"""
        if not member_text:
            return 0
            
        # Remove commas and handle K/M suffixes
        member_text = member_text.replace(",", "").upper()
        
        multiplier = 1
        if 'K' in member_text:
            multiplier = 1000
            member_text = member_text.replace('K', '')
        elif 'M' in member_text:
            multiplier = 1000000
            member_text = member_text.replace('M', '')
        
        try:
            return int(float(member_text) * multiplier)
        except ValueError:
            return 0
    
    def fetch_all_posts(self, force_refresh: bool = False) -> List[Dict]:
        """
        Fetch all posts from current year with pagination or load from cache
        
        Args:
            force_refresh: If True, ignore cached data and fetch fresh from API
            
        Returns:
            List of all posts from current year
        """
        cache_file = self.get_group_filename()
        
        if not force_refresh:
            cached_posts = self.load_from_json(cache_file)
            if cached_posts:
                self.all_posts3 = cached_posts
                print(f"Loaded {len(self.all_posts)} posts from cache")
                return self.all_posts
        
        self.all_posts = []
        has_next = True
        end_cursor = None
        
        print("Fetching posts from API...")
        while has_next:
            try:
                querystring = {
                    "link": self.group_url,
                    "timezone": "UTC",
                    "end_cursor": end_cursor if end_cursor else ""
                }
                
                response = requests.get(
                    "https://facebook-pages-scraper2.p.rapidapi.com/get_facebook_group_posts_details",
                    headers=self.headers,
                    params=querystring
                )
                data = response.json()
                
                if 'data' in data and 'posts' in data['data']:
                    # Filter posts from current year
                    current_year_posts = [
                        post for post in data['data']['posts'] 
                        if datetime.strptime(post['values']['publish_time'], "%Y-%m-%dT%H:%M:%SZ").year == self.current_year
                    ]
                    
                    self.all_posts.extend(current_year_posts)
                    print(f"Fetched {len(current_year_posts)} posts (Total: {len(self.all_posts)})")
                    
                    # Check pagination
                    has_next = data['data']['page_info']['has_next']
                    end_cursor = data['data']['page_info']['end_cursor'] if has_next else None
                    
                    # Rate limiting
                    time.sleep(1)
                    
                    # Stop if we've crossed into previous year
                    if current_year_posts and len(current_year_posts) < len(data['data']['posts']):
                        break
                        
            except Exception as e:
                print(f"Error fetching data: {e}")
                break
        
        # Save to cache
        if self.all_posts:
            enhanced_posts = {
                'metadata': {
                    'group_url': self.group_url,
                    'year': self.current_year,
                    'last_updated': datetime.now().isoformat(),
                    'total_posts': len(self.all_posts)
                },
                'posts': self.all_posts
            }
            self.save_to_json(enhanced_posts, cache_file)
            print(f"Saved {len(self.all_posts)} posts to {cache_file}")
                
        return self.all_posts
    
    def calculate_engagement(self, post: Dict) -> int:
        """Calculate total engagement for a post (reactions + comments + shares)"""
        try:
            reactions = post.get('reactions', {}).get('total_reaction_count', 0)
            comments = int(post.get('details', {}).get('comments_count', 0))
            shares = int(post.get('details', {}).get('share_count', 0))
            return reactions + comments + shares
        except (KeyError, ValueError):
            return 0
    
    def analyze_engagement(self) -> Optional[Dict]:
        """
        Analyze engagement metrics and recent posts
        
        Returns:
            Dictionary containing comprehensive analysis results
        """
        if not self.group_info or not self.all_posts:
            print("Please fetch group details and posts first")
            return None
            
        now = datetime.now()
        current_month = now.month
        thirty_days_ago = now - timedelta(days=30)
        seven_days_ago = now - timedelta(days=7)
        yesterday = now - timedelta(days=1)
        
        # Initialize metrics with all group details
        metrics = {
            'metadata': {
                'generated_at': now.isoformat(),
                'group_url': self.group_url,
                'year_analyzed': self.current_year,
                'analysis_duration_days': (now - datetime(now.year, 1, 1)).days
            },
            'group_info': {
                **self.group_info,  # Include all original group details
                'parsed_member_count': self.parse_member_count(self.group_info.get('group_member_count', '0')),
                'created_date': datetime.fromtimestamp(int(self.group_info.get('created_time', 0))).strftime('%Y-%m-%d'),
                'age_years': (now - datetime.fromtimestamp(int(self.group_info.get('created_time', 0)))).days / 365.25
            },
            'engagement_metrics': {
                'yearly': 0,
                'monthly': 0,
                'weekly': 0,
                'daily': 0,
                'average_daily': 0,
                'engagement_rate': 0,
                'posts_per_day': 0,
                'engagement_per_post': 0
            },
            'post_metrics': {
                'total_this_year': len(self.all_posts),
                'last_30_days': [],
                'count_last_30_days': 0,
                'count_last_7_days': 0,
                'count_last_1_day': 0,
                'with_media_count': 0,
                'with_high_engagement': 0  # > median engagement
            }
        }
        
        # First pass to calculate all engagements
        engagements = []
        for post in self.all_posts:
            try:
                post_date = datetime.strptime(post['values']['publish_time'], "%Y-%m-%dT%H:%M:%SZ")
                engagement = self.calculate_engagement(post)
                engagements.append(engagement)
                
                metrics['engagement_metrics']['yearly'] += engagement
                
                if post_date.month == current_month:
                    metrics['engagement_metrics']['monthly'] += engagement
                
                if post_date >= seven_days_ago:
                    metrics['engagement_metrics']['weekly'] += engagement
                    metrics['post_metrics']['count_last_7_days'] += 1
                
                if post_date >= yesterday:
                    metrics['engagement_metrics']['daily'] += engagement
                    metrics['post_metrics']['count_last_1_day'] += 1
                
                if post_date >= thirty_days_ago:
                    metrics['post_metrics']['count_last_30_days'] += 1
                    has_media = post['values'].get('is_media', 'None') != "None"
                    if has_media:
                        metrics['post_metrics']['with_media_count'] += 1
            except Exception as e:
                print(f"Error processing post: {e}")
                continue
        
        # Calculate median engagement for high engagement threshold
        median_engagement = sorted(engagements)[len(engagements) // 2] if engagements else 0
        
        # Second pass for recent posts with additional metrics
        for post in (p for p in self.all_posts 
                    if datetime.strptime(p['values']['publish_time'], "%Y-%m-%dT%H:%M:%SZ") >= thirty_days_ago):
            try:
                post_date = datetime.strptime(post['values']['publish_time'], "%Y-%m-%dT%H:%M:%SZ")
                engagement = self.calculate_engagement(post)
                days_ago = (now - post_date).days
                
                post_data = {
                    'id': post['details']['post_id'],
                    'link': post['details']['post_link'],
                    'date': post_date.strftime("%Y-%m-%d"),
                    'days_ago': days_ago,
                    'text': post['values'].get('text', '')[:100] + "..." if post['values'].get('text') else "No text",
                    'engagement': engagement,
                    'reactions': post.get('reactions', {}).get('total_reaction_count', 0),
                    'comments': int(post.get('details', {}).get('comments_count', 0)),
                    'shares': int(post.get('details', {}).get('share_count', 0)),
                    'has_media': post['values'].get('is_media', 'None') != "None",
                    'is_high_engagement': engagement > median_engagement,
                    'ocr_text': post['values'].get('ocr_text'),
                    'play_count': post['details'].get('play_count')
                }
                metrics['post_metrics']['last_30_days'].append(post_data)
                
                if post_data['is_high_engagement']:
                    metrics['post_metrics']['with_high_engagement'] += 1
            except Exception as e:
                print(f"Error processing recent post: {e}")
                continue
        
        # Calculate derived metrics
        days_in_year = metrics['metadata']['analysis_duration_days']
        member_count = metrics['group_info']['parsed_member_count']
        
        metrics['engagement_metrics']['average_daily'] = (
            metrics['engagement_metrics']['yearly'] / days_in_year if days_in_year > 0 else 0
        )
        metrics['engagement_metrics']['engagement_rate'] = (
            (metrics['engagement_metrics']['monthly'] / member_count) * 100 if member_count > 0 else 0
        )
        metrics['engagement_metrics']['posts_per_day'] = (
            len(self.all_posts) / days_in_year if days_in_year > 0 else 0
        )
        metrics['engagement_metrics']['engagement_per_post'] = (
            metrics['engagement_metrics']['yearly'] / len(self.all_posts) if self.all_posts else 0
        )
        
        # Sort recent posts by date (newest first)
        metrics['post_metrics']['last_30_days'].sort(key=lambda x: x['days_ago'])
        
        # Save analysis results with timestamp
        analysis_file = self.data_dir / f"analysis_{self.group_info.get('group_id', '')}_{now.strftime('%Y%m%d_%H%M%S')}.json"
        self.save_to_json(metrics, analysis_file)
        print(f"Saved comprehensive analysis to {analysis_file}")
        
        return metrics

# Example Usage
if __name__ == "__main__":
    API_KEY = "c5268228e2mshae211db22c994e6p119e97jsnd90c58e89ef2"  # Replace with your actual API key
    GROUP_URL = "https://www.facebook.com/groups/gieldagryplanszowe"
    
    analyzer = FacebookGroupAnalyzer(GROUP_URL, API_KEY)
    
    # Step 1: Fetch or load group details (with all metadata)
    group_info = analyzer.fetch_group_details()
    
    # Step 2: Fetch or load posts
    analyzer.fetch_all_posts()
    
    # Step 3: Analyze engagement (automatically saves results)
    metrics = analyzer.analyze_engagement()
    
    print(f"\nAnalysis complete! Data saved in JSON format in '{analyzer.data_dir}' directory.")