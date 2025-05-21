import requests
import json
import re
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
# Calculate date range (Jan 1st to today)
# Use UTC instead of local time
end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

class FacebookController:
    
    """Controller for Facebook API interactions."""
    def __init__(self, FACEBOOK_BASE_API_URL:str, account:list):
        self.account = account
        self.base_url = FACEBOOK_BASE_API_URL
        print("FacebookController initialized...")
    
    def get_facebook_pages(self):
        try:
            print("Fetching pages from Facebook...")
            params = {
                'access_token': self.account[4]
            }
            response = requests.get(self.base_url+self.account[5]+"/accounts", params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses
            return response.json()      # Returns a dict
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
    

    def _get_posts_for_page(self, page_id, page_token, since, until):
        """Fetch posts for a single page with pagination handling"""
        print(f"Requesting: {page_id}")
        all_posts = []
        url = f"{self.base_url}/{page_id}/posts"
        
        # params = {
        #     'access_token': page_token,
        #     'fields': 'id,message,created_time',
        #     'since': since,# Format: "2025-01-01"
        #     'until': until, # today
        #     'limit': 100  # Maximum per request
        # }

        # for debugging
        params = {
            'access_token': page_token,
            'fields': 'id,message,created_time',
            'since': f"{since}",# Format: "2025-01-01"
            'until': f"{end_date}", # today
            'limit': 100  # Maximum per request
        }

        
        
        while url:
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                all_posts.extend(data.get('data', []))
                
                # Paginate WITHOUT resetting params
                url = data.get('paging', {}).get('next')
            except requests.exceptions.RequestException as e:
                print(f"Error fetching posts for page {page_id}: {str(e)}")
                break
            except json.JSONDecodeError:
                print(f"Invalid JSON response from page {page_id}")
                break
        
        return all_posts

    def fetch_all_posts_for_pages(self, page_tokens, since, until):
        print("Page Tokens")
        print(page_tokens)
        all_posts = []
        for page_id, page_token in page_tokens:
            try:
                posts = self._get_posts_for_page(page_id, page_token, since, until)
                for post in posts:
                    all_posts.append({
                        'source_page_id': page_id,  # Track origin page
                        'source_page_token': page_token,  # Keep token for later use
                        'post_id': post.get('id'),
                        'created_time': post.get('created_time'),
                        'message': (post.get('message') or '')[:200]
                    })
            except Exception as e:
                print(f"Error processing page {page_id}: {str(e)}")
        return all_posts

    def process_posts_and_get_insights(self, posts, page_token, page_id):
        """Process posts and get insights with complete error protection"""
        insights_data = []
        
        # Filter posts for the specific page and validate structure
        page_posts = []
        for post in posts:
            if not isinstance(post, dict):
                continue
            if post.get('page_id') == page_id and post.get('post_id'):
                page_posts.append(post)
        
        # Process in batches of 50 (Facebook's limit)
        for i in range(0, len(page_posts), 50):
            batch = page_posts[i:i+50]
            post_ids = [p['post_id'] for p in batch if p.get('post_id')]
            
            try:
                insights_batch = self.get_insights_batch(post_ids, page_token)
                for post, insights in zip(batch, insights_batch):
                    try:
                        post_data = post.copy()
                        post_data['insights'] = self._parse_insights(insights.get('data', []))
                        insights_data.append(post_data)
                    except Exception as parse_error:
                        print(f"Error parsing insights for post {post.get('post_id')}: {str(parse_error)}")
                        post_data = post.copy()
                        post_data['insights'] = self._create_empty_insights()
                        insights_data.append(post_data)
            except Exception as batch_error:
                print(f"Error processing batch {i//50}: {str(batch_error)}")
                # Add posts without insights
                insights_data.extend([{**p, 'insights': self._create_empty_insights()} for p in batch])
                continue
                
        return insights_data

    def _create_empty_insights(self):
        """Return default empty insights structure"""
        return {
            'impressions': 0,
            'reach': 0,
            'reactions': 0,
            'clicks': 0
        }

    def _parse_insights(self, insights_list):
        """Safely parse insights data with comprehensive checks"""
        metrics = self._create_empty_insights()
        # print("Parsing insights data...")
        # print(insights_list)
        if not isinstance(insights_list, list):
            return metrics
            
        for metric in insights_list:
            # print("Parsing metric:", metric)
            try:
                name = metric.get('name')
                values = metric.get('values', [{}])
                value = values[0].get('value', 0) if values else 0
                
                if name == 'post_impressions':
                    metrics['impressions'] = int(value) if value else 0
                elif name == 'post_impressions_unique':
                    metrics['reach'] = int(value) if value else 0
                elif name == 'post_reactions_by_type_total':
                    if isinstance(value, dict):
                        metrics['reactions'] = sum(int(v) for v in value.values())
                    else:
                        metrics['reactions'] = int(value) if value else 0
                elif name == 'post_clicks':
                    metrics['clicks'] = int(value) if value else 0

            except Exception as e:
                print(f"Error parsing metric {name}: {str(e)}")
                continue
                
        return metrics

    def get_insights_batch(self, post_ids, page_token):
        """Get insights for a batch of posts with robust error handling"""
        if not post_ids:
            return []
            
        batch_requests = []
        base_params = f"metric=post_impressions,post_impressions_unique,post_reactions_by_type_total,post_clicks&access_token={page_token}"
        
        for post_id in post_ids:
            batch_requests.append({
                "method": "GET",
                "relative_url": f"{post_id}/insights?{base_params}"
            })
        
        try:
            response = requests.post(
                f"{self.base_url}",
                data={
                    'access_token': page_token,
                    'batch': json.dumps(batch_requests)
                },
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Batch request failed: {str(e)}")
            # Return empty insights for all requested posts
            return [{'data': []} for _ in post_ids]
    

    def process_all_pages_insights(self, posts_data):
        """Process insights for all posts while maintaining page associations"""
        
        
        # Group posts by their source page
        page_groups = defaultdict(list)
        for post in posts_data:
            key = (post['source_page_id'], post['source_page_token'])
            page_groups[key].append(post)
        
        # Process each page's posts
        all_insights = []
        for (page_id, page_token), posts in page_groups.items():
            print(f"Processing {len(posts)} posts for page {page_id}")
            
            # Process in batches of 50
            for i in range(0, len(posts), 50):
                batch = posts[i:i+50]
                post_ids = [p['post_id'] for p in batch]
                
                try:
                    insights_batch = self.get_insights_batch(post_ids, page_token)
                    print(f"Fetched insights for batch {i//50 + 1}")
                    # print(insights_batch)
                    for post, insights in zip(batch, insights_batch):
                        # print(f"Processing insights for post {post['post_id']}")
                        # print(insights)
                        body = json.loads(insights.get('body', '{}'))
                        parsed_metrics = self._parse_insights(body.get('data', []))
                        # post['insights'] = self._parse_insights(insights.get('data', []))
                        # parsed = self._parse_insights(insights.get('data', []))
                        post['insights'] = parsed_metrics
                        post['post_link'] = f"https://www.facebook.com/{post['source_page_id']}/posts/{post['post_id']}?view=insights"
                        all_insights.append(post)
                        # print(all_insights)
                except Exception as e:
                    print(f"Error processing batch: {str(e)}")
                    for post in batch:
                        post['insights'] = self._create_empty_insights()
                        all_insights.append(post)
        
        return all_insights

    def get_facebook_page_metrics(self, page_id, page_access_token, date="2025-05-11"):
        """
        Fetches Facebook Page metrics including:
        - Followers count (lifetime total)
        - Daily post engagements, impressions, reach, page views, and new likes
        """
        try:
            print("Fetching Facebook Page followers and daily insights...")
            
            # Step 1: Get followers count (existing functionality)
            followers_params = {
                'access_token': page_access_token,
                'fields': 'followers_count'
            }
            followers_response = requests.get(
                f"{self.base_url}{page_id}",
                params=followers_params
            )
            followers_response.raise_for_status()
            followers_data = followers_response.json()

            # Step 2: Get daily insights (your new metrics)
            insights_params = {
                'access_token': page_access_token,
                'metric': 'page_post_engagements,page_impressions,'
                        'page_impressions_unique,page_views_total,page_fan_adds, page_fans',
                'period': 'day',
                'since': date,
                'until': date
            }
            insights_response = requests.get(
                f"{self.base_url}{page_id}/insights",
                params=insights_params
            )
            insights_response.raise_for_status()
            insights_data = insights_response.json().get('data', [])

            # Parse insights
            metrics = {}
            for entry in insights_data:
                if entry['period'] == 'day':
                    metrics[entry['name']] = entry['values'][0]['value']

            # Combine both responses
            print
            return {
                'date': date,
                'id': followers_data.get('id', 0),
                'followers_count': followers_data.get('followers_count', 0),
                'post_engagements': metrics.get('page_post_engagements', 0),
                'total_impressions': metrics.get('page_impressions', 0),
                'total_reach': metrics.get('page_impressions_unique', 0),
                'page_views': metrics.get('page_views_total', 0),
                'total_likes_today': metrics.get('page_fans', 0),
                'new_likes_today': metrics.get('page_fan_adds', 0)
            }

        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
        
    
        