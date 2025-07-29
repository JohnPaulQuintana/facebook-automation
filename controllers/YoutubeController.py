import requests
import json
import re
import time
import pickle
import os
from datetime import datetime, timedelta, timezone
from datetime import date as dt
from collections import defaultdict
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

class YoutubeController:
    """Controller for YOUTUBE API interactions."""
    def __init__(self, YOUTUBE_BASE_API_URL:str):
        self.base_url = YOUTUBE_BASE_API_URL
        print("FacebookController initialized...")

    # Load credentials from saved token
    def load_credentials(self,token_filename):
        token_path = os.path.join(os.path.dirname(__file__), "..", "tokens", token_filename)
        token_path = os.path.abspath(token_path)

        with open(token_path, "rb") as token_file:
            creds = pickle.load(token_file)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return creds

    # Fetch channel insights for a given date range channel level
    def fetch_channel_insights(self, creds, label, start_date, end_date):
        youtube_analytics = build("youtubeAnalytics", "v2", credentials=creds)

        response = youtube_analytics.reports().query(
            ids="channel==MINE",
            startDate=start_date.isoformat(),
            endDate=end_date.isoformat(),
            metrics="views,engagedViews,likes,comments,shares,subscribersGained,subscribersLost",
            dimensions="day",
            sort="-views"
        ).execute()

        rows = response.get("rows", [])
        print("=====================================================================================")
        print(rows)
        print("=====================================================================================")
        totals = {
            "label": label,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "views": 0,
            "engagedViews": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "engagements":0,
            "subscribersGained": 0,
            "subscribersLost": 0
        }

        for row in rows:
            _, views, engagedViews, likes, comments, shares, subscribersGained, subscribersLost = row
            totals["views"] += views
            totals["engagedViews"] += engagedViews
            totals["likes"] += likes
            totals["comments"] += comments
            totals["shares"] += shares
            totals["engagements"] += (likes + comments + shares)
            totals["subscribersGained"] += subscribersGained
            totals["subscribersLost"] += subscribersLost

        return totals

    #get channel info by username or handle
    def get_channel_info(self,handle,key=None):
        url = f"https://www.googleapis.com/youtube/v3/channels?part=id,snippet,statistics&forHandle={handle}&key={key}"
        resp = requests.get(url).json()
        if "items" in resp and resp["items"]:
            data = resp["items"][0]
            return {
                "channel_id": data["id"],
                "title": data["snippet"]["title"],
                "subscribers": int(data["statistics"].get("subscriberCount", 0)),
                "video_count": int(data["statistics"].get("videoCount", 0))
            }
        return None

    # Fetch all videos with insights for the authenticated user
    def fetch_all_video_with_insights(self, creds):
        youtube_data = build("youtube", "v3", credentials=creds)
        youtube_analytics = build("youtubeAnalytics", "v2", credentials=creds)

        # Step 1: Get all video IDs and metadata
        print("📥 Fetching video metadata...")
        all_videos = []
        video_meta = {}
        next_page_token = None

        while True:
            res = youtube_data.search().list(
                part="id",
                forMine=True,
                type="video",
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            video_ids = [item["id"]["videoId"] for item in res.get("items", [])]
            all_videos.extend(video_ids)

            if video_ids:
                details = youtube_data.videos().list(
                    part="snippet",
                    id=",".join(video_ids)
                ).execute()

                for item in details.get("items", []):
                    vid = item["id"]
                    snippet = item["snippet"]
                    published_date = snippet["publishedAt"].split("T")[0]
                    video_meta[vid] = {
                        "title": snippet["title"],
                        "publishedAt": published_date,
                        "url": f"https://www.youtube.com/watch?v={vid}"
                    }

            next_page_token = res.get("nextPageToken")
            if not next_page_token:
                break

        if not all_videos:
            print("❌ No videos found.")
            return

        # Step 2: Filter videos published in the last 30 days
        end_date = dt.today()
        start_date = end_date - timedelta(days=30)

        recent_videos = [
            vid for vid in all_videos
            if datetime.fromisoformat(video_meta.get(vid, {}).get("publishedAt", "1900-01-01")).date() >= start_date
        ]


        if not recent_videos:
            print("❌ No recent videos published in the last 30 days.")
            return

        print(f"✅ Found {len(recent_videos)} videos from the last 30 days.")

        # Step 3: Fetch analytics
        video_id_list = ",".join(recent_videos)
        print("📊 Fetching analytics data...")
        response = youtube_analytics.reports().query(
            ids="channel==MINE",
            startDate=start_date.isoformat(),
            endDate=end_date.isoformat(),
            metrics="views,engagedViews,likes,comments,shares",
            dimensions="video",
            filters=f"video=={video_id_list}",
            sort="-views"
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            print("❌ No analytics data found.")
            return

        # Step 4: Sort by published date (latest to oldest)
        rows.sort(key=lambda r: video_meta.get(r[0], {}).get("publishedAt", "0000-00-00"), reverse=True)

        video_insights = []

        # Step 5: Output
        for row in rows:
            video_id, views, engaged_views, likes, comments, shares = row
            meta = video_meta.get(video_id, {})
            #format of response
            print("\n🎬 Video:")
            print(f"🆔 ID: {video_id}")
            print(f"📅 Published: {meta.get('publishedAt', 'N/A')}")
            print(f"🔗 URL: {meta.get('url')}")
            print(f"📌 Title: {meta.get('title')}")
            print(f"📈 Views (Impressions): {views}")
            print(f"👁️ Engaged Views (Reach): {engaged_views}")
            print(f"👍 Likes: {likes} | 💬 Comments: {comments} | 🔁 Shares: {shares}")

            insight = {
                "video_id": video_id,
                "published_at": meta.get("publishedAt", "N/A"),
                "url": meta.get("url"),
                "title": meta.get("title"),
                "views": views,
                "engaged_views": engaged_views,
                "likes": likes,
                "comments": comments,
                "shares": shares
            }
            
            video_insights.append(insight)

        return video_insights
    
    # get IG page insights (Followers, Engagements, Impressions and Reach)
    def get_youtube_page_metrics(self,username, api_key, token=None):
        print(f"[INFO] Processing YouTube metrics for Page: {username}, API Key: {api_key}, Token: {token}")
        
        creds = self.load_credentials(f"{token}.pkl")
        today = dt.today()
        yesterday1 = today - timedelta(days=1)
        yesterday = today - timedelta(days=2)  # was `yesterday = today - timedelta(days=1)`
        start_month = yesterday.replace(day=1)
        start_year = yesterday.replace(month=1, day=1)

        # target_date = dt(2025, 7, 5)  # to handle the skiped date issue

        print(yesterday, start_month, start_year)
    
        # Fetch channel info (basic stats)
        channel_info = self.get_channel_info(username,api_key)
        time.sleep(1)  # To avoid hitting API rate limits

        # Fetch insights for skiped date issue
        # daily_insights = self.fetch_channel_insights(creds, "daily", target_date, target_date)
        # monthly_insights = self.fetch_channel_insights(creds, "monthly", start_month, target_date)
        # yearly_insights = self.fetch_channel_insights(creds, "yearly", start_year, target_date)

        # Fetch insights
        daily_insights = self.fetch_channel_insights(creds, "daily", yesterday, yesterday1)
        print("=====================================================================================")
        print("This is Daily Insights:")
        print(daily_insights)
        print("=====================================================================================")
        monthly_insights = self.fetch_channel_insights(creds, "monthly", start_month, yesterday)
        print("=====================================================================================")
        print("This is Monhtly Insights:")
        print(monthly_insights)
        print("=====================================================================================")
        yearly_insights = self.fetch_channel_insights(creds, "yearly", start_year, yesterday)
        print("=====================================================================================")
        print("This is Yearly Insights:")
        print(yearly_insights)
        print("=====================================================================================")

        #get all the videos with insights
        video_insights = self.fetch_all_video_with_insights(creds)
        print(f"[INFO] Successfully fetched YouTube metrics for Page: {username}")

        # Return as structured object
        return {
            "channel": channel_info,
            "daily_insights": daily_insights,
            "monthly_insights": monthly_insights,
            "yearly_insights": yearly_insights,
            "video_insights": video_insights
        }


