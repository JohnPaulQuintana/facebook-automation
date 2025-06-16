import requests
import time

import pickle
import os
from datetime import date, timedelta
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

#BAJI NPR
API_KEY = "AIzaSyDt9uzOkRGgWFFUSNgsuP-4yzc6JLmCnDw"  # Replace with your YouTube API key
#@baji_npl


#BAJI PKR
# API_KEY = "AIzaSyBSOTjfg7XQhNzcvt3ng2r96O6lhhQB7wA"  # Replace with your YouTube API key
#@baji_pak

CHANNEL_HANDLES = ["@JeetBuzz.Pakistan"]  # Add more handles like ["@baji_pak", "@another_channel"]
# Path to your saved token
token_path = "tokens/token_jeetbuzz_pkr.pkl"

# Load credentials from saved token
def load_credentials(token_path):
    with open(token_path, "rb") as token_file:
        creds = pickle.load(token_file)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds

# Fetch and aggregate YouTube Analytics metrics
def fetch_video_insights(creds, label, start_date, end_date):
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
    totals = {"views": 0,"engagedViews ":0, "likes": 0, "comments": 0, "shares": 0, "subscribersGained": 0, "subscribersLost": 0}

    for row in rows:
        _, views, engagedViews, likes, comments, shares, subscribersGained, subscribersLost = row
        # totals["cardImpressions"] += cardImpressions
        totals["views"] += views
        totals["engagedViews "] += engagedViews 
        totals["likes"] += likes
        totals["comments"] += comments
        totals["shares"] += shares
        totals["subscribersGained"] += subscribersGained
        totals["subscribersLost"] += subscribersLost

    print(f"\n📊 {label.upper()} ({start_date} → {end_date})")
    # print(f"cardImpressions: {totals['cardImpressions']}")
    print(f"Impressions: {totals['views']}")
    print(f"views : {totals['engagedViews ']}")
    print(f"Engagements:")
    print(f"  Likes: {totals['likes']}")
    print(f"  Comments: {totals['comments']}")
    print(f"  Shares: {totals['shares']}")
    print(f"Subscribers:")
    print(f"  Gained: {totals['subscribersGained']}")
    print(f"  Lost: {totals['subscribersLost']}")


from datetime import date, timedelta, datetime
from googleapiclient.discovery import build

def fetch_total_video_insights(creds):
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
    end_date = date.today()
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

    # Step 5: Output
    for row in rows:
        video_id, views, engaged_views, likes, comments, shares = row
        meta = video_meta.get(video_id, {})
        print("\n🎬 Video:")
        print(f"🆔 ID: {video_id}")
        print(f"📅 Published: {meta.get('publishedAt', 'N/A')}")
        print(f"🔗 URL: {meta.get('url')}")
        print(f"📌 Title: {meta.get('title')}")
        print(f"📈 Views (Impressions): {views}")
        print(f"👁️ Engaged Views (Reach): {engaged_views}")
        print(f"👍 Likes: {likes} | 💬 Comments: {comments} | 🔁 Shares: {shares}")



def get_channel_info(handle):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=id,snippet,statistics&forHandle={handle}&key={API_KEY}"
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

def get_all_video_ids(channel_id):
    video_ids = []
    url = f"https://www.googleapis.com/youtube/v3/search?part=id&channelId={channel_id}&maxResults=50&order=date&type=video&key={API_KEY}"
    while url:
        resp = requests.get(url).json()
        video_ids += [item["id"]["videoId"] for item in resp.get("items", []) if item["id"].get("videoId")]
        next_page = resp.get("nextPageToken")
        if next_page:
            url = f"https://www.googleapis.com/youtube/v3/search?part=id&channelId={channel_id}&maxResults=50&order=date&type=video&pageToken={next_page}&key={API_KEY}"
            time.sleep(0.3)
        else:
            break
    return video_ids

def get_video_stats_batch(video_ids):
    stats = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        id_str = ",".join(batch)
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={id_str}&key={API_KEY}"
        resp = requests.get(url).json()
        for item in resp.get("items", []):
            s = item["statistics"]
            stats.append({
                "title": item["snippet"]["title"],
                "views": int(s.get("viewCount", 0)),
                "likes": int(s.get("likeCount", 0)),
                "comments": int(s.get("commentCount", 0)),
            })
        time.sleep(0.3)
    return stats

def summarize_stats(video_stats):
    total_views = sum(v["views"] for v in video_stats)
    total_likes = sum(v["likes"] for v in video_stats)
    total_comments = sum(v["comments"] for v in video_stats)
    engagement = total_likes + total_comments
    engagement_rate = round(engagement / total_views, 4) if total_views else 0
    return {
        "total_views": total_views,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "engagement": engagement,
        "engagement_rate": engagement_rate
    }




# === MAIN ===
for handle in CHANNEL_HANDLES:
    print(f"\n🔍 Analyzing: {handle}")
    info = get_channel_info(handle)
    if not info:
        print("❌ Could not retrieve channel info.")
        continue

    print(f"📛 Channel Title: {info['title']}")
    print(f"👥 Followers (Subscribers): {info['subscribers']}")
    print(f"🎞️  Total Videos (listed in stats): {info['video_count']}")

    video_ids = get_all_video_ids(info["channel_id"])
    print(f"🔎 Videos Fetched: {len(video_ids)}")

    video_stats = get_video_stats_batch(video_ids)
    summary = summarize_stats(video_stats)

    print(f"\n📊 Insight Summary:")
    print(f"   Total Views (Impressions): {summary['total_views']}")
    print(f"   Total Likes: {summary['total_likes']}")
    print(f"   Total Comments: {summary['total_comments']}")
    print(f"   Total Engagement: {summary['engagement']}")
    print(f"   Engagement Rate: {summary['engagement_rate'] * 100:.2f}%")

    print("\n📺 All Videos:")
    for video in sorted(video_stats, key=lambda x: x["views"], reverse=True):
        print(f"   • {video['title'][:60]}... — {video['views']} views, {video['likes']} likes, {video['comments']} comments")


    # Load and fetch analytics
    creds = load_credentials(token_path)
    today = date.today()
    yesterday = today - timedelta(days=1)
    start_month = yesterday.replace(day=1)
    start_year = yesterday.replace(month=1, day=1)

    print(start_month, start_year, yesterday)
    # page level insights
    fetch_video_insights(creds, "daily", yesterday, yesterday)
    fetch_video_insights(creds, "monthly", start_month, yesterday)
    fetch_video_insights(creds, "yearly", start_year, yesterday)

    # daily video insights
    fetch_total_video_insights(creds)