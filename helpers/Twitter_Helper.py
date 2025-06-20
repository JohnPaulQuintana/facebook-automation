import re
from collections import defaultdict

class TwitterHelper:
    def __init__(self, post_data_list: list):
        self.posts = [self._parse_post(data) for data in post_data_list]

    # Parses a single post data dictionary into a structured format.
    def _parse_post(self, data: dict):
        engagements = data.get("engagements", {})
        return {
            "tweet_id": data.get("tweet_id"),
            "created_at": data.get("created_at"),
            "title": data.get("title"),
            "media_url": data.get("media_url"),
            "insights": 
                self._parse_insights(
                    data.get("views", 0),
                    engagements.get("likes", 0),
                    engagements.get("replies", 0),
                    engagements.get("retweets", 0),
                    engagements.get("bookmarks", 0),
                    engagements.get("quotes", 0),
                )
        }
    
     # Parses insights data from a post into a structured format.
    def _parse_insights(self, views, likes, replies, retweets, bookmarks, quotes):
        return {
            "views": views,
            "impressions": 0,
            "reactions": sum([likes, replies, retweets, bookmarks, quotes])
        }
    
    def process_twitter_insights_by_page_id(self, code, followers,all_insights, pages_info, spreadsheet) -> bool:
        try:
            print(code, pages_info)
            CURRENCY = pages_info[1]
            BRAND = pages_info[2]
            FOLLOWERS = followers
            SPREADSHEET = pages_info[8]

            # Extract spreadsheet ID from the URL
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", SPREADSHEET)
            if not match:
                print(f"❌ Invalid spreadsheet URL for {BRAND}")

            spreadsheet_id = match.group(1)
            print(f"\n🔄 Processing {len(all_insights)} insights for {BRAND}")
            
            try:
                # spreadsheet.transfer_insight_header_only(spreadsheet_id, CURRENCY, insights)
                spreadsheet.transfer_timeline_insight_data(spreadsheet_id, CURRENCY, all_insights, FOLLOWERS)
                spreadsheet.hide_old_rows(spreadsheet_id, CURRENCY)

                print(f"✅ Insight data transfer completed for {BRAND} Followers: {FOLLOWERS}")
            except Exception as e:
                print(f"❌ Failed processing {BRAND}: {str(e)}")

            return True

        except Exception as e:
            print(f"❌ Unexpected error during processing: {str(e)}")
            return False