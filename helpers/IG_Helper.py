import re
from collections import defaultdict
class IGHELPER:
    def __init__(self, post_data_list: list):
        self.posts = [self._parse_post(data) for data in post_data_list]

    # Parses a single post data dictionary into a structured format.
    def _parse_post(self, data: dict):
        return {
            "source_page_id": data.get("source_page_id"),
            "source_ig_id": data.get("source_ig_id"),
            "source_page_token": data.get("source_page_token"),
            "post_id": data.get("post_id"),
            "created_time": data.get("created_time"),
            "caption": data.get("caption"),
            "media_url": data.get("media_url"),
            "insights": self._parse_insights(data.get("insights", {}))
        }
    
    # Parses insights data from a post into a structured format.
    def _parse_insights(self, insights: dict):
        return {
            "reach": insights.get("reach", 0),
            "impressions": insights.get("impressions", 0),
            "reactions": insights.get("reactions", 0)
        }
    

    def get_currency(self, currency, brand):
        curr = None
        if currency == "PKR" and brand=='BAJI':
            curr = "bajilive.casino"
        elif currency == "NPR" and brand=='BAJI':
            curr = "baji.sports"
        elif currency == "BDT" and brand=='JEETBUZZ':
            curr="jeetbuzzcasino"
        elif currency=="INR" and brand=="JEETBUZZ":
            curr="jeetbuzzsports"
        elif currency=="PKR" and brand=="SIX6S":
            curr="six6s.sport"
        elif currency=="INR" and brand=="SIX6S":
            curr="six6s.casino"
        return curr

    # Returns a list of all posts (NOT USED)
    def get_all_posts(self):
        return self.posts

    # returns a post by its ID (NOT USED)
    def get_post_by_id(self, post_id: str):
        return next((post for post in self.posts if post["post_id"] == post_id), None)

    # Filters post by caption keyword (NOT USED)
    def filter_by_caption_keyword(self, keyword: str):
        return [post for post in self.posts if keyword.lower() in post.get("caption", "").lower()]

    # Summarizes posts into a list of dictionaries with selected fields (NOT USED)
    def summarize_posts(self):
        return [
            {
                "post_id": post["post_id"],
                "created_time": post["created_time"],
                "reach": post["insights"]["reach"],
                "impressions": post["insights"]["impressions"],
                "reactions": post["insights"]["reactions"]
            }
            for post in self.posts
        ]
    
    # return sorted posts by created time.
    def get_sorted_posts(self, reverse: bool = False):
        return sorted(self.posts, key=lambda x: x["created_time"], reverse=reverse)
    

    def process_ig_insights_by_ig_id(self, all_insights, pages_info, spreadsheet) -> bool:
        try:
            
            # Step 1: Group insights by source_ig_id
            insights_by_ig_id = defaultdict(list)
            for insight in all_insights:
                ig_id = insight.get('source_ig_id')
                if ig_id:
                    insights_by_ig_id[ig_id].append(insight)

            # Step 2: Create lookup map from pages_info
            pages_info_map = {page['instagram_id']: page for page in pages_info if 'instagram_id' in page}

            # Step 3: Process each IG ID group
            for ig_id, insights in insights_by_ig_id.items():
                matched_info = pages_info_map.get(ig_id)
                if not matched_info:
                    print(f"⚠️ IG ID {ig_id} not found in pages list")
                    continue

                CURRENCY = matched_info["currency"]
                BRAND = matched_info["brand"]
                FOLLOWERS = matched_info["ig_followers"]
                SPREADSHEET = matched_info["ig_spreadsheet"]

                # Extract spreadsheet ID from the URL
                match = re.search(r"/d/([a-zA-Z0-9-_]+)", SPREADSHEET)
                if not match:
                    print(f"❌ Invalid spreadsheet URL for {BRAND}")
                    continue

                spreadsheet_id = match.group(1)
                print(f"\n🔄 Processing {len(insights)} insights for {BRAND} (IG {ig_id})")

                try:
                    # spreadsheet.transfer_insight_header_only(spreadsheet_id, CURRENCY, insights)
                    spreadsheet.transfer_insight_data(spreadsheet_id, self.get_currency(CURRENCY, BRAND), insights, FOLLOWERS)
                    spreadsheet.hide_old_rows(spreadsheet_id, self.get_currency(CURRENCY, BRAND))

                    print(f"✅ Insight data transfer completed for {BRAND} (IG {ig_id}) Followers: {FOLLOWERS}")
                except Exception as e:
                    print(f"❌ Failed processing {BRAND} (IG {ig_id}): {str(e)}")

            return True

        except Exception as e:
            print(f"❌ Unexpected error during processing: {str(e)}")
            return False