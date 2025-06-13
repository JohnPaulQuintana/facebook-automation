import os
import json
from collections import defaultdict
import re
from datetime import datetime, timedelta, timezone
# from datetime import datetime, timedelta
from dotenv import load_dotenv

# imported class
from controllers.SpreadSheetController import SpreadsheetController
from controllers.IGSpreadSheetController import IGSpreadsheetController
from controllers.YoutubeSheetController import YoutubeSpreadsheetController
from controllers.FacebookController import FacebookController
from controllers.IGController import IGController
from controllers.YoutubeController import YoutubeController
from helpers.IG_Helper import IGHELPER
from helpers.Facebook_Helper import FacebookHelper
from helpers.Youtube_Helper import YoutubeHelper
# Load environment variables
load_dotenv()

ACCOUNT_SHEET_ID = os.getenv("ACCOUNT_SHEET_ID")
FB_GAINED_SHEET_ID = os.getenv("FB_GAINED_SHEET_ID")
IG_GAINED_SHEET_ID = os.getenv("IG_GAINED_SHEET_ID")
YT_GAINED_SHEET_ID = os.getenv("YT_GAINED_SHEET_ID")

FACEBOOK_BASE_API_URL = os.getenv("FACEBOOK_BASE_API_URL")
YOUTUBE_BASE_API_URL = os.getenv("YOUTUBE_BASE_API_URL")
SPREADSHEET_RANGE = os.getenv("SPREADSHEET_RANGE")
RAJI_ACCOUNT = os.getenv("RAJI_ACCOUNT")
debug_dir = "debug_batches"
os.makedirs(debug_dir, exist_ok=True)

def get_date_range_from_env():
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    # Load from .env or fallback to yesterday
    SINCE = os.getenv("SINCE", str(yesterday))
    UNTIL = os.getenv("UNTIL", str(yesterday))

    try:
        since_date = datetime.strptime(SINCE, "%Y-%m-%d")
        until_date = datetime.strptime(UNTIL, "%Y-%m-%d") + timedelta(days=1)
    except ValueError:
        raise ValueError("Invalid date format in SINCE or UNTIL. Expected YYYY-MM-DD.")

    return int(since_date.timestamp()), int(until_date.timestamp())

def main():
    print("Begin the automation for followers gain....")

    #ON DEVELOPMENT
    # Get today's date string
    # today_str = datetime.now().strftime('%d/%m/%Y') # Current date

    #ON DEPLOYED
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    today_str = yesterday.strftime('%Y-%m-%d') #Yesterday date
    
    # read the spreadsheet data
    spreadsheet = SpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    ig_spreadsheet = IGSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    yt_spreadsheet = YoutubeSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    accounts = spreadsheet.get_facebook_accounts()
    pages_sp = spreadsheet.get_facebook_pages()
    # print(pages_sp)
    for account in accounts:
        
        
        # Verify if the account is active and token is valid
        # print(f"Processing account: {account[0]} with name: {account[3]}")
        # token_validator = FacebookTokenValidator(FACEBOOK_BASE_API_URL,account[6], account[7])
        # token_info = token_validator.check_token_validity(account[4])
        # print(f"Token info: {token_info}")
        #end of token validation
    

        facebookController = FacebookController(FACEBOOK_BASE_API_URL ,account)
        ig_Controller = IGController(FACEBOOK_BASE_API_URL)
        youtube_Controller = YoutubeController(YOUTUBE_BASE_API_URL)

        pages = facebookController.get_facebook_pages_with_instagram()
        # print(pages)
        pages_info = []  # Array of page info objects

        # get only badsha pages for this account ragi:
        if account[5] == RAJI_ACCOUNT:
            print(f"Processing account: {account[0]} with name: {account[3]} (RAJI ACCOUNT)")
            pages['data'] = [page for page in pages.get('data', []) if page.get('name') == 'Badsha']

        for page in pages.get('data', []):
            page_id = page.get('id', 0)
            page_access_token = page.get('access_token', 'xxxxxxxxxxxxx')
            ig = page.get('instagram_business_account', False)
            ig_id = ig.get('id', False) if ig else False
            # print(f"Page ID: {page_id}, Access Token: {page_access_token}, Instagram Business Account ID: {ig_id}")
            
            # Match page_id to index 3
            matched_info = next((item for item in pages_sp if item[3] == page_id), None)

            if matched_info:
                currency = matched_info[1]
                brand = matched_info[2]
                PAGE_TYPE = matched_info[4]
                SPREAD_SHEET = matched_info[5]#facebook sheet
                IG_SHEET = matched_info[6]

                # Extract spreadsheet ID
                followers = facebookController.get_facebook_page_metrics(page_id, page_access_token, today_str)
                print(f"Page ID: {page_id}, Followers: {followers}, Currency: {currency}, Brand: {brand}, Page Type: {PAGE_TYPE}")
                
                ig_page_insights = ig_Controller.get_ig_page_metrics(page_id,ig_id,page_access_token)
                if ig_page_insights:
                    print("------------------------------------------------------------------------------")
                    print(ig_page_insights)
                    print("------------------------------------------------------------------------------")

                    #processing ig page insights
                    ig_spreadsheet.get_ig_spreadsheet_column(IG_GAINED_SHEET_ID,brand,currency,ig_page_insights,ig_page_insights[0].get('followers_count', 0), PAGE_TYPE)        
                
                 # get the target column and brand name
                target_column = spreadsheet.get_spreadsheet_column(FB_GAINED_SHEET_ID,brand,currency,followers,followers['followers_count'], PAGE_TYPE)
                # print(target_column)
                # Build the page info object
                page_info = {
                    "page_id": page_id,
                    "instagram_id": ig_id,
                    "access_token": page_access_token,
                    "currency": currency,
                    "brand": brand,
                    "page_type": PAGE_TYPE,
                    "followers": followers,
                    "ig_followers": ig_page_insights[0].get('followers_count', 0) if ig_page_insights else 0,
                    "target_column": target_column,
                    "spreadsheet": SPREAD_SHEET,
                    "ig_spreadsheet": IG_SHEET
                }

                pages_info.append(page_info)

            else:
                print(f"Page ID: {page_id} not found in page_info_list.")

            # transfer it to designated spreadsheet

     
        # 1. Get all pages and their tokens
        # page_tokens = [(page['id'], page['access_token']) for page in pages.get('data', [])]
        page_tokens = [
            (
                page['id'],
                page.get('access_token'),
                page.get('instagram_business_account', {}).get('id')  # This could be None
            )
            for page in pages.get('data', [])
        ]

        # 2. Fetch all posts (now with page tracking)
        # # Get the current year and today’s date
        # current_year = datetime.now().year
        # today_date = datetime.now().strftime('%Y-%m-%d')
        # # Set the starting date to January 1st of the current year
        # start_date = f"{current_year}-01-01"
        #new updates
        # Get today’s date
        today = datetime.now()
        today_date = today.strftime('%Y-%m-%d')

        # Set the start date to 30 days before today
        start_date = (today - timedelta(days=31)).strftime('%Y-%m-%d')  # 29 to include today as the 30th day
        

        # INSTAGRAM
        ig_posts_data = ig_Controller.fetch_all_ig_posts(page_tokens, start_date)
        print("This is IG POST...")
        all_ig_insights = ig_Controller.process_all_post_insights(ig_posts_data)
        # Send to ig helper to process insights
        ig_helper = IGHELPER(all_ig_insights)
        print("This is IG HELPER...")
        sorted_data = ig_helper.get_sorted_posts(True)
        ig_helper.process_ig_insights_by_ig_id(sorted_data, pages_info, ig_spreadsheet)



        # FACEBBOOK
        posts_data = facebookController.fetch_all_posts_for_pages(page_tokens, start_date, today_date)
        all_facebook_insights = facebookController.process_all_pages_insights(posts_data)
        #Send to facebook helper to process insights
        print("This is FACEBOOK HELPER...")
        facebook_helper = FacebookHelper(all_facebook_insights)
        sorted_data = facebook_helper.get_sorted_posts(True)
        facebook_helper.process_facebook_insights_by_page_id(sorted_data, pages_info, spreadsheet)
        # print(sorted_data)

        #YOUTUBE
        if account[0].startswith("YT"):
            chanel_insights = youtube_Controller.get_youtube_page_metrics(account[3], account[4], account[8])
            print(chanel_insights)
            #mathe the code for youtube channel
            matched_info = next((item for item in pages_sp if item[0] == account[0]), None)
            #send it to designated sheet channel level
            if matched_info:
                print(f"Matched info for YouTube channel: {matched_info}")
                yt_spreadsheet.get_youtube_spreadsheet_column(YT_GAINED_SHEET_ID,matched_info[2],matched_info[1],chanel_insights,chanel_insights.get("channel", {}).get("subscribers", 0), matched_info[4])

                #process youtube posts insights
                # yt_spreadsheet.transfer_video_insight_data()
                youtube_helper = YoutubeHelper(chanel_insights.get("video_insights", []))
                youtube_helper.process_youtube_insights_by_page_id(
                    account[0], chanel_insights, matched_info, yt_spreadsheet
                )
            else:
                print(f"No matched info found for YouTube channel: {account[0]}")
                continue
        else:
            print(f"Skipping YouTube processing for account: {account[0]}")

        # #commented for now for ig development
        # # 3. Process insights for ALL pages while maintaining associations
        # all_insights = facebookController.process_all_pages_insights(posts_data)
        # # print(all_insights)
        
        # # Sort all_insights by created_time in descending order (newest first)
        # all_insights.sort(
        #     key=lambda x: datetime.strptime(x['created_time'], '%Y-%m-%dT%H:%M:%S%z'),
        #     reverse=True
        # )
        # # Sort all_insights by created_time in ascending order (oldest first)
        # # all_insights.sort(
        # #     key=lambda x: datetime.strptime(x['created_time'], '%Y-%m-%dT%H:%M:%S%z')
        # # )
        
        # # Group all insights by page_id
        # insights_by_page = defaultdict(list)
        # for insight in all_insights:
        #     page_id = insight['source_page_id']
        #     insights_by_page[page_id].append(insight)

        # # Convert pages_info to a dict for fast lookup
        # pages_info_map = {page['page_id']: page for page in pages_info}

        # # Now process each page_id group
        # for page_id, insights in insights_by_page.items():
        #     # matched_info = next((item for item in pages_sp if item[3] == page_id), None)
        #     # if not matched_info:
        #     #     print(f"Page ID {page_id} not found in pages list")
        #     #     continue

        #     # CURRENCY = matched_info[1]
        #     # BRAND = matched_info[2]
        #     # PAGE_TYPE = matched_info[4]
        #     # SPREAD_SHEET = matched_info[5]
        #     # Get the page info from the map
        #     matched_info = pages_info_map.get(page_id)
        #     if not matched_info:
        #         print(f"Page ID {page_id} not found in pages list")
        #         continue

        #     CURRENCY = matched_info["currency"]
        #     BRAND = matched_info["brand"]
        #     PAGE_TYPE = matched_info["page_type"]
        #     FOLLOWERS = matched_info["followers"]
        #     SPREAD_SHEET = matched_info["spreadsheet"]

        #     # Extract spreadsheet ID
        #     match = re.search(r"/d/([a-zA-Z0-9-_]+)", SPREAD_SHEET)
        #     if not match:
        #         print(f"Invalid spreadsheet URL for {BRAND}")
        #         continue
        #     spreadsheet_id = match.group(1)

        #     print(f"\n🔄 Processing {len(insights)} insights for {BRAND} (page {page_id})")

        #     try:
        #         # Step 1: Ensure headers exist (handles inserting post_id headers)
        #         # spreadsheet.transfer_insight_header_only(spreadsheet_id, CURRENCY, insights)
        #         # print(f"✅ Header transfer completed for {BRAND} (page {page_id})")

        #         # Step 2: Transfer ALL insights in one call (unified row 4 update)
        #         spreadsheet.transfer_insight_data(spreadsheet_id, CURRENCY, insights, FOLLOWERS)
        #         print(f"✅ Insight data transfer completed for {BRAND} (page {page_id}) Folowers: {FOLLOWERS}")

        #         spreadsheet.hide_old_rows(spreadsheet_id, CURRENCY)
        #         print(f"✅ OLD ROWS HIDDEN {BRAND} (page {page_id}) Folowers: {FOLLOWERS}")
        #     except Exception as e:
        #         print(f"❌ Failed processing {BRAND} (page {page_id}): {str(e)}")

        # # # Define the folder and create it if it doesn't exist
        # output_folder = "insight_results"
        # os.makedirs(output_folder, exist_ok=True)

        # # Use the current timestamp or date to make the filename unique
        # filename = f"insights_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        # file_path = os.path.join(output_folder, filename)
        # # Save the data as JSON
        # with open(file_path, 'w', encoding='utf-8') as f:
        #     json.dump(all_insights, f, ensure_ascii=False, indent=4)


    print("Facebook Automation completed:")


# Run the main function
if __name__ == "__main__":
    main()