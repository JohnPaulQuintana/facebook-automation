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
from controllers.TwitterSheetController import TwitterSpreadsheetController
from controllers.client.ClientSheetController import ClientSheetController

from controllers.FacebookController import FacebookController
from controllers.IGController import IGController
from controllers.YoutubeController import YoutubeController
from controllers.TwitterController import TwitterController

from helpers.IG_Helper import IGHELPER
from helpers.Facebook_Helper import FacebookHelper
from helpers.Youtube_Helper import YoutubeHelper
from helpers.Twitter_Helper import TwitterHelper
from helpers.Client_Helper import ClientHelper
# Load environment variables
load_dotenv()

ACCOUNT_SHEET_ID = os.getenv("ACCOUNT_SHEET_ID")
FB_GAINED_SHEET_ID = os.getenv("FB_GAINED_SHEET_ID")
IG_GAINED_SHEET_ID = os.getenv("IG_GAINED_SHEET_ID")
YT_GAINED_SHEET_ID = os.getenv("YT_GAINED_SHEET_ID")
TW_GAINED_SHEET_ID = os.getenv("TW_GAINED_SHEET_ID")
CLIENT_SHEET_ID = os.getenv("CLIENT_SHEET_ID")

FACEBOOK_BASE_API_URL = os.getenv("FACEBOOK_BASE_API_URL")
YOUTUBE_BASE_API_URL = os.getenv("YOUTUBE_BASE_API_URL")
TWITTER_BASE_API_URL = os.getenv("TWITTER_BASE_API_URL")

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

def get_currency(currency, brand):
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

def extract_sheet_id(url: str) -> str:
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    raise ValueError("Invalid Google Sheets URL")

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
    tw_spreadsheet = TwitterSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    # Initialize client
    client_sheet = ClientSheetController()
    
    # # 1. Get your target rows (from your existing function)
    # target_rows = client_sheet.batch_find_targets(
    #     spreadsheet_id="1UsOJZzhQ71veg5oveCxoiIJCmyIdoQRXHrA7Qs276xE",
    #     tab_configs={
    #         "BAJI BDT": {
    #             "targets": ["FACEBOOK PAGE", "FACEBOOK GROUP", "INSTAGRAM CASINO", "INSTAGRAM SPORTS", "TWITTER","YOUTUBE"],
    #             "start_row": 8,
    #             "column": "B"
    #         }
    #     }
    # )["BAJI BDT"]
    # print("Target rows:", target_rows)
    
    # # 2. Prepare your values (example data)
    # platform_values = {
    #     "FACEBOOK PAGE": ["1500", "1800", "2000"],
    #     "INSTAGRAM CASINO": ["800", "1000", "1200"]
    # }
    
    # # 3. Convert to object format
    # platform_cells = client_sheet.convert_to_object_format(target_rows, platform_values)
    # # Result: {'FACEBOOK PAGE': [{'row':9,'value':'1500'}, ...]}

    # # 4. Execute update
    # results = client_sheet.update_platform_cells(
    #     spreadsheet_id="1UsOJZzhQ71veg5oveCxoiIJCmyIdoQRXHrA7Qs276xE",
    #     tab_name="BAJI BDT",
    #     platform_cells=platform_cells
    # )

    # # 5. Print results
    # print("\nUpdate Results:")
    # for platform, success in results.items():
    #     print(f"{platform:20} {'✓' if success else '✗'}")
    # # Get combined cell references
    # # cell_references = client_sheet.get_target_cells_with_month(
    # #     spreadsheet_id="1UsOJZzhQ71veg5oveCxoiIJCmyIdoQRXHrA7Qs276xE",
    # #     tab_name="BAJI BDT",
    # #     target_texts=["FACEBOOK PAGE", "YOUTUBE"],
    # #     start_row=8,
    # #     search_column="B"
    # # )
    # # print("Cell references:", cell_references)


    accounts = spreadsheet.get_facebook_accounts()
    pages_sp = spreadsheet.get_facebook_pages()
    
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
        twitter_Controller = TwitterController(TWITTER_BASE_API_URL, account[4])
        
        client_helper = ClientHelper()

        if account[0].startswith("FB"):
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
                        ig_spreadsheet.get_ig_spreadsheet_column(IG_GAINED_SHEET_ID,brand,get_currency(currency,brand),ig_page_insights,ig_page_insights[0].get('followers_count', 0), PAGE_TYPE)        

                        #update client sheet
                        # Access monthly insights safely
                        monthly = ig_page_insights[0].get('monthly_insights', {})
                        monthly_impressions = monthly.get('impressions', 0)
                        monthly_engagements = monthly.get('engagements', 0)
                        # TARGET = "INSTAGRAM"
                        # if brand == 'BAJI' and matched_info[10] == 'BDT':
                        #     print("Target")
                        client_helper._process_data(
                                f"{matched_info[2]} {matched_info[10]}", CLIENT_SHEET_ID, "INSTAGRAM", client_sheet, 
                                [ig_page_insights[0].get('followers_count', 0), monthly_impressions, monthly_engagements]
                            )
                    
                    # get the target column and brand name
                    target_column = spreadsheet.get_spreadsheet_column(FB_GAINED_SHEET_ID,brand,currency,followers,followers['followers_count'], PAGE_TYPE)
                    
                    #update client sheet
                    client_helper._process_data(
                            f"{matched_info[2]} {matched_info[10]}", CLIENT_SHEET_ID, matched_info[9], client_sheet, 
                            [followers['followers_count'], followers['page_impressions_monthly'], followers['page_post_engagements_monthly']]
                        )
                    
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
            

            # # INSTAGRAM
            ig_posts_data = ig_Controller.fetch_all_ig_posts(page_tokens, start_date)
            print("This is IG POST...")
            all_ig_insights = ig_Controller.process_all_post_insights(ig_posts_data)
            # Send to ig helper to process insights
            ig_helper = IGHELPER(all_ig_insights)
            print("This is IG HELPER...")
            sorted_data = ig_helper.get_sorted_posts(True)
            ig_helper.process_ig_insights_by_ig_id(sorted_data, pages_info, ig_spreadsheet)



            # # FACEBBOOK
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
                print(chanel_insights)

                # Access safely using .get()
                
                monthly_insights = chanel_insights.get('monthly_insights', {})
                monthly_views = monthly_insights.get('views', 0)
                monthly_engagements = monthly_insights.get('engagements', 0)

                client_helper._process_data(
                            f"{matched_info[2]} {matched_info[10]}", CLIENT_SHEET_ID, matched_info[9], client_sheet, 
                            [chanel_insights.get("channel", {}).get("subscribers", 0), monthly_views, monthly_engagements]
                        )
                
                #process youtube posts insights
                ## yt_spreadsheet.transfer_video_insight_data(extract_sheet_id(matched_info[7]), matched_info[1], chanel_insights.get("video_insights", []), chanel_insights.get("channel", {}).get("subscribers", 0))
                if chanel_insights and isinstance(chanel_insights.get("video_insights"), list):
                    youtube_helper = YoutubeHelper(chanel_insights["video_insights"])
                    youtube_helper.process_youtube_insights_by_page_id(
                        account[0], chanel_insights, matched_info, yt_spreadsheet
                    )
                else:
                    print(f"⚠️ Skipping YouTube for {account[0]} — no valid video insights.")

            else:
                print(f"No matched info found for YouTube channel: {account[0]}")
                continue
        else:
            print(f"Skipping YouTube processing for account: {account[0]}")

        #FOR TWITTER
        if account[0].startswith("TW"):
            chanel_insights = twitter_Controller.fetch_channel_insights(account[3])
            print(chanel_insights)
            if chanel_insights:
                rest_id = chanel_insights['rest_id']
                # use this if the account is new to get the total
                # current_year_media = twitter_Controller.get_current_year_media(account[3],rest_id)
                current_year_media = twitter_Controller.get_current_month_media(account[3],rest_id)
                #mathe the code for youtube channel
                matched_info = next((item for item in pages_sp if item[0] == account[0]), None)

                if current_year_media:
                    # Analyze metrics
                    insights = twitter_Controller.analyze_current_year_metrics(current_year_media)
                    # print(insights)
                    #Analyze 30days periods
                    # recent_media = filter_media_last_30_days(current_year_media)

                    
                    #send it to designated sheet channel level
                    if matched_info and insights:
                        print(f"Matched info for YouTube channel: {matched_info}")
                        user_data = tw_spreadsheet.get_twitter_spreadsheet_column(TW_GAINED_SHEET_ID,matched_info[2],matched_info[1],insights,chanel_insights['followers_count'], matched_info[4])
                        # print(user_data)
                        # update client sheet with twitter monthly insights
                        # Access using get
                        current_month = insights.get('current_month', {})
                        views = current_month.get('views', 0)
                        engagements = current_month.get('engagements', 0)
                        brand_cur = 'DEFAULT'
                        if account[0] == "TW2":
                            print("its a badsha...")
                            brand_cur = f"{matched_info[1]} {matched_info[10]}"
                        else:
                            brand_cur = f"{matched_info[2]} {matched_info[10]}"

                        
                        client_helper._process_data(
                            brand_cur, CLIENT_SHEET_ID, matched_info[9], client_sheet, 
                            [chanel_insights['followers_count'], views, engagements]
                        )
                        #process twitter posts insights
                        twitter_helper = TwitterHelper(current_year_media)
                        twitter_helper.process_twitter_insights_by_page_id(
                            account[0], chanel_insights['followers_count'], current_year_media, matched_info, tw_spreadsheet
                        )
                    else:
                        print(f"No matched info found for YouTube channel: {account[0]}")
                        continue

                    print("\nCurrent Year Insights:")
                    print(f"Total Posts: {insights['total']['posts']}")
                    print(f"Total Views: {insights['total']['views']}")
                    print(f"Total Engagements: {insights['total']['engagements']}")
                    print(f"Avg Views/Post: {insights['total']['avg_views']:.1f}")
                    print(f"Avg Engagements/Post: {insights['total']['avg_engagements']:.1f}")
                    
                    # print("\nMonthly Breakdown:")
                    # for month, stats in insights['monthly'].items():
                    #     print(f"{month}: {stats['posts']} posts, {stats['views']} views")
                    
                    print("\nCurrent Month Stats:")
                    print(json.dumps(insights['current_month'], indent=2))
                
                else:
                    print("No current year media found.")
                    insights = {
                            "current_month": {
                                "views": 0,
                                "engagements": 0
                            }
                        }
                    tw_spreadsheet.get_twitter_spreadsheet_column(TW_GAINED_SHEET_ID,matched_info[2],matched_info[1],insights,chanel_insights['followers_count'], matched_info[4])

            
        else:
            print(f"Skipping YouTube processing for account: {account[0]}")





        ## OLD VERSION OF THE CODE
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