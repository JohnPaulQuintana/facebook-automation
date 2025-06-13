import requests
import os
import time
import re
import random
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from config.config import Config
from datetime import datetime, timedelta, timezone
from typing import Optional

# Load environment variables
load_dotenv()

class SpreadsheetController:
    def __init__(self, spreadsheet, range=None):
        self.spreadsheet = spreadsheet
        self.range = range if range else "ACCOUNTS!A3:I"

    def get_facebook_accounts(self):
        print("Fetching accounts from spreadsheet...")
        config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range=self.range).execute()
            values = result.get('values', [])
            if not values:
                print("No data found.")
            else:
                # for row in values:
                #     print(row)  # Process each row as needed
                return values
        except HttpError as err:
            print(f"An error occurred: {err}")

        # return "controller data"

    def get_facebook_pages(self):
        print("Fetching pages from spreadsheet...")
        config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range="PAGES!A2:H").execute()
            values = result.get('values', [])
            if not values:
                print("No data found.")
            else:
                # for row in values:
                #     print(row)  # Process each row as needed
                return values
        except HttpError as err:
            print(f"An error occurred: {err}")

    def get_spreadsheet_column(self, spreadsheet_id: str, tab_name: str, currency: str, insights:list, total_followers: int = 0 ,page_type: str = "page"):
        try:
            # Initialize service
            service = self._initialize_google_sheets_service()
            
            # Get sheet metadata and ID
            sheet_id = self._get_sheet_id(service, spreadsheet_id, tab_name)
            if sheet_id is None:
                return None

            #ON DEVELOPMENT
            # Get today's date string
            # today_str = datetime.now().strftime('%d/%m/%Y') # Current date

            #ON DEPLOYED
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)
            today_str = yesterday.strftime('%d/%m/%Y') #Yesterday date

            # Check or create date column
            date_col_index = self._handle_date_column(
                service, spreadsheet_id, sheet_id, tab_name, today_str
            )
            
            # Get all values from sheet
            values = self._get_sheet_values(service, spreadsheet_id, tab_name)
            if not values:
                return None

            # Find currency row
            currency_row_index = self._find_currency_row(values, currency, page_type)
            if currency_row_index is None:
                return None

            # Get value from column E for difference calculation
            value_in_column_e = self._get_value_from_column_e(values, currency_row_index, currency)
            if value_in_column_e is None:
                return None

            # Update cells with today's date, total followers, and difference
            self._update_sheet_values(
                service, spreadsheet_id, sheet_id, tab_name,
                today_str, currency_row_index, insights,total_followers, value_in_column_e
            )
            
            return values

        except HttpError as err:
            print(f"An error occurred: {err}")
            return None

    def _initialize_google_sheets_service(self):
        """Initialize and return the Google Sheets service."""
        print("Initializing Google Sheets service...")
        config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(config_dict, scopes=scope)
        return build('sheets', 'v4', credentials=creds)

    def _get_sheet_id_copy(self, service, spreadsheet_id, tab_name):
        """Get the sheet ID for the given tab name."""
        print(f"Getting sheet ID for tab '{tab_name}'...")
        spreadsheet_meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for s in spreadsheet_meta['sheets']:
            if s['properties']['title'] == tab_name:
                return s['properties']['sheetId']
        print(f"Sheet/tab '{tab_name}' not found.")
        return None

    def _handle_date_column(self, service, spreadsheet_id, sheet_id, tab_name, today_str):
        """Check if date column exists or create it, returning the column index."""
        print(f"Handling date column for {today_str}...")
        sheet = service.spreadsheets()
        
        # Check if date exists in headers
        header_result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!1:1"
        ).execute()
        headers = header_result.get('values', [[]])[0]

        if today_str in headers:
            print(f"Column for {today_str} already exists.")
            return headers.index(today_str)
        
        # Create new date column
        print(f"Creating new column for {today_str}...")
        date_col_index = 3  # Column D
        requests = [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": date_col_index,
                        "endIndex": date_col_index + 1
                    },
                    "inheritFromBefore": True
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": date_col_index,
                        "endColumnIndex": date_col_index + 1
                    },
                    "cell": {
                        "userEnteredValue": {"stringValue": today_str},
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0},
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            }
                        }
                    },
                    "fields": "userEnteredValue,userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": date_col_index,
                        "endColumnIndex": date_col_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER"
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment)"
                }
            }
        ]

        body = {"requests": requests}
        sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        return date_col_index

    def _get_sheet_values(self, service, spreadsheet_id, tab_name):
        """Get all values from the specified sheet tab."""
        print("Getting sheet values...")
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, 
            range=f"{tab_name}!A1:E"
        ).execute()
        values = result.get('values', [])
        if not values:
            print("No data found.")
        return values

    def _find_currency_row(self, values, currency, page_type):
        """Find the row index for the specified currency."""
        print(f"Finding row for currency '{currency}'...")
        for i, row in enumerate(values):
            currency_with_type = currency + "-" + page_type if page_type == "NEW" else currency
            if row and row[0] == currency_with_type:
                return i + 1  # sheet rows are 1-indexed
        print(f"Currency '{currency}' not found.")
        return None

    def _get_value_from_column_e(self, values, currency_row_index, currency):
        """Get the value from column E for difference calculation."""
        print("Getting value from column E...")
        if len(values[currency_row_index - 1]) > 4:
            return float(values[currency_row_index - 1][4].replace(',', ''))
        print(f"No value found in column E for the currency '{currency}'.")
        return None

    def safe_execute_update(self, func, retries=5, delay=2, backoff=2):
        for attempt in range(retries):
            try:
                return func()  # get the request object
                # return request.execute()  # execute here
            except HttpError as e:
                if e.resp.status == 429 and attempt < retries - 1:
                    wait_time = delay * (backoff ** attempt)
                    print(f"[429] Rate limit hit. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    raise

    def _update_sheet_values(self, service, spreadsheet_id, sheet_id, tab_name, 
                            today_str, currency_row_index, insights, total_followers, value_in_column_e):
        print("Updating sheet values...")
        sheet = service.spreadsheets()

        # Values
        #default is 0 if negative
        # difference = max(0, total_followers - value_in_column_e)
        difference = total_followers - value_in_column_e

        values_only = [
            insights.get('page_post_engagements_day', 0),
            insights.get('page_post_engagements_monthly', 0),
            insights.get('yearly_page_post_engagements', 0),
            insights.get('page_impressions_day', 0),
            insights.get('page_impressions_monthly', 0),
            insights.get('yearly_page_impressions', 0),
            insights.get('page_impressions_unique_day', 0),
            insights.get('page_impressions_unique_monthly', 0),
            insights.get('yearly_page_impressions_unique', 0),
            insights.get('page_views_total_day', 0),
            insights.get('page_views_total_monthly', 0),
            insights.get('yearly_page_views_total', 0),
            insights.get('total_likes_today', 0),
            insights.get('new_likes_today', 0),
        ]

        all_values = [[today_str], [total_followers], [difference]] + [[v] for v in values_only]

        # Batch update all values from D{currency_row_index-1} to D{currency_row_index+15}
        data = [
            {
                "range": f"{tab_name}!D{currency_row_index-1}:D{currency_row_index+15}",
                "values": all_values
            }
        ]
        self.safe_execute_update(lambda: sheet.values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data}
        ).execute())

        # Format date cell
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": currency_row_index - 2,
                        "endRowIndex": currency_row_index - 1,
                        "startColumnIndex": 3,
                        "endColumnIndex": 4
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                            }
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,textFormat.foregroundColor)"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": currency_row_index - 1,
                        "endRowIndex": currency_row_index + 16,
                        "startColumnIndex": 3,
                        "endColumnIndex": 4
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "#,##0"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]
        self.safe_execute_update(lambda: service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute())


        print(f"Updated values and formatted rows {currency_row_index-1} to {currency_row_index+15}")
        
    def _update_sheet_values_old(self, service, spreadsheet_id, sheet_id, tab_name, 
                            today_str, currency_row_index, insights, total_followers, value_in_column_e):
        """Update the sheet with today's date, total followers, and difference."""
        print("Updating sheet values...")
        sheet = service.spreadsheets()
        
        # Update today's date
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!D{currency_row_index-1}",
            valueInputOption="RAW",
            body={"values": [[today_str]]}
        ).execute()

        # Format the cell
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": currency_row_index - 2,
                        "endRowIndex": currency_row_index - 1,
                        "startColumnIndex": 3,
                        "endColumnIndex": 4
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "foregroundColor": {
                                    "red": 1.0,
                                    "green": 1.0,
                                    "blue": 1.0
                                }
                            }
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,textFormat.foregroundColor)"
                }
            }
        ]

        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

        # Update total followers
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!D{currency_row_index}",
            valueInputOption="RAW",
            body={"values": [[total_followers]]}
        ).execute()

        # Calculate and update difference
        difference = max(0, total_followers - value_in_column_e)
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!D{currency_row_index + 1}",
            valueInputOption="RAW",
            body={"values": [[difference]]}
        ).execute()

        # Update insights
        print("Updating insights...")
        print(insights)
        values_only = [
            # insights['followers_count'],
            insights.get('page_post_engagements_day', 0),
            insights.get('page_post_engagements_monthly', 0),
            insights.get('yearly_page_post_engagements', 0),
            insights.get('page_impressions_day', 0),
            insights.get('page_impressions_monthly', 0),
            insights.get('yearly_page_impressions', 0),
            insights.get('page_impressions_unique_day', 0),
            insights.get('page_impressions_unique_monthly', 0),
            insights.get('yearly_page_impressions_unique', 0),
            insights.get('page_views_total_day',0),
            insights.get('page_views_total_monthly',0),
            insights.get('yearly_page_views_total',0),
            insights.get('total_likes_today',0),
            insights.get('new_likes_today',0),
            # insights.get('total_reach',0),
        ]
        start_row = currency_row_index + 2       # e.g., row 7
        for i, value in enumerate(values_only):
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!D{start_row + i}",
                valueInputOption="RAW",
                body={"values": [[value]]}
            ).execute()

        print(f"Updated values for rows {currency_row_index-1} to {currency_row_index+1}")

    def _get_sheet_id(self, service, spreadsheet_id: str, tab_name: str) -> int:
        """Helper method to get sheetId from tab name"""
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields='sheets.properties'
        ).execute()
        
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == tab_name:
                return sheet['properties']['sheetId']
        
        raise ValueError(f"Sheet '{tab_name}' not found in spreadsheet")


    # This is for insight process but not beeing used for now
    def generate_header(self,insights_list):
        """Generate a header row: 'Date' followed by unique post_ids."""
        post_ids = []
        seen = set()

        for item in insights_list:
            post_id = item.get('post_id')
            if post_id and post_id not in seen:
                post_ids.append(post_id)
                seen.add(post_id)

        return ["Date"] + post_ids
    
    def transfer_insight_header_only(self, spreadsheet_id: str, tab_name: str, insights_data: list) -> bool:
        """
        Batch insert new post_id headers at the beginning (after 'Date') in a single update.
        Existing post_ids are not duplicated.
        Each new post_id adds 3 columns: Reach, Impressions, Reactions.
        Returns True if headers were updated, False otherwise.
        """
        try:
            service = self._initialize_google_sheets_service()
            sheet = service.spreadsheets()

            # 1. Extract post_ids and post_links in order from insights_data
            post_id_link_pairs = []
            seen = set()
            for insight in insights_data:
                post_id = insight.get('post_id')
                post_link = insight.get('post_link')
                if post_id and post_id not in seen:
                    post_id_link_pairs.append((post_id, post_link))
                    seen.add(post_id)

            # 2. Get existing header (first 3 rows)
            header_result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!1:3",
                majorDimension='ROWS'
            ).execute()

            header_rows = header_result.get('values', [[''], [''], ['']])
            main_header = header_rows[0] if len(header_rows) > 0 else []
            post_link_row = header_rows[1] if len(header_rows) > 1 else []
            sub_header = header_rows[2] if len(header_rows) > 2 else []

            # Normalize header lengths
            max_len = max(len(main_header), len(post_link_row), len(sub_header))
            main_header += [''] * (max_len - len(main_header))
            post_link_row += [''] * (max_len - len(post_link_row))
            sub_header += [''] * (max_len - len(sub_header))

            # Detect existing post_ids
            existing_post_ids = set()
            col = 1
            while col < len(main_header):
                pid = main_header[col]
                if pid:
                    existing_post_ids.add(pid)
                    col += 3
                else:
                    col += 1

            # Prepare batch update requests
            requests = []
            sheet_id = self._get_sheet_id(service, spreadsheet_id, tab_name)
            insert_index = 1  # Always insert after 'Date'

            new_headers = [(pid, link) for pid, link in reversed(post_id_link_pairs) if pid not in existing_post_ids]

            for pid, link in new_headers:
                col_index = insert_index

                # Insert 3 columns for this post
                requests.append({
                    'insertDimension': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': col_index,
                            'endIndex': col_index + 3
                        },
                        'inheritFromBefore': False
                    }
                })

                # Add post_id header
                requests.append({
                    'updateCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': col_index,
                            'endColumnIndex': col_index + 3
                        },
                        'rows': [{
                            'values': [
                                {'userEnteredValue': {'stringValue': pid}},
                                {},
                                {}
                            ]
                        }],
                        'fields': 'userEnteredValue'
                    }
                })

                # Add post_link in second row
                requests.append({
                    'updateCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': col_index,
                            'endColumnIndex': col_index + 3
                        },
                        'rows': [{
                            'values': [
                                {'userEnteredValue': {'stringValue': link or ''}},
                                {},
                                {}
                            ]
                        }],
                        'fields': 'userEnteredValue'
                    }
                })

                # Add subheaders: Reach, Impressions, Reactions
                requests.append({
                    'updateCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 3,
                            'startColumnIndex': col_index,
                            'endColumnIndex': col_index + 3
                        },
                        'rows': [{
                            'values': [
                                {'userEnteredValue': {'stringValue': 'Reach'}},
                                {'userEnteredValue': {'stringValue': 'Impressions'}},
                                {'userEnteredValue': {'stringValue': 'Reactions'}}
                            ]
                        }],
                        'fields': 'userEnteredValue'
                    }
                })

                # Merge the header cells
                requests.append({
                    'mergeCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': col_index,
                            'endColumnIndex': col_index + 3
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                })

                # Center align post_id header
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': col_index,
                            'endColumnIndex': col_index + 3
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat.horizontalAlignment'
                    }
                })

            if requests:
                sheet.batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"Inserted {len(new_headers)} new post_id headers and post_links.")
                return True
            else:
                print("No new post_id headers or post_links to insert. Next updates")
                return True

        except HttpError as err:
            print(f"Google Sheets API error: {err}")
            return False
        except Exception as e:
            print(f"Failed to update header: {str(e)}")
            return False

    def extract_facebook_post_id(self,url: str) -> Optional[str]:
        """
        Extracts the post ID from a Facebook post URL.
        
        Supports formats like:
        - https://www.facebook.com/{page_id}/posts/{page_id}_{post_id}
        - https://www.facebook.com/{page_id}/posts/{post_id}
        - https://www.facebook.com/story.php?story_fbid={post_id}&id={page_id}
        
        Returns:
            post_id (str) if found, else None
        """
        # Pattern: /posts/{page_id}_{post_id}
        match = re.search(r'/posts/\d+_(\d+)', url)
        if match:
            return match.group(1)

        # Pattern: /posts/{post_id}
        match = re.search(r'/posts/(\d+)', url)
        if match:
            return match.group(1)

        # Pattern: story.php?story_fbid={post_id}
        match = re.search(r'story_fbid=(\d+)', url)
        if match:
            return match.group(1)

        return None
    
    def trim_sheet_rows(self, spreadsheet_id: str, tab_name: str, buffer: int = 10):
        try:
            service = self._initialize_google_sheets_service()
            sheet = service.spreadsheets()

            metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
            sheet_info = next(
                s for s in metadata['sheets'] if s['properties']['title'] == tab_name
            )
            sheet_id = sheet_info['properties']['sheetId']

            # Check how many rows are used based on column A
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A:A",
                majorDimension="COLUMNS"
            ).execute()
            used_rows = len(result.get('values', [[]])[0]) or 1
            new_row_count = used_rows + buffer

            total_cells = new_row_count * 20  # assuming 10 columns
            print(f"📊 Estimated cell usage after trim: {total_cells} cells")

            request = {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        'gridProperties': {
                            'rowCount': new_row_count,
                            'columnCount': 20  # fixed
                        }
                    },
                    'fields': 'gridProperties(rowCount,columnCount)'
                }
            }

            sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': [request]}).execute()
            print(f"🧹 Trimmed sheet '{tab_name}' to {new_row_count} rows")
            return True

        except Exception as e:
            print(f"🔴 Failed to trim: {str(e)}")
            return False

    
    def calculate_day_deltas(self, post_age, insights, existing_row):
        """
        Calculates reach, impressions, and reactions deltas for 3, 7, and 30 days.
        """
        print(f"Calculating deltas for post age: {post_age} days")

        def safe_int(value, fallback=0):
            try:
                return int(value)
            except:
                return fallback

        reach_3 = safe_int(existing_row[3]) if len(existing_row) > 3 else 0
        reach_7 = safe_int(existing_row[4]) if len(existing_row) > 4 else 0
        imp_3 = safe_int(existing_row[7]) if len(existing_row) > 7 else 0
        imp_7 = safe_int(existing_row[8]) if len(existing_row) > 8 else 0
        react_3 = safe_int(existing_row[11]) if len(existing_row) > 11 else 0
        react_7 = safe_int(existing_row[12]) if len(existing_row) > 12 else 0

        reach_now = insights.get('reach', 0)
        imp_now = insights.get('impressions', 0)
        react_now = insights.get('reactions', 0)

        result = {
            "reach_3": '',
            "reach_7": '',
            "reach_30": '',
            "imp_3": '',
            "imp_7": '',
            "imp_30": '',
            "react_3": '',
            "react_7": '',
            "react_30": ''
        }

        # 3-day snapshot (if old enough)
        if post_age >= 3:
            result["reach_3"] = str(reach_3) if reach_3 > 0 else str(reach_now)
            result["imp_3"] = str(imp_3) if imp_3 > 0 else str(imp_now)
            result["react_3"] = str(react_3) if react_3 > 0 else str(react_now)

        # 7-day delta (keep 3-day too)
        if post_age >= 7:
            result["reach_3"] = str(reach_3) if reach_3 > 0 else str(reach_now)
            result["imp_3"] = str(imp_3) if imp_3 > 0 else str(imp_now)
            result["react_3"] = str(react_3) if react_3 > 0 else str(react_now)

            result["reach_7"] = str(max(reach_now - reach_3, 0)) if reach_3 else ''
            result["imp_7"] = str(max(imp_now - imp_3, 0)) if imp_3 else ''
            result["react_7"] = str(max(react_now - react_3, 0)) if react_3 else ''

        # 30-day delta (keep 3-day and 7-day too)
        if post_age >= 30:
            result["reach_3"] = str(reach_3) if reach_3 > 0 else str(reach_now)
            result["reach_7"] = str(reach_7) if reach_7 > 0 else str(max(reach_now - reach_3, 0)) if reach_3 else ''
            result["reach_30"] = str(max(reach_now - (reach_3 + reach_7), 0)) if (reach_3 or reach_7) else ''

            result["imp_3"] = str(imp_3) if imp_3 > 0 else str(imp_now)
            result["imp_7"] = str(imp_7) if imp_7 > 0 else str(max(imp_now - imp_3, 0)) if imp_3 else ''
            result["imp_30"] = str(max(imp_now - (imp_3 + imp_7), 0)) if (imp_3 or imp_7) else ''

            result["react_3"] = str(react_3) if react_3 > 0 else str(react_now)
            result["react_7"] = str(react_7) if react_7 > 0 else str(max(react_now - react_3, 0)) if react_3 else ''
            result["react_30"] = str(max(react_now - (react_3 + react_7), 0)) if (react_3 or react_7) else ''


        return result
    
    
    def safe_execute(self, request, retries=3, delay=5):
        for attempt in range(retries):
            try:
                return request.execute()
            except (ConnectionResetError, HttpError) as e:
                wait = delay * (2 ** attempt)  # exponential backoff
                print(f"[Retry {attempt + 1}] Error: {e}. Waiting {wait}s before retry...")
                time.sleep(wait)
        raise Exception("Google Sheets API call failed after multiple retries")

    
    def transfer_insight_data(self, spreadsheet_id: str, tab_name: str, insights_data: list, followers: dict, date: str = None):
        try:
            print(f"\n📅 DAILY INSIGHTS DUMP FOR {date or 'today'}")
            service = self._initialize_google_sheets_service()
            sheet = service.spreadsheets()

            # 1. Set up dates (YYYY-MM-DD)
            today = datetime.now(timezone.utc).date()
            processing_date = date or today.strftime('%Y-%m-%d')
            yesterday = (today - timedelta(days=1))
            compare_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')  # ⬅️ string now
            if not insights_data:
                print("⚠️ No posts processed today")
                return False

            # 2. Check if processing_date already exists in Column H (row 2)
            existing_data = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!Q3",
                majorDimension="ROWS"
            ).execute().get('values', [[]])

            if existing_data and existing_data[0] and existing_data[0][0] == yesterday:
                print(f"⏭️ Data for {yesterday} already exists - skipping")
                return True

            # i need to fetched all the data on last update 
            # then performed the checking based on post_id located on each row column G3 and below if matched i need to get that row print it for now
            # on G3 its a post url thats why i have this function extract_facebook_post_id its returning post_id
            # 2.5 Fetch all existing post URLs (Column G from G3 downward) and rows
            existing_rows = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A3:R",
                majorDimension="ROWS"
            ).execute().get('values', [])

            # 3. Prepare today's data
            new_rows = []
            for post in insights_data:
                insights = post.get('insights', {})
                date_str = post.get('created_time', yesterday) # fallback to yesterday if missing
                created_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
                only_date = created_date.date().isoformat()  # 'YY-MM-DD'

                # Convert only_date back to date object
                posted_date = datetime.strptime(only_date, "%Y-%m-%d").date()
                # Calculate post age
                post_age = (yesterday - posted_date).days

                # Convert insights post links into a set of post_ids
                incoming_post_id = self.extract_facebook_post_id(post.get("post_link", ""))
                # Match rows where:
                deltas = {
                    "reach_3": '',
                    "reach_7": '',
                    "reach_30": '',
                    "imp_3": '',
                    "imp_7": '',
                    "imp_30": '',
                    "react_3": '',
                    "react_7": '',
                    "react_30": ''
                }
                for row_index, row in enumerate(existing_rows):
                    # Ensure row has at least 15 columns (to access P and Q)
                    if len(row) >= 15:
                        post_url = row[15]
                        last_update_date = row[16]
                        
                        existing_post_id = self.extract_facebook_post_id(post_url)
                        if existing_post_id == incoming_post_id and last_update_date == compare_date:
                            print(f"🔁 Matched Row {row_index + 3} (Updated on {last_update_date}): {row}")
                            deltas = self.calculate_day_deltas(post_age, insights, row)
                            # break
                        # else:
                            # print(f"❌ No match for Row {row_index + 3}") 
                    # break

                # print(deltas)
                if post_age == 3 and deltas['reach_3'] == '':
                    deltas['reach_3'] = str(insights.get('reach', ''))
                    deltas['imp_3'] = str(insights.get('impressions', ''))
                    deltas['react_3'] = str(insights.get('reactions', ''))

                if post_age == 7 and deltas['reach_7'] == '':
                    deltas['reach_7'] = str(insights.get('reach', ''))
                    deltas['imp_7'] = str(insights.get('impressions', ''))
                    deltas['react_7'] = str(insights.get('reactions', ''))
                            
                new_rows.append([
                    str(followers['followers_count']),
                    only_date,
                    post.get('message', '')[:500],
                    deltas["reach_3"],
                    deltas["reach_7"],
                    deltas["reach_30"],
                    str(insights.get('reach', 0)),
                    deltas["imp_3"],
                    deltas["imp_7"],
                    deltas["imp_30"],
                    str(insights.get('impressions', 0)),
                    deltas["react_3"],
                    deltas["react_7"],
                    deltas["react_30"],
                    str(insights.get('reactions', 0)),
                    post.get('post_link', ''),
                    yesterday.strftime('%Y-%m-%d'),
                    post_age
                ])

            # Normalize row lengths (pad to equal length)
            num_columns = max(len(row) for row in new_rows)
            for row in new_rows:
                while len(row) < num_columns:
                    row.append("")

            # 4. Get sheet ID
            sheet_metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
            sheet_id = next(s['properties']['sheetId'] 
                    for s in sheet_metadata['sheets'] 
                    if s['properties']['title'] == tab_name)

            # 🔧 Expand the sheet's column count if needed
            sheet_properties = next(s for s in sheet_metadata['sheets'] if s['properties']['title'] == tab_name)
            current_columns = sheet_properties['properties']['gridProperties'].get('columnCount', 0)

            if num_columns > current_columns:
                print(f"📐 Expanding columns from {current_columns} → {num_columns}")
                resize_request = {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "columnCount": num_columns
                            }
                        },
                        "fields": "gridProperties.columnCount"
                    }
                }
            else:
                resize_request = None

            # 5. Create centered cell format
            centered_format = {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }
            # Trim the sheet to avoid breaching 10M cell limit
            self.trim_sheet_rows(spreadsheet_id, tab_name)
            # 6. Batch requests (insert rows + format all cells)
            requests = []

            # ⬅️ Make sure to insert column resize request FIRST if needed
            if resize_request:
                requests.append(resize_request)

            requests.extend([
                # Insert blank rows
                {
                    'insertDimension': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'ROWS',
                            'startIndex': 2,# Row 3 (0-based index 2)
                            'endIndex': 2 + len(new_rows)
                        },
                        'inheritFromBefore': False
                    }
                },
                # Apply center alignment to headers (row 1)
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'cell': {
                            'userEnteredFormat': centered_format
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                    }
                },
                # Apply center alignment to new data
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 2 + len(new_rows),
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'cell': {
                            'userEnteredFormat': centered_format
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                    }
                },
                # Insert data values
                {
                    'updateCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 2 + len(new_rows),
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'rows': [{'values': [
                            {'userEnteredValue': {'stringValue': str(value)}}
                            for value in row
                        ]} for row in new_rows],
                        'fields': 'userEnteredValue'
                    }
                }
            ])

            # 7. Execute batch update
            request = sheet.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            )
            self.safe_execute(request)

            print(f"✅ Added {len(new_rows)} centered records for {yesterday}")
            return True

        except HttpError as e:
            print(f"🔴 Sheets API Error: {str(e)}")
            if e.resp.status == 429:
                print("⏳ Rate limited - waiting 60 seconds...")
                time.sleep(60)
                return self.transfer_insight_data(spreadsheet_id, tab_name, insights_data, followers, date)
            return False
        except Exception as e:
            print(f"🔴 Critical Failure: {str(e)}")
            return False

    def hide_old_rows(self, spreadsheet_id: str, tab_name: str):
        service = self._initialize_google_sheets_service()
        sheet = service.spreadsheets()
        
        # Get all dates in Column Q (index 16, so range Q3 down)
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!Q3:Q",
            majorDimension="COLUMNS"
        ).execute()

        values = result.get("values", [[]])[0]
        old_yesterday = (datetime.now().date() - timedelta(days=2))

        rows_to_hide = []
        for i, val in enumerate(values):
            try:
                row_date = datetime.strptime(val.strip(), "%Y-%m-%d").date()
                if row_date <= old_yesterday:
                    rows_to_hide.append(i + 2)  # index starts at 0, row 3 = index 0 + 2
            except Exception:
                continue

        if not rows_to_hide:
            print("ℹ️ No rows to hide.")
            return

        # Get the sheet ID
        sheet_metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = next(
            s["properties"]["sheetId"]
            for s in sheet_metadata["sheets"]
            if s["properties"]["title"] == tab_name
        )

        requests = [{
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row,
                    "endIndex": row + 1,
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        } for row in rows_to_hide]

        body = {"requests": requests}
        sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

        print(f"✅ Hidden {len(rows_to_hide)} rows with date <= yesterday.")

    
    def transfer_insight_data_old(self, spreadsheet_id: str, tab_name: str, insights_data: list, followers, date: str = None):
        try:
            print(f"\n=== Transfer to {tab_name} for date: {date or 'today'}, Followers: {followers} ===")
            service = self._initialize_google_sheets_service()
            sheet = service.spreadsheets()

            # Default to today if no date provided
            today = datetime.now(timezone.utc).date()
            date = date or today.strftime('%d/%m/%Y')

            if not insights_data:
                print("Warning: No insights data provided")
                return False

            # Fetch headers (rows 1–3)
            header_result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!1:3",
                majorDimension='ROWS'
            ).execute()
            header_rows = header_result.get('values', [])
            if len(header_rows) < 3:
                raise ValueError("Sheet must have at least 3 header rows.")

            # Build post_id to column mapping (from row 1)
            post_id_columns = {}
            for col_idx, post_id in enumerate(header_rows[0][1:], start=1):  # skip column A (date)
                if post_id:
                    post_id_columns[str(post_id).strip()] = col_idx

            # Preallocate data row with enough columns
            if not post_id_columns:
                print("Warning: No post_id headers found in row 1.")
                return False

            max_required_col = max(col + 2 for col in post_id_columns.values())  # +2 for reach, impressions, reactions
            data_row = [''] * (max_required_col + 1)
            data_row[0] = date  # Insert date in column A

            # Fill in insights in appropriate columns
            for insight in insights_data:
                post_id = str(insight.get('post_id', '')).strip()
                if not post_id or post_id not in post_id_columns:
                    print(f"Skipping unknown post_id: {post_id}")
                    continue

                base_col = post_id_columns[post_id]
                insights = insight.get('insights', {})

                data_row[base_col] = str(insights.get('reach', ''))
                data_row[base_col + 1] = str(insights.get('impressions', ''))
                data_row[base_col + 2] = str(insights.get('reactions', ''))

            # Normalize date for matching
            def normalize_date(s):
                for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y'):
                    try:
                        return datetime.strptime(s, fmt).date()
                    except Exception:
                        continue
                return None

            existing_dates = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A4:A",
                majorDimension='ROWS'
            ).execute().get('values', [])

            target_date = normalize_date(date)
            row_4_date = normalize_date(existing_dates[0][0]) if existing_dates else None

            if row_4_date == target_date:
                # UPDATE row 4
                print("Updating existing row 4")
                update_result = sheet.values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{tab_name}!A4",
                    valueInputOption='USER_ENTERED',
                    body={"values": [data_row]}
                ).execute()
                print(f"Updated cells: {update_result.get('updatedCells', 0)}")
                print(data_row)
            else:
                # INSERT at row 4
                spreadsheet = sheet.get(spreadsheetId=spreadsheet_id).execute()
                sheet_id = next(s['properties']['sheetId'] for s in spreadsheet['sheets']
                                if s['properties']['title'] == tab_name)

                print("Inserting new row at position 4")
                cell_data = [{'userEnteredValue': {'stringValue': val}} if val else {} for val in data_row]
                requests = [
                    {
                        'insertDimension': {
                            'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 3, 'endIndex': 4},
                            'inheritFromBefore': False
                        }
                    },
                    {
                        'updateCells': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 3,
                                'endRowIndex': 4,
                                'startColumnIndex': 0,
                                'endColumnIndex': len(data_row)
                            },
                            'rows': [{'values': cell_data}],
                            'fields': 'userEnteredValue'
                        }
                    }
                ]

                batch_result = sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()
                print(f"Inserted new row: {len(batch_result.get('replies', []))} operations completed")

            print("✅ Insight transfer completed")
            return True

        except HttpError as err:
            content = err.content.decode()
            print(f"Google Sheets API error: {content}")
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise





