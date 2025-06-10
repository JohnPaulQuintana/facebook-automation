from datetime import datetime, timedelta, timezone
from datetime import date as dt
# # Use UTC instead of local time
# query_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')


# current_year = datetime.now().year
# today_date = datetime.now().strftime('%Y-%m-%d')
# start_date = f"{current_year}-01-01"  # Already in UTC (no timezone ambiguity)

# today = datetime.now(timezone.utc).date()
# # yesterday = today - timedelta(days=1)
# today_str = today.strftime('%d/%m/%Y') #Yesterday date

# print('timezone')
# print(today_str)

# print('for meta:',start_date, today_date, query_date)

# Get today’s date
today = datetime.now()
today_date = today.strftime('%Y-%m-%d')

today2 = datetime.now(timezone.utc).date()
yesterday = today2 - timedelta(days=1)
today_str = yesterday.strftime('%Y-%m-%d') #Yesterday date

# Set the start date to 30 days before today
start_date = (today - timedelta(days=29)).strftime('%Y-%m-%d')  # 29 to include today as the 30th day

print(f"Start Date: {start_date}")
print(f"End Date: {today_date}")
print(f"End Date: {today_str}")


# Step 2: Get daily insights for current month
todaym = dt.today()
since = todaym.replace(day=1).isoformat()  # '2025-06-01'
until = todaym.isoformat()

print(since,until, dt.today().date())