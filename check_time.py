from datetime import datetime, timedelta, timezone

# Use UTC instead of local time
query_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')


current_year = datetime.now().year
today_date = datetime.now().strftime('%Y-%m-%d')
start_date = f"{current_year}-01-01"  # Already in UTC (no timezone ambiguity)

today = datetime.now(timezone.utc).date()
# yesterday = today - timedelta(days=1)
today_str = today.strftime('%d/%m/%Y') #Yesterday date

print('timezone')
print(today_str)

print('for meta:',start_date, today_date, query_date)