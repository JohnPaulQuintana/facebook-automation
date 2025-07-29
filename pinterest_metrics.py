import requests

url = "https://pinterest-scraper5.p.rapidapi.com/api/users/info"
querystring = {"username": "baji_bgd"}

headers = {
    "x-rapidapi-key": "c5268228e2mshae211db22c994e6p119e97jsnd90c58e89ef2",
    "x-rapidapi-host": "pinterest-scraper5.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

# Parse the nested structure
user = data.get("data", {}).get("user", {})

if not user:
    print("❌ Failed to retrieve user data.")
else:
    username = user.get("username")
    full_name = user.get("full_name", "N/A")
    followers = user.get("follower_count", 0)
    following = user.get("following_count", 0)
    pins = user.get("pin_count", 0)
    boards = user.get("board_count", 0)
    profile_image = user.get("image_xlarge_url", '')
    website = user.get("website_url", "N/A")
    bio = user.get("about", "N/A")
    last_active = user.get("last_pin_save_time", "N/A")

    print(f"📌 Pinterest Profile Info for @{username}")
    print(f"👤 Name: {full_name}")
    print(f"🧷 Pins: {pins}")
    print(f"📋 Boards: {boards}")
    print(f"👥 Followers: {followers}")
    print(f"➡️ Following: {following}")
    print(f"🌐 Website: {website}")
    print(f"🖼️ Profile Picture: {profile_image}")
    print(f"🕒 Last Pin Activity: {last_active}")
    print(f"📝 Bio: {bio}")
