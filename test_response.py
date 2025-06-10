import requests

# Replace with your actual values
media_id = "18070415743947701"  # Instagram media/post ID
access_token = "EAAQ0qeOgEVYBO91RoqHk21rK3FSl2iucjSsaRHc7qmryjPy6vyNyY9zYV19uP6CHyprpRciqDNZBFRtCLMeK2JRCpHVjXZAM3OOqOxZAjYzClqn9IomQXae5tzA1Gzm7wo6WxSnPkOA2tQv25jiXIxsQduaqBbdNhVB5mtr5Fxi1gFnXIzAp4q5"

url = f"https://graph.facebook.com/v19.0/{media_id}/insights"
params = {
    "metric": "reach,total_interactions,views,profile_visits,profile_activity",
    "access_token": access_token
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json().get("data", [])
    print("Original Insights data:")
    print(data)

    # Build a map of available metrics
    metric_map = {entry["name"]: entry["values"][0]["value"] for entry in data}

    # Extract individual metrics
    reach = metric_map.get("reach", 0)
    interactions = metric_map.get("total_interactions", 0)
    views = metric_map.get("views", 0)
    profile_visits = metric_map.get("profile_visits", 0)
    profile_activity = metric_map.get("profile_activity", 0)

    # Estimate impressions
    estimated_impressions = reach
    if interactions:
        estimated_impressions += interactions * 1.5
    if views:
        estimated_impressions += views
    if profile_visits:
        estimated_impressions += profile_visits * 0.5
    if profile_activity:
        estimated_impressions += profile_activity * 0.5

    # Insert estimated impressions into the original data list
    data.append({
        "name": "estimated_impressions",
        "period": "lifetime",
        "values": [{"value": int(estimated_impressions)}],
        "title": "Estimated Impressions",
        "description": "A calculated estimate based on reach, views, interactions, and profile activity."
    })

    print("\nAugmented Insights data (with estimated impressions):")
    print(data)

else:
    print(f"Error {response.status_code}: {response.text}")
