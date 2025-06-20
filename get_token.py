import pickle
from google_auth_oauthlib.flow import InstalledAppFlow

# Scopes required for YouTube Data + Analytics
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

def save_token_for_account(client_secret_path: str, token_output_path: str):
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(port=8080)
    with open(token_output_path, 'wb') as f:
        pickle.dump(creds, f)
    print(f"✅ Token saved to: {token_output_path}")

# Example: Run for all YouTube account
# client is baji npr email:
save_token_for_account("client/shared_secret.json", "tokens/token_baji_npr.pkl")
