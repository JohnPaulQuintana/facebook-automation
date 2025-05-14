class FacebookHelper:
    def __init__(self, raw_accounts, raw_pages):
        self.raw_accounts = raw_accounts
        self.raw_pages = raw_pages

    # def get_restructured_info(self):
        
        # return f"https://www.facebook.com/v12.0/dialog/oauth?client_id={self.app_id}&redirect_uri={self.redirect_uri}&scope=email,public_profile"

    # def get_access_token(self, code):
    #     token_url = f"https://graph.facebook.com/v12.0/oauth/access_token?client_id={self.app_id}&redirect_uri={self.redirect_uri}&client_secret={self.app_secret}&code={code}"
    #     # Here you would make a request to the token_url and return the access token
    #     # For example using requests library:
    #     # response = requests.get(token_url)
    #     # return response.json().get('access_token')
    #     return "mock_access_token"  # Replace with actual access token retrieval logic