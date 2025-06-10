import requests
import datetime
import logging
from typing import Optional, Dict

class FacebookTokenValidator:
    def __init__(self, base_url: str, app_id: str, app_secret: str):
        """
        Initialize with Facebook app credentials.
        
        Args:
            app_id (str): Facebook App ID
            app_secret (str): Facebook App Secret
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = base_url
        self.logger = logging.getLogger('FacebookTokenValidator')

    def check_token_validity(self, access_token: str) -> Dict:
        """
        Check if a token is valid and get its metadata.
        
        Args:
            access_token (str): The token to validate
            
        Returns:
            Dict: Token metadata including:
                - is_valid (bool)
                - expires_at (datetime)
                - scopes (list)
                - type (str)
                - error (dict, if invalid)
        """
        debug_url = f"{self.base_url}/debug_token"
        params = {
            'input_token': access_token,
            'access_token': f"{self.app_id}|{self.app_secret}"
        }
        
        try:
            response = requests.get(debug_url, params=params).json()
            if 'error' in response:
                return {
                    'is_valid': False,
                    'error': response['error']
                }
            
            data = response['data']
            return {
                'is_valid': data['is_valid'],
                'expires_at': datetime.datetime.fromtimestamp(data['expires_at']),
                'scopes': data.get('scopes', []),
                'type': data.get('type', 'unknown'),
                'profile_id': data.get('profile_id'),
                'application': data.get('application'),
                'remaining_days': (datetime.datetime.fromtimestamp(data['expires_at']) - datetime.datetime.now()).days
            }
            
        except Exception as e:
            self.logger.error(f"Token validation failed: {str(e)}")
            return {
                'is_valid': False,
                'error': {'message': str(e)}
            }

    def refresh_long_lived_token(self, short_lived_token: str) -> Optional[str]:
        """
        Generate a new long-lived token (60 days) from a short-lived token.
        
        Args:
            short_lived_token (str): A valid short-lived token (1-2 hours)
            
        Returns:
            Optional[str]: New long-lived token if successful, None otherwise
        """
        exchange_url = f"{self.base_url}/oauth/access_token"
        params = {
            'grant_type': 'fb_exchange_token',
            'client_id': self.app_id,
            'client_secret': self.app_secret,
            'fb_exchange_token': short_lived_token
        }
        
        try:
            response = requests.get(exchange_url, params=params).json()
            if 'access_token' in response:
                return response['access_token']
            self.logger.error(f"Token refresh failed: {response}")
        except Exception as e:
            self.logger.error(f"Token refresh error: {str(e)}")
        
        return None

    def get_new_long_lived_token(self, user_id: str, user_access_token: str) -> Optional[str]:
        """
        Get a new long-lived token for a user (requires business use case approval).
        
        Args:
            user_id (str): Facebook User ID
            user_access_token (str): Current valid access token
            
        Returns:
            Optional[str]: New long-lived token if successful
        """
        extend_url = f"{self.base_url}/{user_id}/access_tokens"
        params = {
            'grant_type': 'fb_extend_sso_token',
            'client_id': self.app_id,
            'client_secret': self.app_secret,
            'access_token': user_access_token
        }
        
        try:
            response = requests.get(extend_url, params=params).json()
            if 'access_token' in response:
                return response['access_token']
            self.logger.error(f"Long-lived token generation failed: {response}")
        except Exception as e:
            self.logger.error(f"Token generation error: {str(e)}")
        
        return None