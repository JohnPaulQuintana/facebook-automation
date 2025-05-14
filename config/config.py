import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
# Load environment variables from the .env file located in the app/env directory
# load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'env', '.env'))
# Print the current directory and the absolute path to the .env file
# print("Current directory:", os.getcwd())
# env_file_path = os.path.join(os.getcwd(), 'app', '.env')
# print("Looking for .env file at:", env_file_path)

class Config:
    """Configuration class to manage environment variables."""
    # print(os.getenv("TYPE"))
    TYPE = os.getenv("TYPE")
    PROJECT_ID = os.getenv("PROJECT_ID")
    PRIVATE_KEY_ID = os.getenv("PRIVATE_KEY_ID")
    PRIVATE_KEY = os.getenv("PRIVATE_KEY").replace("\\n", "\n")  # Handle newline characters
    CLIENT_EMAIL = os.getenv("CLIENT_EMAIL")
    CLIENT_ID = os.getenv("CLIENT_ID")
    AUTH_URI = os.getenv("AUTH_URI")
    TOKEN_URI = os.getenv("TOKEN_URI")
    AUTH_PROVIDER_X509_CERT_URL = os.getenv("AUTH_PROVIDER_X509_CERT_URL")
    CLIENT_X509_CERT_URL = os.getenv("CLIENT_X509_CERT_URL")
    UNIVERSE_DOMAIN = os.getenv("UNIVERSE_DOMAIN")

    # Add validation for missing PRIVATE_KEY
    if PRIVATE_KEY:
        PRIVATE_KEY = PRIVATE_KEY.replace("\\n", "\n")  # Handle newline characters
    else:
        raise ValueError("PRIVATE_KEY environment variable is not set")
    
    @classmethod
    def as_dict(cls):
        """Optional: Return all configuration as a dictionary."""
        return {
            "type": cls.TYPE,
            "project_id": cls.PROJECT_ID,
            "private_key_id": cls.PRIVATE_KEY_ID,
            "private_key": cls.PRIVATE_KEY,
            "client_email": cls.CLIENT_EMAIL,
            "client_id": cls.CLIENT_ID,
            "auth_uri": cls.AUTH_URI,
            "token_uri": cls.TOKEN_URI,
            "auth_provider_x509_cert_url": cls.AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": cls.CLIENT_X509_CERT_URL,
            "universe_domain": cls.UNIVERSE_DOMAIN,
        }