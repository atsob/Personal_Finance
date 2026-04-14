import warnings
import urllib3

def configure_warnings_and_ssl():
    """Configure warnings and SSL settings."""
    warnings.filterwarnings('ignore', category=UserWarning)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)