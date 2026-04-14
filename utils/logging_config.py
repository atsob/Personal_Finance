import logging
import sys

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        filename='app.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))
    return logging.getLogger(__name__)