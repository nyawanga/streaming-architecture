"""module for superset configs"""

import os
from dotenv import load_dotenv

load_dotenv()

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

ENABLE_PROXY_FIX = True
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")
