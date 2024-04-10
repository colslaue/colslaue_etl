import os
from dotenv import load_dotenv

load_dotenv()


class Config():
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
    DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")
    HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY")


CONFIG = Config()
