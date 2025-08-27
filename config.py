from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    GOOGLE_APPLICATION_CREDENTIALS=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    HUBSPOT_API_KEY=os.environ.get("HUBSPOT_API_KEY")

CONFIG = Config()
