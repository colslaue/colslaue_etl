from dotenv import load_dotenv
from google.cloud import secretmanager
import os

load_dotenv()

def get_secret(secret_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.environ.get("BIGQUERY_PROJECT")
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error accessing secret '{secret_id}': {e}")
        return None


class Config:
    GOOGLE_APPLICATION_CREDENTIALS=get_secret("google_application_credentials")
    HUBSPOT_API_KEY=get_secret("hubspot_api_key")
    BIGQUERY_PROJECT=os.environ.get("BIGQUERY_PROJECT")
    BIGQUERY_HUBSPOT_DATASET=os.environ.get("BIGQUERY_HUBSPOT_DATASET")

CONFIG = Config()
