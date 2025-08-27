from config import CONFIG
from google.cloud import bigquery

class BigQueryClient:
    _singleton = None
    client = None

    def __init__(self):
        self.client = bigquery.Client.from_service_account_json(CONFIG.GOOGLE_APPLICATION_CREDENTIALS)

    @classmethod
    def get_instance(cls):
        if cls._singleton is None:
            cls._singleton = BigQueryClient()
        return cls._singleton.client
