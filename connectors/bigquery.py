from google.cloud import bigquery

class BigQueryClient:
    _singleton = None
    client = None

    def __init__(self):
        self.client = bigquery.Client()

    @classmethod
    def get_instance(cls):
        if cls._singleton is None:
            cls._singleton = BigQueryClient()
        return cls._singleton.client
