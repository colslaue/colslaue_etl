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
        return cls._singleton


def upload_json_to_table(data, table):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    client = BigQueryClient.get_instance()
    client.client.load_table_from_json(
        json_rows=data, destination=table, job_config=job_config
    )
