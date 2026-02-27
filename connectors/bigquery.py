from google.cloud import bigquery
import pandas as pd


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

    def upload_df(
        self,
        destination_table: str,
        df: pd.DataFrame,
        schema=None,
        write_disposition="WRITE_TRUNCATE",
    ):
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition=write_disposition
        )

        load_job = self.client.load_table_from_dataframe(
            dataframe=df, destination=destination_table, job_config=job_config
        )

        return load_job
