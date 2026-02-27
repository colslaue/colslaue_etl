from app import app
from connectors.bigquery import BigQueryClient
from connectors.hubspot_api import HubspotConn
from config import CONFIG
from google.cloud.bigquery import SchemaField

@app.task(name="hubspot_export_deals_to_bigquery")
def hubspot_export_deals_to_bigquery():
    hubspot_conn = HubspotConn.get_instance()
    bigquery_conn = BigQueryClient.get_instance()

    properties = [
        'amount',
        'dealname',
        'hs_object_id'
    ]

    dtypes = {
        "amount": "string",
        "dealname": "string",
        "hs_object_id": "string"
    }

    schema = [
        SchemaField('amount', 'NUMERIC'),
        SchemaField('dealname', 'STRING'),
        SchemaField('hs_object_id', 'INT64'),
    ]

    destination_table = f"{CONFIG.BIGQUERY_PROJECT}.{CONFIG.BIGQUERY_HUBSPOT_DATASET}.deal"

    last_df = None

    for df in hubspot_conn.get_deals(
        properties=properties,
    ):
        df = df.astype(dtypes)
        write_disposition = "WRITE_TRUNCATE" if last_df is None else "WRITE_APPEND"

        last_df = bigquery_conn.upload_df(
            destination_table=destination_table,
            df=df,
            schema=schema,
            write_disposition=write_disposition,
        )