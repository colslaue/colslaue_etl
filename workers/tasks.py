from app import app
from connectors.bigquery import BigQueryClient
from connectors.hubspot_api import HubspotConn
from config import CONFIG
from google.cloud import bigquery


# @app.task(name="hubspot_deals_to_bigquery")
# def hubspot_deals_to_bigquery():
#     hubspot = HubspotConn().get_instance()
#     data = hubspot.crm.deals.basic_api.get_page(limit=10, archived=False)
#     dict = data.to_dict().get("results")
#
#     data = []
#     for row in dict:
#         properties = row.get("properties")
#         data.append(
#             {
#                 "deal_id": row.get("id"),
#                 "amount": properties.get("amount"),
#                 # "close_date": properties.get('closedate'),
#                 # "create_date": properties.get('createdate'),
#                 "deal_name": properties.get("dealname"),
#                 "deal_stage": properties.get("dealstage"),
#                 "pipeline": properties.get("pipeline"),
#                 # "updated_at": row.get('updated_at')
#             }
#         )
#
#     bigquery_client = BigQueryClient().get_instance()
#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
#     )
#     bigquery_client.load_table_from_json(
#         json_rows=data,
#         destination=f"{CONFIG.BIGQUERY_PROJECT}.{CONFIG.BIGQUERY_HUBSPOT_DATASET}.deal",
#         job_config=job_config,
#     )


@app.task(name="hubspot_companies_to_bigquery")
def hubspot_companies_to_bigquery():
    hubspot = HubspotConn().get_instance()
    data = hubspot.crm.companies.basic_api.get_page(limit=10, archived=False)
    dict = data.to_dict().get("results")

    data = []
    for row in dict:
        properties = row.get("properties")
        data.append(
            {
                "company_id": row.get("id"),
                "name": properties.get("name"),
                "domain": properties.get("domain"),
            }
        )

    bigquery_client = BigQueryClient().get_instance()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    bigquery_client.load_table_from_json(
        json_rows=data,
        destination=f"{CONFIG.BIGQUERY_PROJECT}.{CONFIG.BIGQUERY_HUBSPOT_DATASET}.company",
        job_config=job_config,
    )

def test():
    print("hello, world")
