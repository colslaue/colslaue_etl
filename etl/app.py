import etl.connectors.bigquery as bigquery
import etl.connectors.hubspot as hubspot
from celery import Celery
from celery.schedules import crontab
from config import CONFIG
from dbt.cli.main import dbtRunner

app = Celery("etl_scheduler", broker=CONFIG.CELERY_BROKER_URL)


@app.task
def dbt_build():
    dbt = dbtRunner()
    cli_args = ["build", "--target", "prod"]
    dbt.invoke(cli_args)


@app.task
def upload_deals_to_bigquery():
    deal_data = hubspot.HubspotAPI.get_instance("/crm/v3/objects/deals?properties=amount,closedate,createdate,dealname,dealstage,pipeline,hubspot_owner_id").get_data()
    deal_data = hubspot.flatten_deals(deal_data)
    bigquery.upload_json_to_table(deal_data, "hubspot.deal")


@app.task
def upload_companies_to_bigquery():
    company_data = hubspot.HubspotAPI.get_instance("/crm/v3/objects/companies?properties=createdate,name,hubspot_owner_id,annualrevenue,country").get_data()
    company_data = hubspot.flatten_companies(company_data)
    bigquery.upload_json_to_table(company_data, "hubspot.company")


@app.task
def upload_deal_company_assoc_to_bigquery():
    deal_company_data = hubspot.HubspotAPI.get_instance("/crm/v4/objects/deal?&associations=company").get_data()
    deal_company_data = hubspot.flatten_deal_company(deal_company_data)
    bigquery.upload_json_to_table(deal_company_data, "hubspot.deal_company")


@app.task
def upload_users_to_bigquery():
    user_data = hubspot.HubspotAPI.get_instance("/crm/v3/owners/").get_data()
    user_data = hubspot.flatten_users(user_data)
    bigquery.upload_json_to_table(user_data, "hubspot.user")


@app.task
def upload_contacts_to_bigquery():
    contact_data = hubspot.HubspotAPI.get_instance("/crm/v3/objects/contacts?properties=firstname,lastname,hs_lifecyclestage_marketingqualifiedlead_date,hs_lifecyclestage_salesqualifiedlead_date,hs_lifecyclestage_customer_date,country,jobtitle").get_data()
    contact_data = hubspot.flatten_contacts(contact_data)
    bigquery.upload_json_to_table(contact_data, "hubspot.contact")


@app.task
def push_avg_deal_to_hubspot_company():
    query = """
        SELECT
            company_id
            ,average_deal_size
        FROM `colslaue.report.hubspot__company_overview`
    """
    avg_deal = bigquery.BigQueryClient.get_instance().client.query(query).result().to_dataframe().to_dict(orient="records")
    for row in avg_deal:
        company = row["company_id"]
        avg = row["average_deal_size"]
        properties = {
            "properties": {
                "average_deal_size": f"{avg}"
            }
        }
        data = hubspot.HubspotAPI.get_instance(f"/crm/v3/objects/companies/{company}")
        data.push_data(properties)


app.conf.beat_schedule = {
    "dbt_build": {
        "task": "etl.app.dbt_build",
        "schedule": crontab()
    },
    "deals_to_bigquery": {
        "task": "etl.app.upload_deals_to_bigquery",
        "schedule": crontab()
    },
    "companies_to_bigquery": {
        "task": "etl.app.upload_companies_to_bigquery",
        "schedule": crontab()
    },
    "deal_company_to_bigquery": {
        "task": "etl.app.upload_deal_company_assoc_to_bigquery",
        "schedule": crontab()
    },
    "users_to_bigquery": {
        "task": "etl.app.upload_users_to_bigquery",
        "schedule": crontab()
    },
    "contacts_to_bigquery": {
        "task": "etl.app.upload_contacts_to_bigquery",
        "schedule": crontab()
    },
    "avg_deal_to_hubspot": {
        "task": "etl.app.push_avg_deal_to_hubspot_company",
        "schedule": crontab()
    }
}
