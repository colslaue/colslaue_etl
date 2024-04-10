import etl.connectors.bigquery as bigquery
import etl.connectors.hubspot as hubspot
from celery import Celery
from celery.schedules import crontab
from config import CONFIG
from dbt.cli.main import dbtRunner

app = Celery("etl_scheduler", broker=CONFIG.CELERY_BROKER_URL)


@app.task
def dbt_run():
    dbt = dbtRunner()
    cli_args = ["run"]
    dbt.invoke(cli_args)


@app.task
def upload_deals_to_bigquery():
    hubspot_data = hubspot.HubspotAPI.get_instance("/crm/v3/objects/deals").get_data()
    data = hubspot.flatten_deals(hubspot_data)
    bigquery.upload_json_to_table(data, "hubspot.deal")


app.conf.beat_schedule = {
    "dbt_run": {
        "task": "etl.app.dbt_run",
        "schedule": crontab(minute="0", hour="3")
    },
    "hubspot_to_bigquery":{
        "task": "etl.app.upload_deals_to_bigquery",
        "schedule": crontab(minute="*/15")
    }
}
