from celery import Celery
from celery.schedules import crontab

app = Celery(
    "tasks",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0",
    include=["workers.tasks"],
)

app.conf.beat_schedule = {
    "hubspot_deals_to_bigquery": {
        "task": "hubspot_deals_to_bigquery",
        "schedule": crontab(
            minute="*",
        ),
    },
    "hubspot_companies_to_bigquery": {
        "task": "hubspot_companies_to_bigquery",
        "schedule": crontab(
            minute="*",
        ),
    },
}
