from celery import Celery
from celery.schedules import crontab

app = Celery(
    "tasks",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0",
    include=([
            "workers.hubspot_to_bigquery",
        ]
    ),
)

app.conf.beat_schedule = {
    "hubspot_export_deals_to_bigquery": {
        "task": "hubspot_export_deals_to_bigquery",
        "schedule": crontab(
            minute="*",
        ),
    },
}
