from celery import Celery
from celery.schedules import crontab
from config import CONFIG
from dbt.cli.main import dbtRunner

app = Celery("etl_scheduler", broker=CONFIG.CELERY_BROKER_URL)


@app.task
def hello_world():
    return "Hello, World"


@app.task
def dbt_run():
    dbt = dbtRunner()
    cli_args = ["run"]
    dbt.invoke(cli_args)


app.conf.beat_schedule = {
    "hello world": {
        "task": "etl.app.hello_world",
        "schedule": 15
    },
    "dbt_run": {
        "task": "etl.app.dbt_run",
        "schedule": 15
    }
}
