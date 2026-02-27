import pandas as pd
from unittest.mock import MagicMock, patch

import pytest

from connectors.bigquery import BigQueryClient


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset BigQueryClient singleton so each test gets a fresh instance."""
    yield
    BigQueryClient._singleton = None


@patch("connectors.bigquery.bigquery.Client")
def test_get_instance_returns_singleton(mock_client_cls):
    """get_instance returns the same instance on multiple calls."""
    BigQueryClient._singleton = None
    a = BigQueryClient.get_instance()
    b = BigQueryClient.get_instance()
    assert a is b
    mock_client_cls.assert_called_once()


@patch("connectors.bigquery.bigquery.Client")
def test_upload_df_calls_load_table_from_dataframe_with_defaults(mock_client_cls):
    """upload_df uses WRITE_TRUNCATE by default and calls load_table_from_dataframe."""
    BigQueryClient._singleton = None
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    client = BigQueryClient.get_instance()
    df = pd.DataFrame({"col": [1, 2, 3]})
    client.upload_df(
        destination_table="project.dataset.table",
        df=df,
    )

    mock_client.load_table_from_dataframe.assert_called_once()
    call_kw = mock_client.load_table_from_dataframe.call_args[1]
    assert call_kw["destination"] == "project.dataset.table"
    assert call_kw["dataframe"].equals(df)
    job_config = call_kw["job_config"]
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.schema is None


@patch("connectors.bigquery.bigquery.Client")
def test_upload_df_passes_schema_and_write_disposition(mock_client_cls):
    """upload_df forwards schema and write_disposition to LoadJobConfig."""
    BigQueryClient._singleton = None
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    from google.cloud.bigquery import SchemaField

    schema = [
        SchemaField("name", "STRING"),
        SchemaField("count", "INT64"),
    ]
    client = BigQueryClient.get_instance()
    df = pd.DataFrame({"name": ["a"], "count": [1]})
    client.upload_df(
        destination_table="proj.dset.tbl",
        df=df,
        schema=schema,
        write_disposition="WRITE_APPEND",
    )

    call_kw = mock_client.load_table_from_dataframe.call_args[1]
    assert call_kw["destination"] == "proj.dset.tbl"
    job_config = call_kw["job_config"]
    assert job_config.schema == schema
    assert job_config.write_disposition == "WRITE_APPEND"


@patch("connectors.bigquery.bigquery.Client")
def test_upload_df_returns_load_job(mock_client_cls):
    """upload_df returns the result of load_table_from_dataframe."""
    BigQueryClient._singleton = None
    mock_job = MagicMock()
    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job
    mock_client_cls.return_value = mock_client

    client = BigQueryClient.get_instance()
    result = client.upload_df(
        destination_table="p.d.t",
        df=pd.DataFrame(),
    )

    assert result is mock_job
