import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from workers.hubspot_to_bigquery import hubspot_export_deals_to_bigquery


@patch("workers.hubspot_to_bigquery.CONFIG")
@patch("workers.hubspot_to_bigquery.BigQueryClient")
@patch("workers.hubspot_to_bigquery.HubspotConn")
def test_hubspot_export_deals_to_bigquery_single_page(
    mock_hubspot_conn_cls,
    mock_bigquery_client_cls,
    mock_config,
):
    """Single page of deals: get_deals yields one DataFrame, upload_df called once with WRITE_TRUNCATE."""
    mock_config.BIGQUERY_PROJECT = "test-project"
    mock_config.BIGQUERY_HUBSPOT_DATASET = "test_dataset"

    mock_hubspot = MagicMock()
    single_df = pd.DataFrame(
        {
            "amount": ["100"],
            "dealname": ["Deal One"],
            "hs_object_id": ["12345"],
        }
    )
    mock_hubspot.get_deals.return_value = iter([single_df])
    mock_hubspot_conn_cls.get_instance.return_value = mock_hubspot

    mock_bq = MagicMock()
    mock_bigquery_client_cls.get_instance.return_value = mock_bq

    hubspot_export_deals_to_bigquery()

    mock_hubspot.get_deals.assert_called_once_with(properties=["amount", "dealname", "hs_object_id"])
    assert mock_bq.upload_df.call_count == 1
    call_kw = mock_bq.upload_df.call_args[1]
    assert call_kw["destination_table"] == "test-project.test_dataset.deal"
    assert call_kw["write_disposition"] == "WRITE_TRUNCATE"
    assert len(call_kw["schema"]) == 3


@patch("workers.hubspot_to_bigquery.CONFIG")
@patch("workers.hubspot_to_bigquery.BigQueryClient")
@patch("workers.hubspot_to_bigquery.HubspotConn")
def test_hubspot_export_deals_to_bigquery_multiple_pages(
    mock_hubspot_conn_cls,
    mock_bigquery_client_cls,
    mock_config,
):
    """Multiple pages: first upload WRITE_TRUNCATE, second WRITE_APPEND."""
    mock_config.BIGQUERY_PROJECT = "test-project"
    mock_config.BIGQUERY_HUBSPOT_DATASET = "test_dataset"

    mock_hubspot = MagicMock()
    page1 = pd.DataFrame(
        {"amount": ["100"], "dealname": ["Deal One"], "hs_object_id": ["111"]}
    )
    page2 = pd.DataFrame(
        {"amount": ["200"], "dealname": ["Deal Two"], "hs_object_id": ["222"]}
    )
    mock_hubspot.get_deals.return_value = iter([page1, page2])
    mock_hubspot_conn_cls.get_instance.return_value = mock_hubspot

    mock_bq = MagicMock()
    mock_bigquery_client_cls.get_instance.return_value = mock_bq

    hubspot_export_deals_to_bigquery()

    assert mock_bq.upload_df.call_count == 2
    first_call = mock_bq.upload_df.call_args_list[0][1]
    second_call = mock_bq.upload_df.call_args_list[1][1]
    assert first_call["write_disposition"] == "WRITE_TRUNCATE"
    assert second_call["write_disposition"] == "WRITE_APPEND"
    assert first_call["destination_table"] == second_call["destination_table"] == "test-project.test_dataset.deal"
