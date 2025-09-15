from workers.tasks import (
    CONFIG,
    hubspot_deals_to_bigquery,
)
import unittest
from unittest.mock import Mock, patch


class TestHubspotDealsToBigQuery(unittest.TestCase):
    @patch('workers.tasks.HubspotConn')
    @patch('workers.tasks.BigQueryClient')
    def test_export_logic_with_mocks(
        self, mock_BigQueryClient, mock_HubspotConn
    ):

        mock_row = Mock()
        mock_row.id = 12345
        mock_row.amount = 123.45
        mock_row.deal_name = "deal name"
        mock_row.deal_stage = "deal stage"
        mock_row.pipeline = "pipeline"

        mock_results = [mock_row]

        mock_session = mock_HubspotConn.get_instance.return_value.__enter__.return_value
        mock_session.crm.deals.basic_api.get_page.return_value = mock_results
        mock_bq_client = mock_BigQueryClient.get_instance.return_value.__enter__.return_value

        hubspot_deals_to_bigquery()

        mock_session.crm.deals.basic_api.get_page.assert_called_once()

        expected_output = [
            {
                "id": 12345,
                "amount": 123.45,
                "deal_name": "deal name",
                "deal_stage": "deal stage",
                "pipeline": "pipeline",
            }
        ]

        expected_table_id = (
            f"{CONFIG.BIGQUERY_PROJECT}.{CONFIG.BIGQUERY_HUBSPOT_DATASET}.deal"
        )
        mock_bq_client.load_table_from_json.assert_called_once_with(expected_output, expected_table_id)