from workers.tasks import (
    # hubspot_deals_to_bigquery,
    hubspot_companies_to_bigquery,
)
import unittest
from unittest.mock import Mock, patch, ANY


# class TestHubspotDealsToBigQuery(unittest.TestCase):
#     @patch("workers.tasks.HubspotConn")
#     @patch("workers.tasks.BigQueryClient")
#     @patch("workers.tasks.CONFIG")
#     def test_export_logic_with_mocks(
#         self, mock_CONFIG, mock_BigQueryClient, mock_HubspotConn
#     ):
#
#         mock_CONFIG.BIGQUERY_PROJECT = "bigquery_project"
#         mock_CONFIG.BIGQUERY_HUBSPOT_DATASET = "bigquery_hubspot_dataset"
#         mock_CONFIG.HUBSPOT_API_KEY = "hubspot_api_key"
#         mock_CONFIG.GOOGLE_APPLICATION_CREDENTIALS = "google_application_credentials"
#         mock_session = mock_HubspotConn.return_value.get_instance.return_value
#         mock_session.crm.deals.basic_api.get_page.return_value = Mock(
#             to_dict=lambda: {
#                 "results": [
#                     {
#                         "id": 12345,
#                         "properties": {
#                             "amount": 123.45,
#                             "dealname": "deal name",
#                             "dealstage": "deal stage",
#                             "pipeline": "pipeline",
#                         },
#                     }
#                 ]
#             }
#         )
#         mock_bq_client = mock_BigQueryClient.return_value.get_instance.return_value
#
#         hubspot_deals_to_bigquery()
#
#         mock_session.crm.deals.basic_api.get_page.assert_called_once()
#
#         expected_output = [
#             {
#                 "deal_id": 12345,
#                 "amount": 123.45,
#                 "deal_name": "deal name",
#                 "deal_stage": "deal stage",
#                 "pipeline": "pipeline",
#             }
#         ]
#
#         expected_table_id = f"{mock_CONFIG.BIGQUERY_PROJECT}.{mock_CONFIG.BIGQUERY_HUBSPOT_DATASET}.deal"
#         mock_bq_client.load_table_from_json.assert_called_once_with(
#             json_rows=expected_output,
#             destination=expected_table_id,
#             job_config=ANY,
#         )


class TestHubspotCompaniesToBigQuery(unittest.TestCase):
    @patch("workers.tasks.HubspotConn")
    @patch("workers.tasks.BigQueryClient")
    @patch("workers.tasks.CONFIG")
    def test_export_logic_with_mocks(
        self, mock_CONFIG, mock_BigQueryClient, mock_HubspotConn
    ):

        mock_CONFIG.BIGQUERY_PROJECT = "bigquery_project"
        mock_CONFIG.BIGQUERY_HUBSPOT_DATASET = "bigquery_hubspot_dataset"
        mock_CONFIG.HUBSPOT_API_KEY = "hubspot_api_key"
        mock_CONFIG.GOOGLE_APPLICATION_CREDENTIALS = "google_application_credentials"
        mock_session = mock_HubspotConn.return_value.get_instance.return_value
        mock_session.crm.companies.basic_api.get_page.return_value = Mock(
            to_dict=lambda: {
                "results": [
                    {
                        "id": 12345,
                        "properties": {
                            "name": "name",
                            "domain": "domain",
                        },
                    }
                ]
            }
        )
        mock_bq_client = mock_BigQueryClient.return_value.get_instance.return_value

        hubspot_companies_to_bigquery()

        print(f"DEBUG: Calls made to mock_session: {mock_session.mock_calls}")

        mock_session.crm.companies.basic_api.get_page.assert_called_once()

        expected_output = [
            {
                "company_id": 12345,
                "name": "name",
                "domain": "domain",
            }
        ]

        expected_table_id = f"{mock_CONFIG.BIGQUERY_PROJECT}.{mock_CONFIG.BIGQUERY_HUBSPOT_DATASET}.company"
        mock_bq_client.load_table_from_json.assert_called_once_with(
            json_rows=expected_output,
            destination=expected_table_id,
            job_config=ANY,
        )
