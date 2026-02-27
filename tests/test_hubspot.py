import pandas as pd
from unittest.mock import MagicMock, patch

import pytest

from connectors.hubspot_api import HubspotConn


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset HubspotConn singleton so each test gets a fresh instance."""
    yield
    HubspotConn._singleton = None


@patch("connectors.hubspot_api.CONFIG")
def test_get_instance_returns_singleton(mock_config):
    """get_instance returns the same instance on multiple calls."""
    mock_config.HUBSPOT_API_KEY = "test-key"
    HubspotConn._singleton = None
    a = HubspotConn.get_instance()
    b = HubspotConn.get_instance()
    assert a is b


@patch("connectors.hubspot_api.requests.Session")
@patch("connectors.hubspot_api.CONFIG")
def test_get_deals_single_page_yields_one_dataframe(mock_config, mock_session_cls):
    """get_deals with one page yields a single DataFrame with requested properties."""
    mock_config.HUBSPOT_API_KEY = "test-key"
    HubspotConn._singleton = None

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "results": [
            {
                "id": "1",
                "properties": {
                    "amount": "100",
                    "dealname": "Deal A",
                    "hs_object_id": "111",
                },
            },
        ],
        "paging": {},
    }
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    mock_session_cls.return_value.__enter__.return_value = mock_session
    mock_session_cls.return_value.__exit__.return_value = None

    conn = HubspotConn.get_instance()
    pages = list(conn.get_deals(properties=["amount", "dealname", "hs_object_id"]))

    assert len(pages) == 1
    df = pages[0]
    assert list(df.columns) == ["amount", "dealname", "hs_object_id"]
    assert len(df) == 1
    assert df.iloc[0]["amount"] == "100"
    assert df.iloc[0]["dealname"] == "Deal A"
    assert df.iloc[0]["hs_object_id"] == "111"

    mock_session.get.assert_called_once()
    call_url = mock_session.get.call_args[1]["url"]
    assert "amount,dealname,hs_object_id" in call_url
    assert "objects/0-3" in call_url


@patch("connectors.hubspot_api.requests.Session")
@patch("connectors.hubspot_api.CONFIG")
def test_get_deals_multiple_pages_follows_next_link(mock_config, mock_session_cls):
    """get_deals with paging yields multiple DataFrames and follows paging.next.link."""
    mock_config.HUBSPOT_API_KEY = "test-key"
    HubspotConn._singleton = None

    def response_for(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url == "https://api.hubapi.com/crm/v3/objects/0-3?properties=amount&limit=1":
            resp.json.return_value = {
                "results": [{"id": "1", "properties": {"amount": "100"}}],
                "paging": {"next": {"link": "https://api.hubapi.com/next-page"}},
            }
        else:
            resp.json.return_value = {
                "results": [{"id": "2", "properties": {"amount": "200"}}],
                "paging": {},
            }
        return resp

    mock_session = MagicMock()
    mock_session.get.side_effect = response_for
    mock_session_cls.return_value.__enter__.return_value = mock_session
    mock_session_cls.return_value.__exit__.return_value = None

    conn = HubspotConn.get_instance()
    pages = list(conn.get_deals(properties=["amount"]))

    assert len(pages) == 2
    assert pages[0].iloc[0]["amount"] == "100"
    assert pages[1].iloc[0]["amount"] == "200"
    assert mock_session.get.call_count == 2
    assert (
        mock_session.get.call_args_list[1][1]["url"]
        == "https://api.hubapi.com/next-page"
    )


@patch("connectors.hubspot_api.requests.Session")
@patch("connectors.hubspot_api.CONFIG")
def test_get_deals_empty_results_stops(mock_config, mock_session_cls):
    """get_deals with no results yields nothing and does not loop."""
    mock_config.HUBSPOT_API_KEY = "test-key"
    HubspotConn._singleton = None

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"results": [], "paging": {}}
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    mock_session_cls.return_value.__enter__.return_value = mock_session
    mock_session_cls.return_value.__exit__.return_value = None

    conn = HubspotConn.get_instance()
    pages = list(conn.get_deals(properties=["amount"]))

    assert len(pages) == 0
    mock_session.get.assert_called_once()
