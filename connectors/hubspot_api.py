from config import CONFIG
from contextlib import contextmanager
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
import logging
import pandas as pd
import requests


class HubspotConn:
    _singleton = None

    def __init__(self):
        self._base_url = 'https://api.hubapi.com/crm/v3'
        self._headers = {'Authorization': f'Bearer {CONFIG.HUBSPOT_API_KEY}'}
        self._timeout = (5, 30)

    @classmethod
    def get_instance(cls):
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    @contextmanager
    def _session(self):
        with requests.Session() as session:
            session.headers.update(self._headers)

            retries = Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )

            session.mount('https://', HTTPAdapter(max_retries=retries))

            yield session

    def get_deals(self, properties: list = None, history: bool = False):
        properties = ",".join(properties)
        url: str | None = f"{self._base_url}/objects/0-3?properties={properties}&propertiesWithHistory={properties}&limit=1"
        headers = self._headers
        timeout = self._timeout

        with self._session() as session:
            while url:
                try:
                    response = session.get(url=url, headers=headers, timeout=timeout)
                    response.raise_for_status()
                    data = response.json()

                    results = data.get('results')
                    if not results:
                        break

                    df = pd.DataFrame(results)

                    # get next page if it exists otherwise return None to end the loop
                    paging = data.get('paging')
                    if paging and 'next' in paging:
                        url = paging['next'].get('link')
                    else:
                        url = None

                    yield df

                except Exception as e:
                    logging.error(e)
                    raise

def test():
    conn = HubspotConn.get_instance()
    properties = [
        'dealname',
        'amount'
    ]
    data = conn.get_deals(properties=properties, history=True)
    for page in data:
        print(page.to_dict())
