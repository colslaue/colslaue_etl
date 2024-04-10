import requests
from requests import RequestException
from config import CONFIG


class HubspotAPI:
    _singleton = None
    base_url = "https://api.hubapi.com"
    full_url = None
    endpoint = None
    header = {
        "authorization": f"Bearer {CONFIG.HUBSPOT_API_KEY}"
    }

    def __init__(self, endpoint):
        self.full_url = f"{self.base_url}{endpoint}"

    @classmethod
    def get_instance(cls, endpoint):
        if cls._singleton is None or endpoint != cls.endpoint:
            cls._singleton = HubspotAPI(endpoint)
        return cls._singleton

    def get_data(self):
        try:
            response = requests.get(self.full_url, headers=self.header)
            response.raise_for_status()
        except RequestException as e:
            return e
        return response.json()


def flatten_deals(data):
    flattened_data = []
    for result in data["results"]:
        flat_dict = {
            "id": result["id"],
            "amount": result["properties"]["amount"],
            "closedate": result["properties"]["closedate"],
            "createdate": result["properties"]["createdate"],
            "dealname": result["properties"]["dealname"],
            "dealstage": result["properties"]["dealstage"],
            "pipeline": result["properties"]["pipeline"]
        }
        flattened_data.append(flat_dict)
    return flattened_data
