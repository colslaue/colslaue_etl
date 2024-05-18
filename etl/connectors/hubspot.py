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
            "pipeline": result["properties"]["pipeline"],
            "hubspot_owner_id": result["properties"]["hubspot_owner_id"]
        }
        flattened_data.append(flat_dict)
    return flattened_data


def flatten_companies(data):
    flattened_data = []
    for result in data["results"]:
        flat_dict = {
            "id": result["id"],
            "createdate": result["properties"]["createdate"],
            "name": result["properties"]["name"],
            "annualrevenue": result["properties"]["annualrevenue"],
            "hubspot_owner_id": result["properties"]["hubspot_owner_id"],
            "country": result["properties"]["country"]
        }
        flattened_data.append(flat_dict)
    return flattened_data


def flatten_deal_company(data):
    flattened_data = []
    for result in data["results"]:
        for company in result["associations"]["companies"]["results"]:
            flat_dict = {
                "deal_id": result["id"],
                "companyid": company["id"],
                "type": company["type"]
            }
            flattened_data.append(flat_dict)
    return flattened_data


def flatten_users(data):
    flattened_data = []
    for result in data["results"]:
        flat_dict = {
            "id": result["id"],
            "email": result["email"],
            "firstname": result["firstName"],
            "lastName": result["lastName"],
            "userid": result["userId"]
        }
        flattened_data.append(flat_dict)
    return flattened_data


def flatten_contacts(data):
    flattened_data = []
    for result in data["results"]:
        flat_dict = {
            "id": result["id"],
            "firstname": result["properties"]["firstname"],
            "lastname": result["properties"]["lastname"],
            "hs_lifecyclestage_customer_date": result["properties"]["hs_lifecyclestage_customer_date"],
            "hs_lifecyclestage_marketingqualifiedlead_date": result["properties"]["hs_lifecyclestage_marketingqualifiedlead_date"],
            "hs_lifecyclestage_salesqualifiedlead_date": result["properties"]["hs_lifecyclestage_salesqualifiedlead_date"],
            "country": result["properties"]["country"],
            "jobtitle": result["properties"]["jobtitle"]
        }
        flattened_data.append(flat_dict)
    return flattened_data
