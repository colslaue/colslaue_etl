from config import CONFIG
import hubspot

class HubspotConn:
    _singleton = None
    conn = None

    def __init__(self):
        self.conn = hubspot.Client.create(access_token=CONFIG.HUBSPOT_API_KEY)

    @classmethod
    def get_instance(cls):
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton.conn
