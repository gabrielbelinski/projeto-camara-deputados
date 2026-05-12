from requests import RequestException
import logging
import retry_session as rs

class FetchClient:
    def __init__(self):
        self.session = rs.create_retry_session()

    def close(self):
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    def paginate(self, url, params=None):
        while url:
            try:
                response = self.session.get(url, params=params, timeout=30, headers={"accept": "application/json", "User-Agent": "projeto-engdados-camara/1.0"})
                response.raise_for_status()

                data_payload = response.json()
                data = data_payload.get("dados")
                links = data_payload.get("links")

                for item in data:
                    yield item
                    
                next_url = next((link["href"] for link in links if link["rel"] == "next"), None)
                url = next_url
                params = None 
            except RequestException as e:
                logging.exception(f"Houve um erro ao acessar {url}")
                return