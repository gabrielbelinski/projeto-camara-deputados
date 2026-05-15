from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

def create_retry_session() -> requests.Session:
    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=[429,
            500,
            502,
            503,
            504],
        allowed_methods=["GET"],
        raise_on_status=False
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=50
    )

    session : requests.Session = requests.Session()
    session.mount("https://", adapter)
    #session.mount("http://", adapter)
    
    return session