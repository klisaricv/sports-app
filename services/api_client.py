# services/api_client.py
import random, time, requests
from requests.adapters import HTTPAdapter
import os


try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None

SESSION = requests.Session()
# header (API key) dodaš ovdje
SESSION.headers.update({'x-apisports-key': os.getenv('APISPORTS_KEY', 'YOUR_API_KEY_HERE')})

if Retry is not None:
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    SESSION.mount("http://", adapter)
    SESSION.mount("https://", adapter)

def rate_limited_request(url, params=None, max_retries=5, timeout=20):
    for attempt in range(max_retries+1):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
        except requests.RequestException:
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', '2'))
            time.sleep(retry_after + random.uniform(0.5, 1.5))
            continue
        time.sleep(2 ** attempt)
    return None
