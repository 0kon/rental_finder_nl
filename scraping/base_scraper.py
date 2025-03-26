import logging
import requests
from abc import ABC, abstractmethod
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class BaseScraper(ABC):
    def __init__(self, base_url: str, use_proxy: bool = False):
        self.base_url = base_url.rstrip("/") if base_url is not None else ""
        self.use_proxy = use_proxy
        self.logger = logging.getLogger(self.__class__.__name__)

        # Create a requests.Session for persistent connections
        self.session = requests.Session()
        self._configure_session()

    def _configure_session(self, total_retries: int = 3, backoff_factor: float = 0.3):
        # Sets up retry logic for certain HTTP errors or timeouts.
        retries = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504, 404]  # retry codes
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    @abstractmethod
    def scrape_listings(self):
        pass

    def make_request(self, url: str, method: str = "GET", **kwargs):
        # A helper to handle HTTP requests (GET or POST).
        # Integrates optional proxy/user-agent rotation if self.use_proxy is True.
        try:
            if method.upper() == "GET":
                resp = self.session.get(url, timeout=15, **kwargs)
            elif method.upper() == "POST":
                resp = self.session.post(url, timeout=15, **kwargs)
            else:
                self.logger.warning(f"Unsupported HTTP method '{method}', defaulting to GET.")
                resp = self.session.get(url, timeout=15, **kwargs)

            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None


