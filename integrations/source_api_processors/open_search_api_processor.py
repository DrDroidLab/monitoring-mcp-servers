import logging
import requests
from requests.auth import HTTPBasicAuth

from integrations.processor import Processor

logger = logging.getLogger(__name__)


class OpenSearchApiProcessor(Processor):
    client = None

    def __init__(self, protocol: str, host: str, port: str, username: str, password: str, verify_certs: bool = False):
        self.base_url = f"{protocol}://{host}:{port}"
        self.auth = HTTPBasicAuth(username, password)
        self.verify_certs = verify_certs

    def _make_request(self, method, endpoint, data=None, params=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.request(
                method,
                url,
                auth=self.auth,
                verify=self.verify_certs,
                json=data,
                params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error making request to OpenSearch: {e}")
            raise

    def test_connection(self):
        try:
            indices = self._make_request("GET", "_cat/indices?format=json")
            if len(indices) > 0:
                return True
            else:
                raise Exception("OpenSearch Connection Error:: No indices found in OpenSearch")
        except Exception as e:
            logger.error(f"Exception occurred while fetching OpenSearch indices with error: {e}")
            raise e

    def fetch_indices(self):
        try:
            indices = self._make_request("GET", "_alias")
            return list(indices.keys())
        except Exception as e:
            logger.error(f"Exception occurred while fetching OpenSearch indices with error: {e}")
            raise e

    def query(self, index, query):
        try:
            result = self._make_request("POST", f"{index}/_search", data=query)
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching OpenSearch data with error: {e}")
            raise e

    def get_document(self, index, doc_id):
        try:
            result = self._make_request("GET", f"{index}/_doc/{doc_id}", params={"preference": "_primary_first"})
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching OpenSearch data with error: {e}")
            raise e
