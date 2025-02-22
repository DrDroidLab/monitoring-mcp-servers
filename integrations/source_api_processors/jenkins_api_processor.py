from datetime import datetime
import json
import logging
import requests
from requests.auth import HTTPBasicAuth

from integrations.processor import Processor

logger = logging.getLogger(__name__)


class JenkinsAPIProcessor(Processor):
    def __init__(self, url, username, api_token=None, crumb=False):
        self.config = {'url': url}
        self.username = username
        self.api_token = api_token
        self.crumb_enabled = crumb
        self.crumb = None
        self.crumb_header = None

        # For API token auth
        if not self.crumb_enabled:
            if not api_token:
                raise Exception("API Token is required when not using crumb authentication")
            self.auth = HTTPBasicAuth(username, api_token)
        else:
            self.auth = HTTPBasicAuth(username, api_token if api_token else "")

    def _make_request_with_crumb(self, method, url, **kwargs):
        try:
            headers = kwargs.pop('headers', {})
            if self.crumb_enabled:
                if not self.crumb:
                    self.fetch_crumb()
                if self.crumb and self.crumb_header:
                    headers[self.crumb_header] = self.crumb
            kwargs['headers'] = headers
            kwargs['auth'] = self.auth
            response = requests.request(method, url, **kwargs)
            if response.status_code == 403 and self.crumb_enabled:
                self.crumb = None
                self.fetch_crumb()
                if self.crumb and self.crumb_header:
                    headers[self.crumb_header] = self.crumb
                    kwargs['headers'] = headers
                    response = requests.request(method, url, **kwargs)
            return response
        except Exception as e:
            logger.error(f"Jenkins Make Request failed: {e}")
            raise e

    def test_connection(self):
        try:
            url = f"{self.config['url']}/api/json"
            response = self._make_request_with_crumb('GET', url)
            if response.status_code == 200:
                return True
            else:
                return Exception(f"Connection failed: {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while testing Jenkins connection with error: {e}")
            raise e

    def fetch_crumb(self):
        try:
            url = f"{self.config['url']}/crumbIssuer/api/json"
            response = requests.get(url, auth=self.auth)
            if response.status_code == 200:
                crumb_data = response.json()
                self.crumb = crumb_data.get("crumb")
                self.crumb_header = crumb_data.get("crumbRequestField")
                return self.crumb
            else:
                logger.error(f"Failed to fetch Jenkins crumb: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching crumb: {e}")
            return None

    def run_job(self, job_name, parameters=None, timeout=120):
        try:
            if parameters and isinstance(parameters, dict) and parameters:  # Check if parameters is a non-empty dict
                url = f"{self.config['url']}/job/{job_name}/buildWithParameters"
                response = self._make_request_with_crumb('POST', url, data=parameters, timeout=timeout)
            else:
                url = f"{self.config['url']}/job/{job_name}/build?delay=0sec"
                response = self._make_request_with_crumb('POST', url, data='', timeout=timeout)
            if response.status_code == 201:
                return True
            else:
                raise Exception(f"Failed to trigger job build: {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while triggering Jenkins job with error: {e}")
            raise e

    def get_job_parameters(self, job_name, timeout=120):
        try:
            url = f"{self.config['url']}/job/{job_name}/api/json"
            response = self._make_request_with_crumb('GET', url, timeout=timeout)

            if response.status_code == 200:
                data = response.json()
                property_list = data.get('property', [])
                for prop in property_list:
                    if 'parameterDefinitions' in prop:
                        return prop['parameterDefinitions']
                return []  # No parameters found
            else:
                logger.error(f"Failed to get job parameters: {response.status_code}, {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception occurred while getting job parameters with error: {e}")
            return []

    def get_last_build(self, job_name, timeout=120):
        try:
            url = f"{self.config['url']}/job/{job_name}/lastBuild/api/json"
            response = self._make_request_with_crumb('GET', url, timeout=timeout)
            result = []
            if response.status_code == 200:
                data = response.json()
                build_time = data.get('timestamp')
                number = data.get('number')
                status = data.get('result')
                actions = data.get('actions', [])
                build_parameters = {}
                for action in actions:
                    if 'parameters' in action:
                        parameters = action['parameters']
                        build_parameters = {param['name']: param['value'] for param in parameters}
                logs = ""
                logs_url = f"{self.config['url']}/job/{job_name}/{number}/consoleText"
                logs_response = self._make_request_with_crumb('GET', logs_url, timeout=timeout)
                if logs_response.status_code == 200:
                    logs = logs_response.text
                result.append({'Time': datetime.fromtimestamp(build_time / 1000.0).strftime('%Y-%m-%d %H:%M:%S'),
                               'Build #': number, 'Status': status,
                               'Parameters': json.dumps(build_parameters, indent=2), 'Logs': logs})
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching Jenkins last build with error: {e}")
            raise e
