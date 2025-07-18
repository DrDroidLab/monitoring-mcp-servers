import logging

import requests

from integrations.processor import Processor
from protos.base_pb2 import TimeRange

logger = logging.getLogger(__name__)


class GrafanaApiProcessor(Processor):
    client = None

    def __init__(self, grafana_host, grafana_api_key, ssl_verify='true'):
        self.__host = grafana_host
        self.__api_key = grafana_api_key
        self.__ssl_verify = False if ssl_verify and ssl_verify.lower() == 'false' else True
        self.headers = {
            'Authorization': f'Bearer {self.__api_key}'
        }

    def test_connection(self):
        try:
            url = '{}/api/datasources'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify, timeout=20)
            if response and response.status_code == 200:
                return True
            else:
                status_code = response.status_code if response else None
                raise Exception(
                    f"Failed to connect with Grafana. Status Code: {status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana data sources with error: {e}")
            raise e

    def check_api_health(self):
        try:
            url = '{}/api/health'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify, timeout=20)
            if response and response.status_code == 200:
                return response.json()
            else:
                status_code = response.status_code if response else None
                raise Exception(
                    f"Failed to connect with Grafana. Status Code: {status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while checking grafana api health with error: {e}")
            raise e

    def fetch_data_sources(self):
        try:
            url = '{}/api/datasources'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana data sources with error: {e}")
            raise e

    def fetch_dashboards(self):
        try:
            url = '{}/api/search'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana dashboards with error: {e}")
            raise e

    def fetch_dashboard_details(self, uid):
        try:
            url = '{}/api/dashboards/uid/{}'.format(self.__host, uid)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana dashboard details with error: {e}")
            raise e

    # Promql Datasource APIs
    def fetch_promql_metric_labels(self, promql_datasource_uid, metric_name):
        try:
            url = '{}/api/datasources/proxy/uid/{}/api/v1/labels?match[]={}'.format(self.__host, promql_datasource_uid,
                                                                                    metric_name)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching promql metric labels with error: {e}")
            raise e

    def fetch_promql_metric_label_values(self, promql_datasource_uid, metric_name, label_name):
        try:
            url = '{}/api/datasources/proxy/uid/{}/api/v1/label/{}/values?match[]={}'.format(self.__host,
                                                                                             promql_datasource_uid,
                                                                                             label_name, metric_name)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching promql metric labels with error: {e}")
            raise e

    # TODO(MG): Only kept for backward compatibility. Remove this method.
    def fetch_promql_metric_timeseries(self, promql_datasource_uid, query, start, end, step):
        try:
            url = '{}/api/datasources/proxy/uid/{}/api/v1/query_range?query={}&start={}&end={}&step={}'.format(
                self.__host, promql_datasource_uid, query, start, end, step)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while getting promql metric timeseries with error: {e}")
            raise e

    def fetch_alert_rules(self):
        try:
            url = '{}/api/v1/provisioning/alert-rules'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Failed to fetch alert rules. Status Code: {response.status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana alert rules with error: {e}")
            raise e

    def fetch_dashboard_variable_label_values(self, promql_datasource_uid, label_name):
        try:
            url = f'{self.__host}/api/datasources/proxy/uid/{promql_datasource_uid}/api/v1/label/{label_name}/values'
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json().get('data', [])
            else:
                logger.error(f"Failed to fetch label values for {label_name}. Status: {response.status_code}, Body: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception occurred while fetching promql metric labels for {label_name} with error: {e}")
            return []

    def panel_query_datasource_api(self, tr: TimeRange, queries, interval_ms=300000):
        try:
            if not queries or len(queries) == 0:
                raise ValueError("No queries provided.")

            url = f"{self.__host}/api/ds/query"

            from_tr = int(tr.time_geq * 1000)
            to_tr = int(tr.time_lt * 1000)

            for query in queries:
                query["intervalMs"] = interval_ms # 5 minutes default, in milliseconds

            payload = {
                "queries": queries,
                "from": str(from_tr),
                "to": str(to_tr)
            }

            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code == 429:
                logger.info("Grafana query API responded with 429 (rate limited). Headers: %s", response.headers)
                return None

            response.raise_for_status()

            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error("Grafana query API error: status code %s, response: %s", e.response.status_code, e.response.text)
            raise e
        except Exception as e:
            logger.error("Exception occurred while querying Grafana datasource: %s", e)
            raise e
