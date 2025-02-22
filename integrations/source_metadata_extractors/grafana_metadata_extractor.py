import logging
import re
import time
from copy import deepcopy

from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.grafana_api_processor import GrafanaApiProcessor
from protos.base_pb2 import Source, SourceModelType as SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


def promql_get_metric_name(promql):
    metric_name_end = promql.index('{')
    i = metric_name_end - 1
    while i >= 0:
        if promql[i] == ' ' or promql[i] == '(' or i == 0:
            metric_name_start = i + 1
            if i == 0:
                metric_name_start = 0
            metric_name = promql[metric_name_start:metric_name_end]
            return metric_name
        i -= 1
    return None


def promql_get_metric_optional_label_variable_pairs(promql):
    expr_label_str = promql.split('{')[1].split('}')[0]
    expr_label_str = expr_label_str.replace(' ', '')
    pattern = r'(\w+)\s*([=~]+)\s*"?(\$[\w]+)"?'
    matches = re.findall(pattern, expr_label_str)
    label_value_pairs = {label: value for label, op, value in matches}
    return label_value_pairs


class GrafanaSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, grafana_host, grafana_api_key, ssl_verify="true"):
        self.__grafana_api_processor = GrafanaApiProcessor(grafana_host, grafana_api_key, ssl_verify)
        super().__init__(request_id, connector_name, Source.GRAFANA)

    @log_function_call
    def extract_data_source(self):
        model_type = SourceModelType.GRAFANA_DATASOURCE
        try:
            datasources = self.__grafana_api_processor.fetch_data_sources()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana data sources with error: {e}")
            return
        if not datasources:
            return
        model_data = {}
        for ds in datasources:
            datasource_id = ds['uid']
            model_data[datasource_id] = ds
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_dashboards(self):
        model_type = SourceModelType.GRAFANA_DASHBOARD
        try:
            all_dashboards = self.__grafana_api_processor.fetch_dashboards()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana dashboards with error: {e}")
            return
        if not all_dashboards:
            return
        all_db_dashboard_uids = []
        for db in all_dashboards:
            if db['type'] == 'dash-db':
                all_db_dashboard_uids.append(db['uid'])
        model_data = {}
        for uid in all_db_dashboard_uids:
            try:
                dashboard_details = self.__grafana_api_processor.fetch_dashboard_details(uid)
            except Exception as e:
                logger.error(f"Exception occurred while fetching grafana dashboard details with error: {e}")
                continue
            if not dashboard_details:
                continue
            model_data[uid] = dashboard_details
            if len(model_data) > 5:
                model_data_copy = deepcopy(model_data)
                self.create_or_update_model_metadata(model_type, model_data_copy)
                model_data = {}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
