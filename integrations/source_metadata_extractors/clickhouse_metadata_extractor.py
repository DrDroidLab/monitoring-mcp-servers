import logging

from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.clickhouse_db_processor import ClickhouseDBProcessor
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class ClickhouseSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, interface: str, host: str, port: str, user: str,
                 password: str):
        self.__ch_db_processor = ClickhouseDBProcessor(interface, host, port, user, password)

        super().__init__(request_id, connector_name, Source.CLICKHOUSE)

    @log_function_call
    def extract_database(self):
        model_type = SourceModelType.CLICKHOUSE_DATABASE
        try:
            databases = self.__ch_db_processor.fetch_databases()
        except Exception as e:
            logger.error(f'Error fetching databases: {e}')
            return
        if not databases:
            return
        model_data = {}
        for db in databases:
            model_data[db] = {}
            if len(model_type) >= 10:
                self.create_or_update_model_metadata(model_type, model_data)
                model_data = {}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
