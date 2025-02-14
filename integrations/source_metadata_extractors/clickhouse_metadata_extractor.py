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
            logger.error(f"Exception occurred while fetching clickhouse databases with error: {e}")
            return
        if not databases:
            return
        model_data = {}
        try:
            db_table_map = self.__ch_db_processor.fetch_tables(databases)
            db_table_details_map = self.__ch_db_processor.fetch_table_details(db_table_map)
            if db_table_details_map:
                for db_name, table_details in db_table_details_map.items():
                    model_data[db_name] = table_details
            self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Exception occurred while fetching clickhouse tables with error: {e}")
        return model_data
