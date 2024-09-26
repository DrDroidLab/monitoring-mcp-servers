import logging

from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.elastic_search_api_processor import ElasticSearchApiProcessor
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class ElasticSearchSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, host: str, port: str, api_key_id: str, api_key: str, connector_name: str = None):
        self.__es_api_processor = ElasticSearchApiProcessor(host, port, api_key_id, api_key)

        super().__init__(connector_name, Source.ELASTIC_SEARCH)

    @log_function_call
    def extract_index(self):
        model_type = SourceModelType.ELASTIC_SEARCH_INDEX
        try:
            indexes = self.__es_api_processor.fetch_indices()
        except Exception as e:
            logger.error(f'Error fetching databases: {e}')
            return
        if not indexes:
            return
        model_data = {}
        for ind in indexes:
            model_data[ind] = {}
            if len(model_data) >= 10:
                self.create_or_update_model_metadata(model_type, model_data)
                model_data = {}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
