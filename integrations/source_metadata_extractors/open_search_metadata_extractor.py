from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.open_search_api_processor import OpenSearchApiProcessor, logger
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call


class OpenSearchSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, protocol: str, host: str, username: str, password: str, verify_certs: bool = False,
                 port: str = None, account_id=None, connector_id=None):
        self.__os_api_processor = OpenSearchApiProcessor(protocol, host, username, password, verify_certs, port)

        super().__init__(account_id, connector_id, Source.OPEN_SEARCH)

    @log_function_call
    def extract_index(self, save_to_db=False):
        model_type = SourceModelType.OPEN_SEARCH_INDEX
        try:
            indexes = self.__os_api_processor.fetch_indices()
        except Exception as e:
            logger.error(f"Error while fetching OpenSearch indices: {e}")
            return
        if not indexes:
            return
        model_data = {}
        for ind in indexes:
            model_data[ind] = {}
            if save_to_db:
                self.create_or_update_model_metadata(model_type, ind, {})
        return model_data
