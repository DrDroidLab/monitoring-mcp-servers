import logging
from datetime import datetime, date

from protos.base_pb2 import Source
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class SourceMetadataExtractor:

    def __init__(self, request_id: str, connector_name: str, source: Source):
        self.request_id = request_id
        self.connector_name = connector_name
        self.source = source

    @log_function_call
    def create_or_update_model_metadata(self, model_type, collected_models):
        try:
            for model_uid, metadata in collected_models.items():
                for k, v in metadata.items():
                    if isinstance(v, (datetime, date)):
                        metadata[k] = v.isoformat()
        except Exception as e:
            logger.error(f'Error creating or updating model_type: {model_type}, model_uid: {model_uid}: {e}')
