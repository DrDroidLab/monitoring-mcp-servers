# import logging
#
# from integrations.source_metadata_extractor import SourceMetadataExtractor
# from integrations.source_api_processors.gcm_api_processor import GcmApiProcessor
# from protos.base_pb2 import Source, SourceModelType
# from utils.logging_utils import log_function_call
#
# logger = logging.getLogger(__name__)
#
#
# class GcmSourceMetadataExtractor(SourceMetadataExtractor):
#
#     def __init__(self, project_id, service_account_json, connector_name: str = None):
#         self.__project_id = project_id
#         self.__service_account_json = service_account_json
#         super().__init__(connector_name, Source.GCM)
#
#     @log_function_call
#     def extract_metric_descriptors(self):
#         model_type = SourceModelType.GCM_METRIC
#         model_data = {}
#         gcm_api_processor = GcmApiProcessor(self.__project_id, self.__service_account_json)
#
#         try:
#             all_metric_descriptors = gcm_api_processor.fetch_metrics_list()
#             for descriptor in all_metric_descriptors:
#                 try:
#                     metric_type = descriptor['type']
#
#                     model_data[metric_type] = {
#                         'metric_type': metric_type
#                     }
#
#                     if save_to_db:
#                         self.create_or_update_model_metadata(model_type, metric_type, model_data[metric_type])
#                 except Exception as e:
#                     logger.error(f'Error processing metric descriptor: {e}')
#         except Exception as e:
#             logger.error(f'Error extracting metric descriptors: {e}')
#
#         return model_data
