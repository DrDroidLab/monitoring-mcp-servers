import logging

from integrations.source_api_processors.argocd_api_processor import ArgoCDAPIProcessor
from integrations.source_metadata_extractor import SourceMetadataExtractor
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)

class ArgoCDSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, argocd_server, argocd_token, account_id=None, connector_id=None):
        # print("Inside ArgoCDSourceMetadataExtractor")
        self.argocd_processor = ArgoCDAPIProcessor(argocd_server, argocd_token)
        super().__init__(account_id, connector_id, Source.ARGOCD)

    @log_function_call
    def extract_applications(self, save_to_db=False):
        model_data = {}
        model_type = SourceModelType.ARGOCD_APPS
        try:
            applications = self.argocd_processor.get_deployment_info()
            if not applications:
                return model_data

            for application in applications.get('items', []):  # Iterate over application items
                try:
                    app_name = application["metadata"]["name"]  # Access the name safely
                    #model_data[app_name] = application
                    model_data[app_name] = {
                        "name": app_name}
                    if save_to_db:
                        self.create_or_update_model_metadata(model_type, app_name, model_data[app_name])  # Move inside loop
                except KeyError as e:
                    logger.error(f"Missing key {e} in application: {application}")  # Handle missing keys
                #print("Model Data: ", model_data)
        except Exception as e:
            logger.error(f'Error extracting ArgoCD applications: {e}')

        return model_data