import json
import logging

from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.kubectl_api_processor import KubectlApiProcessor
from protos.base_pb2 import Source, SourceModelType

from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class KubernetesMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, **credentials):
        super().__init__(request_id, connector_name, Source.KUBERNETES)
        self.__kubectl_api_processor = KubectlApiProcessor(**credentials)

    @log_function_call
    def extract_namespaces(self):
        model_type = SourceModelType.KUBERNETES_NAMESPACE
        model_data = {}
        try:
            command = "get namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                namespace_name = metadata.get('name')
                if not namespace_name:
                    continue
                model_data[namespace_name] = item
            logger.info(f"Extracted {len(model_data)} namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes namespaces: {e}")

    @log_function_call
    def extract_services(self):
        model_type = SourceModelType.KUBERNETES_SERVICE
        model_data = {}
        try:
            command = "get services --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                service_name = metadata.get('name')
                service_namespace = metadata.get('namespace', 'default')
                if not service_name:
                    continue
                namespaced_name = f"{service_namespace}/{service_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} services from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes services: {e}")

    @log_function_call
    def extract_deployments(self):
        model_type = SourceModelType.KUBERNETES_DEPLOYMENT
        model_data = {}
        try:
            command = "get deployments --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                deployment_name = metadata.get('name')
                deployment_namespace = metadata.get('namespace', 'default')
                if not deployment_name:
                    continue
                namespaced_name = f"{deployment_namespace}/{deployment_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} deployments from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes deployments: {e}")
