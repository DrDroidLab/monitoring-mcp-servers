import logging
from copy import deepcopy

from integrations.source_api_processors.jenkins_api_processor import JenkinsAPIProcessor
from integrations.source_metadata_extractor import SourceMetadataExtractor
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class JenkinsSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, url: str, username: str, api_token: str,
                 crumb: bool = False, ):
        self.jenkins_processor = JenkinsAPIProcessor(url, username, api_token, crumb=crumb)
        super().__init__(request_id, connector_name, Source.ARGOCD)

    @log_function_call
    def extract_jobs(self):
        model_data = {}
        model_type = SourceModelType.JENKINS_JOBS
        try:
            jobs_response = self.jenkins_processor.test_connection()
            if jobs_response is not True:
                logger.error("Jenkins connection test failed.")
                return model_data
            url = f"{self.jenkins_processor.config['url']}/api/json?tree=jobs[name]"
            response = self.jenkins_processor._make_request_with_crumb('GET', url)
            if response.status_code != 200:
                logger.error(f"Failed to fetch Jenkins jobs: {response.status_code}, {response.text}")
                return model_data
            jobs = response.json().get('jobs', [])
            for job in jobs:
                try:
                    job_name = job.get("name", "")
                    job_class = job.get("_class", "")
                    if job_name:
                        parameters = self.jenkins_processor.get_job_parameters(job_name)
                        model_data[job_name] = {"name": job_name, "class": job_class, "parameters": parameters}
                    if len(model_data) > 5:
                        model_data_copy = deepcopy(model_data)
                        self.create_or_update_model_metadata(model_type, model_data_copy)
                        model_data = {}
                except KeyError as e:
                    logger.error(f"Missing key {e} in job: {job}")
        except Exception as e:
            logger.error(f'Error extracting Jenkins jobs: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
