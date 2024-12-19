import logging

from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.github_api_processor import GithubAPIProcessor
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class GithubSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, api_key, org):
        self.__api_key = api_key
        self.org = org
        super().__init__(request_id, connector_name, Source.GITHUB)

    @log_function_call
    def extract_repos(self):
        model_type = SourceModelType.GITHUB_REPOSITORY
        model_data = {}
        gh_processor = GithubAPIProcessor(self.__api_key, self.org)
        try:
            all_repos = gh_processor.list_all_repos()
            for repo in all_repos:
                try:
                    repo_name = repo['name']
                    model_data[repo_name] = repo
                except Exception as e:
                    logger.error(f'Error processing github repos: {e}')
        except Exception as e:
            logger.error(f'Error extracting github repos: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
