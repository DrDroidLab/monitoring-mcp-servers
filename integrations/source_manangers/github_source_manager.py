from google.protobuf.struct_pb2 import Struct

from utils.credentilal_utils import generate_credentials_dict
from utils.proto_utils import dict_to_proto

from google.protobuf.wrappers_pb2 import StringValue

from integrations.source_api_processors.github_api_processor import GithubAPIProcessor
from integrations.source_manager import SourceManager
from protos.base_pb2 import TimeRange, Source
from protos.connectors.connector_pb2 import Connector as ConnectorProto
from protos.literal_pb2 import LiteralType
from protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult
from protos.playbooks.source_task_definitions.github_task_pb2 import Github
from protos.ui_definition_pb2 import FormField, FormFieldType


class TimeoutException(Exception):
    pass


class GithubSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.GITHUB
        self.task_proto = Github
        self.task_type_callable_map = {
            Github.TaskType.FETCH_RELATED_COMMITS: {
                'executor': self.fetch_related_commits,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Related Commits for a function',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="owner"),
                              display_name=StringValue(value="Owner"),
                              description=StringValue(value='Enter Owner'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repo"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="Enter File Path"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="function_name"),
                              display_name=StringValue(value="Enter Function name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Github.TaskType.FETCH_FILE: {
                'executor': self.fetch_file,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Files',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="owner"),
                              display_name=StringValue(value="Owner"),
                              description=StringValue(value='Enter Repository Owner'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Enter Repository name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="Enter File Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Github.TaskType.UPDATE_FILE: {
                'executor': self.update_file,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update Files',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="owner"),
                              display_name=StringValue(value="Owner"),
                              description=StringValue(value='Enter Repository Owner'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Enter Repository name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="Enter File Path"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_fetch_button"),
                              display_name=StringValue(value="Fetch File"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.BUTTON_FT),
                    FormField(key_name=StringValue(value="committer_name"),
                              display_name=StringValue(value="Enter Name for Commit message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="committer_email"),
                              display_name=StringValue(value="Enter Email for Commit message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="content"),
                              display_name=StringValue(value="Modified Content"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.CODE_EDITOR_FT),
                    FormField(key_name=StringValue(value="sha"),
                              display_name=StringValue(value="Enter Sha of previous commit"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch_name"),
                              display_name=StringValue(value="Enter Branch name. Defaults to main/master"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
        }

    def get_connector_processor(self, github_connector, **kwargs):
        generated_credentials = generate_credentials_dict(github_connector.type, github_connector.keys)
        return GithubAPIProcessor(**generated_credentials)

    def fetch_related_commits(self, time_range: TimeRange, github_task: Github,
                              github_connector: ConnectorProto) -> PlaybookTaskResult:
        # Loop through the commits and get the diff for each one
        try:
            task = github_task.fetch_related_commits
            repo = task.repo.value
            file_path = task.file_path.value
            function = task.function_name.value
            commits = self.get_connector_processor(github_connector).get_file_commits(repo, file_path)
            related_commits = []
            for commit in commits:
                commit_sha = commit['sha']
                commit_details = self.get_connector_processor(github_connector).get_commit_sha(repo, commit_sha)
                # Get the diff for the specific file
                for file in commit_details['files']:
                    if file['filename'] == file_path:
                        patch = file.get('patch', '')
                        if function in patch:
                            related_commit = {'commit_url': commit['html_url'], 'commit_sha': commit_sha,
                                              'author_name': commit['commit']['author']['name'],
                                              'author_email': commit['commit']['author']['email'],
                                              'commit_date': commit['commit']['author']['date'], 'patch': patch}
                            related_commits.append(related_commit)
            response_struct = dict_to_proto({"all_commits": related_commits}, Struct)
            commit_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=commit_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_related_commits task: {e}")

    def fetch_file(self, time_range: TimeRange, github_task: Github,
                   github_connector: ConnectorProto) -> PlaybookTaskResult:
        try:
            task = github_task.fetch_file
            repo = task.repo.value
            file_path = task.file_path.value
            processor = self.get_connector_processor(github_connector)
            all_repos = processor.list_all_repos()
            all_repo_names = [repo['name'] for repo in all_repos]
            if repo not in all_repo_names:
                raise Exception(f"Repository {repo} not found")
            file_details = processor.fetch_file(repo, file_path)
            response_struct = dict_to_proto(file_details, Struct)
            file_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=file_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_related_commits task: {e}")

    def update_file(self, time_range: TimeRange, github_task: Github,
                    github_connector: ConnectorProto) -> PlaybookTaskResult:
        try:
            task = github_task.update_file
            repo = task.repo.value
            file_path = task.file_path.value
            content = task.content.value
            sha = task.sha.value
            committer_name = task.committer_name.value
            committer_email = task.committer_email.value
            branch_name = task.branch_name.value if task.branch_name.value else None
            processor = self.get_connector_processor(github_connector)
            all_repos = processor.list_all_repos()
            all_repo_names = [repo['name'] for repo in all_repos]
            if repo not in all_repo_names:
                raise Exception(f"Repository {repo} not found")
            file_details = processor.update_file(repo, file_path, sha, content, committer_name, committer_email,
                                                 branch_name)
            response_struct = dict_to_proto(file_details, Struct)
            file_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=file_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_related_commits task: {e}")
