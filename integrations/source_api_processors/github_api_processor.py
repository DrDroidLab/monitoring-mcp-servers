import logging
import requests

from integrations.processor import Processor

logger = logging.getLogger(__name__)

main_branches = ['master', 'main']


class GithubAPIProcessor(Processor):
    def __init__(self, api_key, org):
        self.__api_key = api_key
        self.org = org

    def test_connection(self):
        try:
            url = 'https://api.github.com/octocat'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            response = requests.request("GET", url, headers=headers)
            if response.status_code == 200:
                return True
            else:
                raise Exception(f"Github Connection failed: {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.test_connection:: Exception occurred with error: {e}")
            raise e

    def get_file_commits(self, repo, file_path):
        try:
            commits_url = f'https://api.github.com/repos/{self.org}/{repo}/commits?path={file_path}&per_page=100'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }

            # Fetch the latest commits for the file
            response = requests.request("GET", commits_url, headers=headers)
            if response:
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"GithubAPIProcessor.get_file_commits:: Error occurred while fetching github commit "
                                 f"details for file: {file_path} in {self.org}/{repo} with status_code: "
                                 f"{response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_file_commits:: Exception occurred while fetching github commit issue "
                         f"details for file: {file_path} in {self.org}/{repo} with error: {e}")
        return None

    def list_all_repos(self):
        try:
            page = 1
            repo_url = f'https://api.github.com/orgs/{self.org}/repos'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            all_repos = []
            while True:
                data = {'page': page, 'per_page': 100}
                response = requests.request("GET", repo_url, headers=headers, params=data)
                if response:
                    if response.status_code == 200:
                        if len(response.json()) > 0:
                            all_repos.extend(response.json())
                            page += 1
                            continue
                    else:
                        logger.error(f"GithubAPIProcessor.list_all_repos:: Error occurred while fetching github repos "
                                     f"in {self.org} with status_code: {response.status_code} and response: "
                                     f"{response.text}")
                break
            return all_repos
        except Exception as e:
            logger.error(f"GithubAPIProcessor.list_all_repos:: Exception occurred while fetching github repos "
                         f"in {self.org} with error: {e}")
        return None

    def get_commit_sha(self, repo, commit_sha):
        try:
            commit_url = f'https://api.github.com/repos/{self.org}/{repo}/commits/{commit_sha}'
            payload = {}
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            response = requests.request("GET", commit_url, headers=headers, data=payload)
            if response:
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"GithubAPIProcessor.get_commit_sha:: Error occurred while fetching github commit "
                                 f"details for commit_sha: {commit_sha} in {self.org}/{repo} with status_code: "
                                 f"{response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_commit_sha:: Exception occurred while fetching github commit details "
                         f"for commit_sha: {commit_sha} in {self.org}/{repo} with error: {e}")
        return None

    def fetch_file(self, repo, file_path):
        try:
            file_url = f'https://api.github.com/repos/{self.org}/{repo}/contents/{file_path}'
            payload = {}
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
            }
            response = requests.request("GET", file_url, headers=headers, data=payload)
            if response:
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"GithubAPIProcessor.fetch_file:: Error occurred while fetching github file details "
                                 f"for file: {file_path} in {self.org}/{repo} with status_code: "
                                 f"{response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.fetch_file:: Exception occurred while fetching github file details for "
                         f"file: {file_path} in {self.org}/{repo} with error: {e}")
        return None

    def update_file(self, repo, file_path, sha, content, committer_name, committer_email, branch_name=None):
        try:
            file_url = f'https://api.github.com/repos/{self.org}/{repo}/contents/{file_path}'
            payload = {
                'message': 'File Update from Doctor Droid',
                'committer': {
                    'name': committer_name,
                    'email': committer_email
                },
                "content": content,
                'sha': sha,
            }
            if branch_name:
                created_branch = self.create_branch(repo, branch_name)
                if not created_branch:
                    logger.error(f"GithubAPIProcessor.update_file:: Error occurred while creating branch: {branch_name}"
                                 f" in {self.org}/{repo}")
                    return None
                payload['branch'] = branch_name
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
            }
            response = requests.request("PUT", file_url, headers=headers, json=payload)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GithubAPIProcessor.update_file:: Error occurred while updating github file details for "
                             f"file: {file_path} in {self.org}/{repo} with status_code: {response.status_code} and "
                             f"response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.update_file:: Exception occurred while updating github file details for "
                         f"file: {file_path} in {self.org}/{repo} with error: {e}")
        return None

    def list_all_branch(self, repo, protected=False):
        page = 1
        try:
            branch_url = f'https://api.github.com/repos/{self.org}/{repo}/branches'
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
            }
            all_branches = []
            while True:
                data = {'page': page, 'per_page': 100, 'protected': 'false'}
                if protected:
                    data = {'page': page, 'per_page': 100, 'protected': 'true'}
                response = requests.request("GET", branch_url, headers=headers, params=data)
                if response.status_code == 200:
                    if len(response.json()) > 0:
                        all_branches.extend(response.json())
                        page += 1
                        continue
                else:
                    logger.error(f"GithubAPIProcessor.get_branch:: Error occurred while getting all github branches "
                                 f"in {self.org}/{repo} with status_code: {response.status_code} "
                                 f"and response: {response.text}")
                break
            return all_branches
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_branch:: Exception occurred while getting all github branches "
                         f"in {self.org}/{repo} with error: {e}")
        return None

    def get_branch(self, repo, branch_name):
        try:
            branch_url = f'https://api.github.com/repos/{self.org}/{repo}/branches/{branch_name}'
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
            }
            response = requests.request("GET", branch_url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GithubAPIProcessor.get_branch:: Error occurred while getting github branch details for "
                             f"branch: {branch_name} in {self.org}/{repo} with status_code: {response.status_code} and "
                             f"response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_branch:: Exception occurred while getting github branch details for "
                         f"branch: {branch_name} in {self.org}/{repo} with error: {e}")
        return None

    def create_branch(self, repo, branch_name):
        try:
            is_existing = self.get_branch(repo, branch_name)
            if is_existing:
                logger.warning(f"GithubAPIProcessor.create_branch:: Branch {branch_name} already exists in "
                               f"{self.org}/{repo}")
                return is_existing

            all_branches = self.list_all_branch(repo, protected=True)

            main_branch = None
            for b in all_branches:
                if b['name'] in main_branches:
                    main_branch = b
                    break
            if not main_branch:
                logger.error(f"GithubAPIProcessor.create_branch:: Main branch not found in {self.org}/{repo}")
                return None

            github_ref_url = f'https://api.github.com/repos/{self.org}/{repo}/git/refs'
            master_branch_sha = main_branch['commit']['sha']
            data = {
                "ref": f'refs/heads/{branch_name}',
                "sha": master_branch_sha
            }
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
            }
            response = requests.request("POST", github_ref_url, headers=headers, json=data)
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"GithubAPIProcessor.create_branch:: Error occurred while creating github branch: "
                             f"{branch_name} in {self.org}/{repo} with status_code: {response.status_code} and "
                             f"response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.create_branch:: Exception occurred while creating github branch: "
                         f"{branch_name} in {self.org}/{repo} with error: {e}")
        return None
