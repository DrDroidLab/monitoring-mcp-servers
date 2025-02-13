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

    def get_file_commits(self, repo, file_path, branch='main'):
        try:
            commits_url = f'https://api.github.com/repos/{self.org}/{repo}/commits?path={file_path}&per_page=100&sha={branch}'
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
                                 f"details for file: {file_path} in {self.org}/{repo}/{branch} with status_code: "
                                 f"{response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_file_commits:: Exception occurred while fetching github commit issue "
                         f"details for file: {file_path} in {self.org}/{repo}/{branch} with error: {e}")
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

    def search_file(self, repo, file_path):
        """
        Search for a file in repository by progressively trying more specific paths.

        Args:
            repo: Repository name
            file_path: Full path to search (e.g. "/code/integrations/source_api_processors/grafana_api_processor.py")

        Returns:
            Dict containing:
                - status: "success" or "error"
                - file_path: Full correct path if found
                - message: Error message if not found
        """
        try:
            # Clean up the file path - remove leading slash and any 'code/' prefix
            cleaned_path = file_path.lstrip('/')
            if cleaned_path.startswith('code/'):
                cleaned_path = cleaned_path[5:]

            # Split path into components
            path_parts = cleaned_path.split('/')
            file_name = path_parts[-1]

            # Start with just the filename
            current_search = file_name

            # Try direct fetch first with cleaned path
            direct_result = self.fetch_file(repo, cleaned_path)
            if direct_result is not None:
                return {
                    "status": "success",
                    "file_path": cleaned_path
                }

            # Get all branches
            found_files = []
            for branch in main_branches:
                commits = self.get_branch_commits(repo, branch)
                if commits and len(commits) > 0:
                    latest_commit = commits[0]
                    commit_sha = latest_commit['sha']

                    # Get the tree for this commit
                    commit_details = self.get_commit_sha(repo, commit_sha)
                    if commit_details and 'files' in commit_details:
                        # Start by searching just filename
                        matching_files = [
                            file['filename'] for file in commit_details['files']
                            if file_name in file['filename']
                        ]
                        found_files.extend(matching_files)

                        if len(found_files) == 0:
                            return {
                                "status": "error",
                                "message": f"File {file_name} not found in repository {self.org}/{repo}"
                            }

                        # If multiple matches found, progressively add path components
                        # from right to left until we get a unique match
                        if len(found_files) > 1:
                            search_path = file_name
                            for path_part in reversed(path_parts[:-1]):
                                search_path = f"{path_part}/{search_path}"
                                filtered_files = [
                                    f for f in found_files
                                    if f.endswith(search_path)
                                ]
                                if len(filtered_files) == 1:
                                    return {
                                        "status": "success",
                                        "file_path": filtered_files[0]
                                    }
                                elif len(filtered_files) > 1:
                                    found_files = filtered_files
                                else:
                                    # No matches with this path component
                                    continue

                            # If we still have multiple matches, return the first one
                            # that most closely matches our path structure
                            if len(found_files) > 0:
                                # Sort by similarity to original path
                                found_files.sort(key=lambda x: len(set(x.split('/')) & set(path_parts)), reverse=True)
                                return {
                                    "status": "success",
                                    "file_path": found_files[0]
                                }
                        elif len(found_files) == 1:
                            return {
                                "status": "success",
                                "file_path": found_files[0]
                            }

            return {
                "status": "error",
                "message": f"Could not find unique match for {file_name} in repository {self.org}/{repo}"
            }

        except Exception as e:
            logger.error(f"GithubAPIProcessor.search_file:: Exception occurred while searching for file: "
                         f"{file_path} in {self.org}/{repo} with error: {e}")
            return {
                "status": "error",
                "message": f"Error searching file: {str(e)}"
            }

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

    def get_branch_commits(self, repo, branch='main', time_since=None, time_until=None, author=None):
        try:
            query_params = f'sha={branch}&per_page=100'
            if time_since:
                query_params += f'&since={time_since}'
            if time_until:
                query_params += f'&until={time_until}'
            if author:
                query_params += f'&author={author}'
            recent_commits_url = f'https://api.github.com/repos/{self.org}/{repo}/commits?{query_params}'
            print("url::", recent_commits_url)
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }

            # Fetch the latest commits for the file
            response = requests.request("GET", recent_commits_url, headers=headers)
            if response:
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"GithubAPIProcessor.get_branch_commits:: Error occurred while fetching github commit "
                                 f"details for branch: {branch} in {self.org}/{repo} with status_code: "
                                 f"{response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_branch_commits:: Exception occurred while fetching github commit issue "
                         f"details for branch: {branch} in {self.org}/{repo} with error: {e}")
        return None

    def get_recent_merges(self, repo, branch='main'):
        try:
            recent_pulls_url = f'https://api.github.com/repos/{self.org}/{repo}/pulls?base={branch}&per_page=100&sort=updated&direction=desc&state=closed'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }

            # Fetch the latest commits for the file
            response = requests.request("GET", recent_pulls_url, headers=headers)
            if response:
                if response.status_code == 200:
                    recent_merges = response.json()
                    recent_merges = [merge for merge in recent_merges if merge.get('merged_at')]
                    if recent_merges:
                        recent_merges = sorted(recent_merges, key=lambda x: x.get('merged_at', 0), reverse=True)
                    return recent_merges
                else:
                    logger.error(f"GithubAPIProcessor.get_recent_merges:: Error occurred while fetching github PR merges "
                                 f"details for branch: {branch} in {self.org}/{repo} with status_code: "
                                 f"{response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"GithubAPIProcessor.get_recent_merges:: Exception occurred while fetching github PR merges "
                         f"details for branch: {branch} in {self.org}/{repo} with error: {e}")
        return None

    def list_all_members(self):
        try:
            page = 1
            repo_url = f'https://api.github.com/orgs/{self.org}/members'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            all_members = []
            while True:
                data = {'page': page, 'per_page': 100}
                response = requests.request("GET", repo_url, headers=headers, params=data)
                if response:
                    if response.status_code == 200:
                        if len(response.json()) > 0:
                            all_members.extend(response.json())
                            page += 1
                            continue
                    else:
                        logger.error(f"GithubAPIProcessor.list_all_members:: Error occurred while fetching github members "
                                     f"in {self.org} with status_code: {response.status_code} and response: "
                                     f"{response.text}")
                break
            return all_members
        except Exception as e:
            logger.error(f"GithubAPIProcessor.list_all_members:: Exception occurred while fetching github repos "
                         f"in {self.org} with error: {e}")
        return None
