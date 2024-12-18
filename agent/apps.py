import requests
from django.apps import AppConfig

from agent import settings
from utils.yaml_utils import load_yaml


class AgentConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'agent'

    def ready(self):
        # Path to your YAML file
        filepath = settings.BASE_DIR / 'credentials/secrets.yaml'

        # Load the YAML data and set it as an attribute
        self.yaml_data = load_yaml(filepath)
        if not self.yaml_data:
            raise ValueError(f'No connections found in {filepath}')

        drd_cloud_host = settings.DRD_CLOUD_API_HOST
        drd_cloud_api_token = settings.DRD_CLOUD_API_TOKEN

        # Establish reachability with DRD Cloud
        response = requests.get(f'{drd_cloud_host}/connectors/proxy/ping',
                                headers={'Authorization': f'Bearer {drd_cloud_api_token}'})

        if response.status_code != 200:
            raise ValueError(f'Failed to connect to DRD Cloud: {response.text}')
