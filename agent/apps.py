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
