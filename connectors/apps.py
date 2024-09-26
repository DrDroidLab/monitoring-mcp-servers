from django.apps import AppConfig
from django.conf import settings

from connectors.tasks import register_connectors


class ConnectorsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'connectors'

    def ready(self):
        if not settings.LOADED_CONNECTIONS:
            raise ValueError(f'No connections found in {settings.SECRETS_FILE_PATH}')

        drd_cloud_host = settings.DRD_CLOUD_API_HOST
        drd_cloud_api_token = settings.DRD_CLOUD_API_TOKEN
        loaded_connections = settings.LOADED_CONNECTIONS

        if not loaded_connections:
            raise ValueError(f'No connections found in {settings.SECRETS_FILE_PATH}')

        register_connectors.delay(drd_cloud_host, drd_cloud_api_token, loaded_connections)
