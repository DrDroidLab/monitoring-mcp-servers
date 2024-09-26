import logging

import requests
from celery import shared_task

from protos.connectors.connector_pb2 import Connector
from utils.credentilal_utils import credential_yaml_to_connector_proto
from utils.proto_utils import proto_to_dict

logger = logging.getLogger(__name__)


@shared_task(max_retries=3, default_retry_delay=10)
def register_connectors(drd_cloud_host, drd_cloud_api_token, loaded_connections):
    connectors = []
    for c, metadata in loaded_connections.items():
        connector_proto = credential_yaml_to_connector_proto(c, metadata)
        connectors.append(proto_to_dict(Connector(name=connector_proto.name, type=connector_proto.type)))

    response = requests.post(f'{drd_cloud_host}/connectors/proxy/register',
                             headers={'Authorization': f'Bearer {drd_cloud_api_token}'},
                             json={'connectors': connectors})
    if response.status_code != 200:
        logger.error(f'Failed to register connectors with DRD Cloud: {response.json()}')
