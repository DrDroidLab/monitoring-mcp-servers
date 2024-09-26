from typing import Union

from django.conf import settings
from django.http import HttpResponse
from google.protobuf.wrappers_pb2 import BoolValue

from integrations.source_facade import source_facade
from utils.credentilal_utils import credential_yaml_to_connector_proto
from utils.static_mappings import integrations_connector_type_connector_keys_map
from protos.base_pb2 import Message
from protos.connectors.api_pb2 import TestConnectorRequest, TestConnectorResponse
from protos.connectors.connector_pb2 import Connector
from utils.decorators import account_post_api


# Create your views here.
@account_post_api(TestConnectorRequest)
def connectors_test_connection(request_message: TestConnectorRequest) -> Union[TestConnectorResponse, HttpResponse]:
    connector: Connector = request_message.connector
    if not connector:
        return TestConnectorResponse(success=BoolValue(value=False),
                                     message=Message(title='Connector details not found'))

    if not connector.name or not connector.name.value:
        return TestConnectorResponse(success=BoolValue(value=False),
                                     message=Message(title='Connector name not found'))

    loaded_connections = settings.LOADED_CONNECTIONS
    if not loaded_connections:
        return TestConnectorResponse(success=BoolValue(value=False),
                                     message=Message(title='No loaded connections found'))
    connector_name = connector.name.value
    if connector_name not in loaded_connections:
        return TestConnectorResponse(success=BoolValue(value=False),
                                     message=Message(
                                         title=f'No loaded connections found for connector: {connector_name}'))

    connector = credential_yaml_to_connector_proto(connector_name, loaded_connections[connector_name])

    all_keys_found = False
    all_ck_types = [ck.key_type for ck in connector.keys]
    required_key_types = integrations_connector_type_connector_keys_map.get(connector.type, [])
    for rkt in required_key_types:
        if sorted(rkt) == sorted(list(set(all_ck_types))):
            all_keys_found = True
            break
    if not all_keys_found:
        return TestConnectorResponse(success=BoolValue(value=False),
                                     message=Message(title='Missing Required Connector Keys',
                                                     description='Please provide all required keys'))
    connection_state, err = source_facade.test_source_connection(connector)
    if err is not None:
        return TestConnectorResponse(success=BoolValue(value=False), message=Message(title=err))
    if connection_state:
        return TestConnectorResponse(success=BoolValue(value=connection_state),
                                     message=Message(title='Source Connection Successful'))
    else:
        return TestConnectorResponse(success=BoolValue(value=connection_state),
                                     message=Message(title='Source Connection Failed'))
