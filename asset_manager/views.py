import logging
import uuid
from typing import Union

from django.conf import settings
from django.http import HttpResponse
from google.protobuf.wrappers_pb2 import BoolValue, StringValue

from asset_manager.tasks import populate_connector_metadata
from protos.assets.api_pb2 import FetchAssetRequest, FetchAssetResponse
from protos.base_pb2 import Message, Source
from utils.credentilal_utils import credential_yaml_to_connector_proto, generate_credentials_dict
from utils.decorators import account_post_api
from utils.static_mappings import integrations_connector_type_connector_keys_map

logger = logging.getLogger(__name__)

loaded_connections = settings.LOADED_CONNECTIONS


@account_post_api(FetchAssetRequest)
def assets_models_fetch(request_message: FetchAssetRequest) -> \
        Union[FetchAssetResponse, HttpResponse]:
    if not request_message.connector.name or not request_message.connector.name.value:
        return FetchAssetResponse(success=BoolValue(value=False), message=Message(title="Invalid Request",
                                                                                  description="Missing connector name"))
    connector_name = request_message.connector.name.value
    if not loaded_connections:
        return FetchAssetResponse(success=BoolValue(value=False), message=Message(title="Invalid Request",
                                                                                  description="No loaded connections found"))
    if connector_name not in loaded_connections:
        return FetchAssetResponse(success=BoolValue(value=False), message=Message(title="Invalid Request",
                                                                                  description=f"Connector {connector_name} not found in loaded connections"))
    connector_proto = credential_yaml_to_connector_proto(connector_name, loaded_connections[connector_name])
    connector_keys_proto = connector_proto.keys
    all_ck_types = [ck.key_type for ck in connector_keys_proto]
    required_key_types = integrations_connector_type_connector_keys_map.get(connector_proto.type, [])
    all_keys_found = False
    for rkt in required_key_types:
        if sorted(rkt) == sorted(all_ck_types):
            all_keys_found = True
            break
    if not all_keys_found:
        return FetchAssetResponse(success=BoolValue(value=False),
                                  message=Message(title="Invalid Request",
                                                  description="Missing required connector keys"))
    connector_type: Source = connector_proto.type
    credentials_dict = generate_credentials_dict(connector_type, connector_keys_proto)
    if credentials_dict:
        request_id = uuid.uuid4().hex
        populate_connector_metadata.delay(request_id, connector_name, connector_type, credentials_dict)
        return FetchAssetResponse(success=BoolValue(value=True), request_id=StringValue(value=request_id))
    else:
        return FetchAssetResponse(success=BoolValue(value=False), message=Message(title="Invalid Request",
                                                                                  description="Invalid credentials"))
