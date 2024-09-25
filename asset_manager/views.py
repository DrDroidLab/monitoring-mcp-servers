import logging
from typing import Union

from django.http import HttpResponse
from django.shortcuts import render
from google.protobuf.wrappers_pb2 import BoolValue

from protos.base_pb2 import Message
from protos.connectors.assets.api_pb2 import FetchAssetRequest, FetchAssetResponse
from utils.decorators import account_post_api

logger = logging.getLogger(__name__)


# Create your views here.
@account_post_api(FetchAssetRequest)
def assets_models_refresh(request_message: FetchAssetRequest) -> \
        Union[FetchAssetResponse, HttpResponse]:
    if not request_message.connector_id or not request_message.connector_id.value:
        return FetchAssetResponse(success=BoolValue(value=False), message=Message(title="Invalid Request",
                                                                                  description="Missing connector details"))
    connector_id = request_message.connector_id.value
    db_connectors = get_db_account_connectors(account, connector_id)
    if not db_connectors.exists() or not db_connectors:
        return GetConnectorsAssetsModelsRefreshResponse(success=BoolValue(value=False),
                                                        message=Message(title="Invalid Request",
                                                                        description="Connector not found"))
    db_connector = db_connectors.first()
    connector_proto: ConnectorProto = db_connector.unmasked_proto
    connector_keys_proto = connector_proto.keys
    all_ck_types = [ck.key_type for ck in connector_keys_proto]
    required_key_types = integrations_connector_type_connector_keys_map.get(connector_proto.type, [])
    all_keys_found = False
    for rkt in required_key_types:
        if sorted(rkt) == sorted(all_ck_types):
            all_keys_found = True
            break
    if not all_keys_found:
        return GetConnectorsAssetsModelsRefreshResponse(success=BoolValue(value=False),
                                                        message=Message(title="Invalid Request",
                                                                        description="Missing required connector keys"))

    trigger_connector_metadata_fetch(account, connector_proto, connector_keys_proto)
    return GetConnectorsAssetsModelsRefreshResponse(success=BoolValue(value=True))
