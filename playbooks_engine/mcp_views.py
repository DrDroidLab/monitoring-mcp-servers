import logging
import json
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, JsonResponse
from typing import Optional
from django.conf import settings

from drdroid_debug_toolkit.core.integrations.source_facade import source_facade
from playbooks_engine.mcp_utils import generate_mcp_tools_for_source_manager, execute_mcp_tool, generate_mcp_tools_for_connectors, execute_mcp_tool_with_connector
from utils.decorators import mcp_api
from utils.credentilal_utils import credential_yaml_to_connector_proto

logger = logging.getLogger(__name__)

# Server info for MCP protocol
MCP_SERVER_INFO = {
    "name": "drdroid-mcp-server",
    "version": "1.0.0"
}

MCP_SERVER_CAPABILITIES = {
    "tools": {}
}

MCP_PROTOCOL_VERSION = "2025-06-18"

# Global cache for tool mappings to avoid regenerating on every call
_tool_mappings_cache = {}


def _get_all_tools_and_mappings():
    """Get all tools and mappings for all available connectors from secrets.yaml"""
    # Check cache first
    if 'global' in _tool_mappings_cache:
        return _tool_mappings_cache['global']

    # Use the new connector-based approach
    all_tools, tool_mappings = generate_mcp_tools_for_connectors()

    # Cache the results
    result = (all_tools, tool_mappings)
    _tool_mappings_cache['global'] = result
    return result


def _clear_tools_cache():
    """Clear the tools cache for a specific account or all accounts"""
    global _tool_mappings_cache
    _tool_mappings_cache.clear()


@csrf_exempt
# @mcp_api()
def mcp_endpoint(request):
    """Single MCP endpoint following JSON-RPC 2.0 protocol."""
    if request.method == "GET":
        # Return server info for GET requests
        return JsonResponse(MCP_SERVER_INFO)

    try:
        # Parse JSON request body
        request_data = json.loads(request.body.decode('utf-8'))
        logger.info(f"MCP request: {request_data}")

        method = request_data.get("method")
        params = request_data.get("params", {})
        request_id = request_data.get("id")

        # Handle notifications (no response expected)
        if method and method.startswith("notifications/"):
            logger.info(f"Received notification: {method}")
            return HttpResponse(status=204)  # No Content

        # Handle different MCP methods
        if method == "initialize":
            return _handle_initialize(params, request_id)
        elif method == "tools/list":
            return _handle_tools_list(params, request_id, request)
        elif method == "tools/call":
            return _handle_tools_call(params, request_id, request)
        elif method == "ping":
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {}
            })
        elif method == "debug/clear_cache":
            # Development endpoint to clear cache
            _clear_tools_cache()
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"message": f"Cache cleared"}
            })
        elif method == "debug/connectors":
            # Development endpoint to list connectors and their tools
            try:
                loaded_connections = settings.LOADED_CONNECTIONS
                connectors_info = {}
                
                for connector_name, connector_config in loaded_connections.items():
                    connectors_info[connector_name] = {
                        "type": connector_config.get("type", "unknown"),
                        "config": {k: "***" if "key" in k.lower() or "password" in k.lower() or "token" in k.lower() else v 
                                 for k, v in connector_config.items()}
                    }
                
                all_tools, tool_mappings = generate_mcp_tools_for_connectors()
                
                return JsonResponse({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "connectors": connectors_info,
                        "tools_count": len(all_tools),
                        "tools": [tool["name"] for tool in all_tools],
                        "tool_mappings": {k: {
                            "connector_name": v["connector_name"],
                            "source": str(v["source"]),
                            "task_type": str(v["task_type"]),
                            "task_type_name": v["task_type_name"]
                        } for k, v in tool_mappings.items()}
                    }
                })
            except Exception as e:
                logger.error(f"Error in debug/connectors: {e}")
                return JsonResponse({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32000,
                        "message": str(e)
                    }
                }, status=500)
        else:
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }, status=400)

    except json.JSONDecodeError:
        return JsonResponse({
            "jsonrpc": "2.0",
            "error": {
                "code": -32700,
                "message": "Parse error"
            }
        }, status=400)
    except Exception as e:
        logger.error(f"MCP endpoint error: {e}")
        return JsonResponse({
            "jsonrpc": "2.0",
            "id": request_data.get("id") if 'request_data' in locals() else None,
            "error": {
                "code": -32000,
                "message": str(e)
            }
        }, status=500)


def _handle_initialize(params, request_id):
    """Handle MCP initialize method"""
    protocol_version = params.get("protocolVersion")

    if protocol_version != MCP_PROTOCOL_VERSION:
        return JsonResponse({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32602,
                "message": f"Unsupported protocol version: {protocol_version}"
            }
        }, status=400)

    result = {
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "serverInfo": MCP_SERVER_INFO,
        "capabilities": MCP_SERVER_CAPABILITIES
    }

    return JsonResponse({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": result
    })


def _handle_tools_list(params, request_id, request):
    """Handle tools/list method"""
    try:
        logger.info(f"MCP tools/list request")

        # Get all tools and mappings for the account
        all_tools, tool_mappings = _get_all_tools_and_mappings()

        result = {"tools": all_tools}
        return JsonResponse({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result
        })

    except Exception as e:
        logger.error(f"Error in tools/list: {e}")
        return JsonResponse({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32000,
                "message": str(e)
            }
        }, status=500)


def _handle_tools_call(params, request_id, request):
    """Handle tools/call method"""
    try:
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if not tool_name:
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": "Missing tool name parameter"
                }
            }, status=400)

        logger.info(f"MCP tools/call request: {tool_name} with args {arguments}")

        # Get all tools and mappings for the account
        all_tools, tool_mappings = _get_all_tools_and_mappings()

        if tool_name not in tool_mappings:
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Tool not found: {tool_name}"
                }
            }, status=404)

        # Execute the tool using the new connector-based approach
        try:
            tool_result = execute_mcp_tool_with_connector(tool_name, arguments, tool_mappings)
            result = {
                "content": [
                    {
                        "type": "text",
                        "text": str(tool_result)
                    }
                ]
            }
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            })
        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return JsonResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": f"Error executing tool: {e}"
                }
            }, status=500)

    except Exception as e:
        logger.error(f"Error in tools/call: {e}")
        return JsonResponse({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32000,
                "message": str(e)
            }
        }, status=500) 