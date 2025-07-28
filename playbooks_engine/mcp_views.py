import logging
import json
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, JsonResponse
from typing import Optional
from django.conf import settings

from integrations.source_facade import source_facade
from playbooks_engine.mcp_utils import generate_mcp_tools_for_source_manager, execute_mcp_tool
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
    """Get all tools and mappings for all available source managers that have credentials defined"""
    # Check cache first
    if 'global' in _tool_mappings_cache:
        return _tool_mappings_cache['global']

    all_tools = []
    tool_mappings = {}

    # Get loaded connections from settings
    loaded_connections = settings.LOADED_CONNECTIONS
    if not loaded_connections:
        logger.info("No credentials found in secrets.yaml, no tools will be generated")
        result = (all_tools, tool_mappings)
        _tool_mappings_cache['global'] = result
        return result

    # Get unique source types from loaded connections
    available_source_types = set()
    for connector_name, connector_config in loaded_connections.items():
        if 'type' in connector_config:
            source_type = connector_config['type']
            available_source_types.add(source_type)
            logger.info(f"Found credentials for source type: {source_type}")

    # Create mapping from proto enum values to string names
    from protos.base_pb2 import Source
    source_enum_to_string = {
        Source.UNKNOWN: "UNKNOWN",
        Source.SENTRY: "SENTRY",
        Source.SEGMENT: "SEGMENT",
        Source.ELASTIC_SEARCH: "ELASTIC_SEARCH",
        Source.AMPLITUDE: "AMPLITUDE",
        Source.AWS_KINESIS: "AWS_KINESIS",
        Source.CLOUDWATCH: "CLOUDWATCH",
        Source.CLEVERTAP: "CLEVERTAP",
        Source.RUDDERSTACK: "RUDDERSTACK",
        Source.MOENGAGE: "MOENGAGE",
        Source.CRIBL: "CRIBL",
        Source.KAFKA: "KAFKA",
        Source.DATADOG: "DATADOG",
        Source.FILEBEAT: "FILEBEAT",
        Source.LOGSTASH: "LOGSTASH",
        Source.FLUENTD: "FLUENTD",
        Source.FLUENTBIT: "FLUENTBIT",
        Source.PAGER_DUTY: "PAGER_DUTY",
        Source.NEW_RELIC: "NEW_RELIC",
        Source.SLACK: "SLACK",
        Source.HONEYBADGER: "HONEYBADGER",
        Source.GOOGLE_CHAT: "GOOGLE_CHAT",
        Source.DATADOG_OAUTH: "DATADOG_OAUTH",
        Source.GCM: "GCM",
        Source.PROMETHEUS: "PROMETHEUS",
        Source.ELASTIC_APM: "ELASTIC_APM",
        Source.VICTORIA_METRICS: "VICTORIA_METRICS",
        Source.SLACK_CONNECT: "SLACK_CONNECT",
        Source.GRAFANA: "GRAFANA",
        Source.CLICKHOUSE: "CLICKHOUSE",
        Source.DOCUMENTATION: "DOCUMENTATION",
        Source.POSTGRES: "POSTGRES",
        Source.OPS_GENIE: "OPS_GENIE",
        Source.EKS: "EKS",
        Source.AGENT_PROXY: "AGENT_PROXY",
        Source.GRAFANA_VPC: "GRAFANA_VPC",
        Source.GITHUB: "GITHUB",
        Source.SQL_DATABASE_CONNECTION: "SQL_DATABASE_CONNECTION",
        Source.OPEN_AI: "OPEN_AI",
        Source.REMOTE_SERVER: "REMOTE_SERVER",
        Source.API: "API",
        Source.BASH: "BASH",
        Source.AZURE: "AZURE",
        Source.GRAFANA_MIMIR: "GRAFANA_MIMIR",
        Source.GKE: "GKE",
        Source.MS_TEAMS: "MS_TEAMS",
        Source.GRAFANA_LOKI: "GRAFANA_LOKI",
        Source.KUBERNETES: "KUBERNETES",
        Source.SMTP: "SMTP",
        Source.BIG_QUERY: "BIG_QUERY",
        Source.ZENDUTY: "ZENDUTY",
        Source.ROOTLY: "ROOTLY",
        Source.JIRA_CLOUD: "JIRA_CLOUD",
        Source.ASANA: "ASANA",
        Source.CONFLUENCE_CLOUD: "CONFLUENCE_CLOUD",
        Source.CONFLUENCE_SELF_HOSTED: "CONFLUENCE_SELF_HOSTED",
        Source.GOOGLE_DRIVE: "GOOGLE_DRIVE",
        Source.NOTION: "NOTION",
        Source.MONGODB: "MONGODB",
        Source.OPEN_SEARCH: "OPEN_SEARCH",
        Source.LINEAR: "LINEAR",
        Source.JENKINS: "JENKINS",
        Source.GITHUB_ACTIONS: "GITHUB_ACTIONS",
        Source.CUSTOM_STRATEGIES: "CUSTOM_STRATEGIES",
        Source.ARGOCD: "ARGOCD",
        Source.DRD_ALERT_WEBHOOK: "DRD_ALERT_WEBHOOK",
        Source.ROLLBAR: "ROLLBAR",
        Source.LAMBDA: "LAMBDA",
        Source.POSTHOG: "POSTHOG",
        Source.SIGNOZ: "SIGNOZ",
    }

    # Get all available connector integrations from the facade
    all_sources = source_facade.get_all_available_connector_integrations()

    # Only process sources that have credentials defined
    for source in all_sources:
        try:
            # Convert source enum to string name for comparison
            source_str = source_enum_to_string.get(source, str(source))
            
            # Check if this source has credentials defined
            if source_str not in available_source_types:
                logger.info(f"Skipping source {source_str} - no credentials found")
                continue
                
            source_manager = source_facade.get_source_manager(source)
            if source_manager:
                tools, mapping = generate_mcp_tools_for_source_manager(source_manager)
                all_tools.extend(tools)
                tool_mappings.update(mapping)
                logger.info(f"Generated {len(tools)} tools for source {source_str}")
        except Exception as e:
            logger.error(f"Error generating tools for source {source}: {e}")
            continue

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

        # Execute the tool
        try:
            tool_result = execute_mcp_tool(tool_name, arguments, tool_mappings)
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