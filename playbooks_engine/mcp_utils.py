import logging
from typing import Any, Dict, List, Tuple
from google.protobuf.struct_pb2 import Struct

from integrations.source_facade import source_facade
from protos.base_pb2 import TimeRange
from protos.playbooks.playbook_pb2 import PlaybookTask
from utils.proto_utils import dict_to_proto, proto_to_dict
from utils.time_utils import current_epoch_timestamp

logger = logging.getLogger(__name__)


def convert_literal_type_to_json_type(literal_type: Any) -> str:
    """Convert protobuf LiteralType to JSON Schema type string."""
    try:
        # Use enum values directly
        if literal_type == 1:  # STRING
            return "string"
        elif literal_type == 2:  # LONG
            return "integer"
        elif literal_type == 3:  # DOUBLE
            return "number"
        elif literal_type == 4:  # BOOLEAN
            return "boolean"
        elif literal_type == 5:  # STRING_ARRAY
            return "array"
        else:
            return "string"
    except:
        return "string"


def convert_form_field_to_json_schema(field: Any) -> Dict[str, Any]:
    """Convert a FormField proto to JSON Schema property definition."""
    try:
        field_schema = {
            "type": convert_literal_type_to_json_type(field.data_type),
            "description": field.description.value if hasattr(field, 'description') and field.description else
                          field.display_name.value if hasattr(field, 'display_name') and field.display_name else ""
        }

        # Handle array type
        if field.data_type == 5:  # STRING_ARRAY
            field_schema["items"] = {"type": "string"}

        # Add default value if present
        try:
            if hasattr(field, 'HasField') and field.HasField("default_value"):
                default_val = field.default_value
                if default_val.literal_type == 1:  # STRING
                    field_schema["default"] = default_val.string.value
                elif default_val.literal_type == 2:  # LONG
                    field_schema["default"] = default_val.long.value
                elif default_val.literal_type == 4:  # BOOLEAN
                    field_schema["default"] = default_val.boolean.value
        except:
            pass

        return field_schema
    except Exception as e:
        logger.error(f"Error converting form field: {e}")
        return {"type": "string", "description": ""}


def generate_mcp_tools_for_source_manager(source_manager: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Generate MCP tools and tool-to-task-type mapping for a source manager."""
    tools = []
    tool_to_task_mapping = {}

    try:
        # Check if we have any active connectors for this source
        connectors = source_manager.get_all_active_connectors()
        if not connectors:
            logger.info(f"No active connectors found for source {source_manager.source}")
            return tools, tool_to_task_mapping
    except Exception as e:
        logger.error(f"Error getting connectors for source {getattr(source_manager, 'source', 'unknown')}: {e}")
        return tools, tool_to_task_mapping

    try:
        # Get source name for tool naming - use actual source name instead of proto ID
        source_name = str(source_manager.source).lower()
        # Convert proto enum to readable source name
        source_mapping = {
            'cloudwatch': 'cloudwatch',
            'datadog': 'datadog',
            'datadog_oauth': 'datadog',
            'new_relic': 'newrelic',
            'grafana': 'grafana',
            'grafana_mimir': 'mimir',
            'azure': 'azure',
            'gke': 'gke',
            'gcm': 'gcm',
            'grafana_loki': 'loki',
            'postgres': 'postgres',
            'clickhouse': 'clickhouse',
            'sql_database_connection': 'sql',
            'elastic_search': 'elasticsearch',
            'big_query': 'bigquery',
            'mongodb': 'mongodb',
            'open_search': 'opensearch',
            'api': 'api',
            'bash': 'bash',
            'kubernetes': 'k8s',
            'smtp': 'smtp',
            'slack': 'slack',
            'documentation': 'docs',
            'rootly': 'rootly',
            'zenduty': 'zenduty',
            'github': 'github',
            'argocd': 'argocd',
            'jira_cloud': 'jira',
            'jenkins': 'jenkins',
            'posthog': 'posthog',
            'signoz': 'signoz',
            'sentry': 'sentry',
            'github_actions': 'github_actions',
            'eks': 'eks'
        }

        # Use mapping or fallback to cleaned source name
        source_name = source_mapping.get(source_name, source_name.replace('_', '').replace('.', ''))

        for task_type, task_info in source_manager.task_type_callable_map.items():
            try:
                # Create shorter task type name
                task_type_name = str(task_type).lower()
                if hasattr(source_manager, 'task_proto') and source_manager.task_proto:
                    try:
                        task_type_name = source_manager.task_proto.TaskType.Name(task_type).lower()
                        # Remove common prefixes to make it shorter
                        if task_type_name.startswith('task_type_'):
                            task_type_name = task_type_name[10:]
                        elif task_type_name.startswith('task_'):
                            task_type_name = task_type_name[5:]
                    except:
                        pass

                # Create shorter tool name: {source}_{task}
                tool_name = f"{source_name}_{task_type_name}"

                # Ensure tool name doesn't exceed 60 characters
                if len(tool_name) > 60:
                    # Truncate to 60 characters, keeping the most important parts
                    if len(source_name) > 30:
                        source_name = source_name[:30]
                    if len(task_type_name) > 30:
                        task_type_name = task_type_name[:30]
                    tool_name = f"{source_name}_{task_type_name}"
                    # Final safety check
                    if len(tool_name) > 60:
                        tool_name = tool_name[:60]

                # Convert form_fields to JSON Schema properties
                properties = {}
                required = []

                for field in task_info.get('form_fields', []):
                    try:
                        field_name = field.key_name.value if hasattr(field, 'key_name') else f"field_{len(properties)}"
                        field_schema = convert_form_field_to_json_schema(field)
                        properties[field_name] = field_schema

                        if not getattr(field, 'is_optional', True):
                            required.append(field_name)
                    except Exception as e:
                        logger.error(f"Error processing form field: {e}")
                        continue

                # Create MCP tool definition
                tool = {
                    "name": tool_name,
                    "description": task_info.get('display_name', f'{source_name.title()} task'),
                    "inputSchema": {
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }
                }

                tools.append(tool)
                tool_to_task_mapping[tool_name] = {
                    "source": source_manager.source,
                    "task_type": task_type,
                    "task_type_name": task_type_name
                }

            except Exception as e:
                logger.error(f"Error creating tool for task type {task_type}: {e}")
                continue
    except Exception as e:
        logger.error(f"Error generating tools for source manager: {e}")

    return tools, tool_to_task_mapping


def build_playbook_task_from_mcp_args(source: Any, task_type: Any, task_type_name: str, arguments: Dict[str, Any], connector_id: int) -> Any:
    """Build a PlaybookTask proto from MCP arguments."""
    try:
        # Import ConnectorType to get the proper name
        from protos.connectors.connector_pb2 import Source as ConnectorType

        # Get source name for task structure - handle enum properly
        source_name = "unknown"
        try:
            # Use ConnectorType.Name to get the proper string name
            source_name = ConnectorType.Name(source).lower()
        except:
            # Fallback to string conversion
            try:
                source_name = str(source).lower()
                if 'source.' in source_name:
                    source_name = source_name.split('.')[-1]
                elif '.' in source_name:
                    source_name = source_name.split('.')[-1]
            except:
                raise ValueError(f"Could not determine source name for source: {source}")

        print(f"Building task with source_name: {source_name}, task_type: {task_type}, task_type_name: {task_type_name}")

        # Build the task structure
        task_dict = {
            "source": source,
            "task_connector_sources": [{
                "id": connector_id,
                "source": source,
            }]
        }

        # Add task-specific fields
        task_field = task_type_name.lower()
        task_dict[source_name] = {
            "type": task_type
        }
        task_dict[source_name][task_field] = arguments

        print(f"Task dict structure: {task_dict}")

        # Convert to PlaybookTask proto
        task_proto = dict_to_proto(task_dict, PlaybookTask)
        return task_proto
    except Exception as e:
        logger.error(f"Error building playbook task: {e}")
        raise


def execute_mcp_tool(tool_name: str, arguments: Dict[str, Any], tool_mapping: Dict[str, Any]) -> Dict[str, Any]:
    """Execute an MCP tool using the existing playbook facade."""
    try:
        if tool_name not in tool_mapping:
            raise ValueError(f"Tool {tool_name} not found")

        tool_info = tool_mapping[tool_name]
        source = tool_info["source"]
        task_type = tool_info["task_type"]
        task_type_name = tool_info["task_type_name"]

        # Get first available connector for this source
        source_manager = source_facade.get_source_manager(source)
        if not source_manager:
            raise ValueError(f"No source manager found for source {source}")
        connectors = source_manager.get_all_active_connectors()

        if not connectors:
            raise ValueError(f"No active connectors found for source {source}")

        connector = connectors[0]  # Use first available

        # Build PlaybookTask proto
        task = build_playbook_task_from_mcp_args(
            source=source,
            task_type=task_type,
            task_type_name=task_type_name,
            arguments=arguments,
            connector_id=connector.id.value
        )

        # Create time range (last 4 hours like in views)
        current_time = current_epoch_timestamp()
        time_range = TimeRange(time_geq=int(current_time - 14400), time_lt=int(current_time))

        # Execute using existing facade
        result = source_facade.execute_task(
            time_range=time_range,
            global_variable_set=Struct(),
            task=task,
        )

        # Convert result to dict for JSON serialization
        try:
            # Check if result is a list/iterable (multiple results)
            if isinstance(result, list):
                return {"results": [proto_to_dict(r) for r in result]}
            else:
                # Single result
                return {"result": proto_to_dict(result)}
        except Exception as e:
            logger.error(f"Error converting result to dict: {e}")
            return {"result": {"error": str(e)}}

    except Exception as e:
        logger.error(f"Error executing MCP tool {tool_name}: {e}")
        return {"error": str(e)} 