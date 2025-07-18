import json
import logging
import re
import string
import requests
from typing import TYPE_CHECKING, Optional, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import BoolValue, DoubleValue, StringValue

from alert_ops_engine.utils.string_utils import is_partial_match
from connectors.utils import generate_credentials_dict
from integrations.source_api_processors.grafana_api_processor import GrafanaApiProcessor
from integrations.source_asset_managers.asset_manager_facade import asset_manager_facade
from playbooks_engine.executor.playbook_source_manager import PlaybookSourceManager
from protos.event.assets.asset_pb2 import AccountConnectorAssets, AccountConnectorAssetsModelFilters
from protos.event.base_pb2 import TimeRange
from protos.event.connectors_pb2 import Connector as ConnectorProto
from protos.event.connectors_pb2 import ConnectorKey as SourceKeyType
from protos.event.connectors_pb2 import ConnectorMetadataModelType as SourceModelType
from protos.event.connectors_pb2 import ConnectorType as Source
from protos.event.literal_pb2 import Literal, LiteralType
from protos.event.playbooks.playbook_commons_pb2 import (
    ApiResponseResult,
    LabelValuePair,
    PlaybookTaskResult,
    PlaybookTaskResultType,
    TextResult,
    TimeseriesResult,
)
from protos.event.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
from protos.event.ui_definition_pb2 import FormField, FormFieldType
from utils.proto_utils import dict_to_proto, proto_to_dict
from utils.constants import *
from connectors.utils import get_connector_key_type_string

if TYPE_CHECKING:
    from protos.event.assets.grafana_asset_pb2 import GrafanaAssetModel, GrafanaDashboardAssetModel, \
        GrafanaDatasourceAssetModel

logger = logging.getLogger(__name__)


class GrafanaSourceManager(PlaybookSourceManager):
    # Constants for dynamic interval calculation
    MAX_DATA_POINTS = 70
    MIN_STEP_SIZE_SECONDS = 60  # Minimum interval is 1 minute

    # Duration thresholds (seconds) mapped to minimum bucket size (seconds)
    _INTERVAL_THRESHOLDS_SECONDS = [
        (2592001, 43200),  # > 30 days -> 12h minimum step size
        (604801, 21600),  # > 7 days -> 6h minimum step size
        (86401, 10800),  # > 1 day -> 40m minimum step size
        (43201, 3600),  # > 12 hours -> 20m minimum step size
        (21601, 1800),  # > 6 hours -> 10m minimum step size
        (3601, 120),  # > 1 hour -> 2m minimum step size
        (1801, 60),  # > 30 minutes -> 1m minimum step size
    ]

    # Standard bucket sizes (seconds) to round up to
    STANDARD_STEP_SIZES_SECONDS = [30, 60, 120, 300, 600, 900, 1800, 3600, 10800, 21600, 43200, 86400]

    def __init__(self):
        self.source = Source.GRAFANA
        self.task_proto = Grafana
        self.task_type_callable_map = {
            Grafana.TaskType.PROMETHEUS_DATASOURCE_METRIC_EXECUTION: {
                "executor": self.execute_prometheus_datasource_metric_execution,
                "model_types": [SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Query any of your Prometheus Data Sources from Grafana",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="datasource_uid"),
                        display_name=StringValue(value="Data Source UID"),
                        description=StringValue(value="Select Data Source UID "),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size(Seconds)"),
                        description=StringValue(value="(Optional)Enter Step Size"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="query_type"),
                        display_name=StringValue(value="Query Type"),
                        description=StringValue(value='Select Query Type'),
                        data_type=LiteralType.STRING,
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="PromQL")),
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="PromQL")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="Flux")),
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="promql_expression"),
                        display_name=StringValue(value="Query Expression"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                ],
            },
            Grafana.TaskType.QUERY_DASHBOARD_PANEL_METRIC: {
                "executor": self.execute_query_dashboard_panel_metric_execution,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Query any of your dashboard panels from Grafana",
                "category": "Metrics",
                "is_agent_enabled": False,
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_id"),
                        display_name=StringValue(value="Dashboard"),
                        description=StringValue(value="Select Dashboard"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="panel_id"),
                        display_name=StringValue(value="Panel"),
                        description=StringValue(value="Select Panel"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="datasource_uid"),
                        display_name=StringValue(value="Datasource UID"),
                        description=StringValue(value="Datasource UID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="queries"),
                        display_name=StringValue(value="Queries"),
                        description=StringValue(value="Queries"),
                        data_type=LiteralType.STRING_ARRAY,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="[]")),
                    ),
                ],
            },
            Grafana.TaskType.EXECUTE_ALL_DASHBOARD_PANELS: {
                "executor": self.execute_all_dashboard_panels,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.TIMESERIES,
                "display_name": "Execute queries for panels in a Grafana Dashboard",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_uid"),
                        display_name=StringValue(value="Dashboard UID"),
                        description=StringValue(value="Select Dashboard UID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="panel_ids"),
                        display_name=StringValue(value="Panel IDs (Optional, Comma-separated)"),
                        description=StringValue(
                            value="Enter comma-separated panel IDs to execute only specific panels. Leave blank to execute all."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size(Seconds)"),
                        description=StringValue(value="(Optional)Enter Step Size"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="template_variables"),
                        display_name=StringValue(value="Template Variables"),
                        description=StringValue(value="Template Variables"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True,
                    ),
                ],
            },
            Grafana.TaskType.FETCH_DASHBOARD_VARIABLE_LABEL_VALUES: {
                "executor": self.execute_fetch_dashboard_variable_label_values,
                "asset_descriptor": self.execute_prometheus_datasource_metric_asset_descriptor,
                "model_types": [SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch label values for dashboard variables from Prometheus Datasource",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="datasource_uid"),
                        display_name=StringValue(value="Data Source UID"),
                        description=StringValue(value="Select Prometheus Data Source UID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="label_name"),
                        display_name=StringValue(value="Label Name"),
                        description=StringValue(value="Enter the label name to fetch values for"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
        }

    def get_connector_processor(self, grafana_connector, **kwargs):
        generated_credentials = generate_credentials_dict(grafana_connector.type, grafana_connector.keys)
        return GrafanaApiProcessor(**generated_credentials)

    def execute_prometheus_datasource_metric_execution(self, time_range: TimeRange, grafana_task: Grafana,
                                                       grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.prometheus_datasource_metric_execution
            datasource_uid = task.datasource_uid.value
            interval = task.interval.value
            if interval is None:
                interval = 60
            interval_ms = interval * 1000
            query_type = task.query_type.value if task.query_type and task.query_type.value else "PromQL"
            metric_query = task.promql_expression.value

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            if query_type == 'Flux':
                queries = [
                    {"query": metric_query, "datasource": {"uid": datasource_uid}, "refId": "A",
                     'rawQuery': True}
                ]
            else:
                queries = [{"expr": metric_query, "datasource": {"uid": datasource_uid}, "refId": "A"}]

            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Datasource_Uid -> {datasource_uid}, "
                f"Promql_Metric_Query -> {metric_query}, Offset -> 0",
                flush=True,
            )

            formatted_queries = self._format_query_step_interval(queries, time_range)

            response = grafana_api_processor.panel_query_datasource_api(tr=time_range, queries=formatted_queries,
                                                                        interval_ms=interval_ms)

            if not response:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from Grafana for query: {queries}")),
                    source=self.source,
                )

            response_struct = dict_to_proto(response, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Grafana task: {e}") from e

    def execute_fetch_dashboard_variable_label_values(self, time_range: TimeRange, grafana_task: Grafana,
                                                      grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.fetch_dashboard_variable_label_values
            datasource_uid = task.datasource_uid.value
            label_name = task.label_name.value

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Datasource_Uid -> {datasource_uid}, "
                f"Label_Name -> {label_name}",
                flush=True,
            )

            label_values = grafana_api_processor.fetch_dashboard_variable_label_values(datasource_uid, label_name)

            if not label_values:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No label values returned from Grafana for label: {label_name}")),
                    source=self.source,
                )

            # Convert the list of label values to a structured response
            response_data = {
                "label_name": label_name,
                "datasource_uid": datasource_uid,
                "values": label_values,
                "count": len(label_values)
            }

            response_struct = dict_to_proto(response_data, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Grafana fetch dashboard variable label values task: {e}") from e

    def execute_query_dashboard_panel_metric_execution(self, time_range: TimeRange, grafana_task: Grafana,
                                                       grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.query_dashboard_panel_metric
            dashboard_id = task.dashboard_id.value
            panel_id = task.panel_id.value
            datasource_uid = task.datasource_uid.value
            letters = string.ascii_uppercase
            queries = [{**proto_to_dict(q), "datasource": {"uid": datasource_uid}, "refId": letters[idx]} for idx, q in
                       enumerate(task.queries)]

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Dashboard ID -> {dashboard_id}, Panel ID -> {panel_id}"
                f"Queries -> {queries}, Offset -> 0",
                flush=True,
            )

            formatted_queries = self._format_query_step_interval(queries, time_range)
            response = grafana_api_processor.panel_query_datasource_api(tr=time_range, queries=formatted_queries)

            if not response:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from Grafana for query: {queries}")),
                    source=self.source,
                )

            response_struct = dict_to_proto(response, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Grafana task: {e}") from e

    def _extract_template_variable_values(self, dashboard_dict: dict) -> dict:
        """Extracts current values from dashboard template variables."""
        template_vars_dict = {}
        dashboard_templating = dashboard_dict.get("templating", {})
        if isinstance(dashboard_templating, dict) and "list" in dashboard_templating:
            template_vars = dashboard_templating.get("list", [])
            for var in template_vars:
                if not isinstance(var, dict):
                    continue
                var_name = var.get("name", "")
                if not var_name:
                    continue
                current = var.get("current", {})
                if isinstance(current, dict):
                    template_vars_dict[var_name] = current.get("value")
        return template_vars_dict

    def _create_datasource_name_to_uid_mapping(self, grafana_api_processor) -> dict:
        """Creates a mapping from datasource names to UIDs by fetching from Grafana API."""
        try:
            datasources = grafana_api_processor.fetch_data_sources()
            if not datasources:
                logger.warning("No datasources found from Grafana API")
                return {}
            
            name_to_uid_map = {}
            for ds in datasources:
                if isinstance(ds, dict):
                    name = ds.get("name", "")
                    uid = ds.get("uid", "")
                    if name and uid:
                        name_to_uid_map[name] = uid
                        logger.debug(f"Mapped datasource '{name}' to UID '{uid}'")
            
            logger.info(f"Created datasource mapping for {len(name_to_uid_map)} datasources")
            return name_to_uid_map
        except Exception as e:
            logger.error(f"Failed to fetch datasources from Grafana API: {e}")
            return {}

    def _resolve_template_variables_in_string(self, input_string: str, template_vars_dict: dict) -> str:
        """Resolves template variables (e.g., $var or ${var}) in a string."""
        if not input_string or not isinstance(input_string, str):
            return input_string

        resolved_string = input_string
        # Regex to find $var or ${var} or ${var:format}
        var_refs = re.findall(r"\$\{([^}]+)(?::(?:csv|json|pipe|regex|distributed))?\}|\$([a-zA-Z0-9_]+)", input_string)
        for ref_tuple in var_refs:
            var_ref = next((item for item in ref_tuple if item), None)  # Get the captured group (variable name)
            if var_ref and var_ref in template_vars_dict:
                var_value = template_vars_dict[var_ref]
                # Basic handling for multi-value variables (join with '|')
                if isinstance(var_value, list):
                    replacement = "|".join(map(str, var_value)) if var_value else ""
                else:
                    replacement = str(var_value) if var_value is not None else ""
                # Replace both ${var} and $var formats, ensuring word boundaries for $var
                resolved_string = re.sub(r"\$\{" + re.escape(var_ref) + r"(?::(?:csv|json|pipe|regex|distributed))?\}",
                                         replacement, resolved_string)
                resolved_string = re.sub(r"\$" + re.escape(var_ref) + r"\b", replacement, resolved_string)
        return resolved_string

    def _resolve_target_datasource(
            self, target_datasource: Union[dict, str, None], dashboard_datasource: Union[dict, None],
            template_vars_dict: dict, panel_id, datasource_name_to_uid_map: dict
    ) -> Union[dict, None]:
        """Resolves the datasource for a target, handling variables and defaults."""
        resolved_ds = {}
        datasource_value_to_resolve = None

        if isinstance(target_datasource, dict) and target_datasource.get("uid"):
            datasource_value_to_resolve = target_datasource["uid"]
        elif isinstance(target_datasource, str):
            datasource_value_to_resolve = target_datasource
        elif target_datasource is None or (isinstance(target_datasource, dict) and not target_datasource.get("uid")):
            # Use dashboard default datasource if target is null/empty
            if dashboard_datasource and dashboard_datasource.get("uid"):
                datasource_value_to_resolve = dashboard_datasource["uid"]
            else:
                logger.warning(
                    f"Target in panel {panel_id} has no datasource and no dashboard default datasource found.")
                return None  # Cannot proceed without a datasource
        else:
            # Handle potentially unsupported datasources like -- Grafana -- or unknown formats
            logger.info(
                f"Skipping target in panel {panel_id} due to potentially unsupported datasource: {target_datasource}")
            return None

        if not datasource_value_to_resolve:
            logger.warning(
                f"Could not determine datasource value to resolve for panel {panel_id}. Target DS: {target_datasource}")
            return None

        # Resolve any variables in the datasource string (UID or name)
        resolved_uid_or_name = self._resolve_template_variables_in_string(datasource_value_to_resolve,
                                                                          template_vars_dict)

        if not resolved_uid_or_name:
            logger.warning(
                f"Datasource variable resolution resulted in empty value for panel {panel_id}. Original: {datasource_value_to_resolve}")
            return None

        # Check if the resolved value is a datasource name and convert to UID
        actual_uid = resolved_uid_or_name
        if resolved_uid_or_name in datasource_name_to_uid_map:
            actual_uid = datasource_name_to_uid_map[resolved_uid_or_name]
            logger.debug(f"Resolved datasource name '{resolved_uid_or_name}' to UID '{actual_uid}' for panel {panel_id}")
        
        # Grafana API /api/ds/query requires UID.
        resolved_ds = {"uid": str(actual_uid)}

        return resolved_ds

    def _prepare_grafana_queries(
            self, dashboard_dict: dict, template_vars_dict: dict, datasource_name_to_uid_map: dict, panel_ids_filter: Optional[list[str]] = None
    ) -> tuple[list[dict], dict]:
        """Builds the list of queries and the panel reference map from dashboard panels, optionally filtering by panel IDs."""
        all_queries = []
        panel_ref_map = {}
        panels = dashboard_dict.get("panels", [])
        dashboard_datasource = dashboard_dict.get("datasource")  # Default datasource for the dashboard
        letters = string.ascii_uppercase + string.digits
        query_idx = 0

        # Convert panel_ids_filter to a set for efficient lookup
        filter_set = set(panel_ids_filter) if panel_ids_filter else None

        for panel in panels:
            datasource_dict = panel.get("datasource") if "datasource" in panel else dashboard_datasource
            panel_id = panel.get("id")
            panel_title = panel.get("title", "")
            targets = panel.get("targets")

            if not targets or not panel_id:
                continue

            # Apply panel ID filter if provided
            if filter_set and str(panel_id) not in filter_set:
                continue

            for target in targets:
                if query_idx >= len(letters):
                    logger.warning(
                        f"Reached maximum query limit ({len(letters)}). Skipping remaining targets for dashboard.")
                    break

                raw_query = False
                expr = target.get("expr", "")
                if not expr:
                    expr = target.get("query", "")  # handling for raw flux query
                    if expr:
                        raw_query = True
                target_datasource_info = target.get("datasource")

                # Resolve Datasource
                resolved_datasource = self._resolve_target_datasource(target_datasource_info, datasource_dict,
                                                                      template_vars_dict, panel_id, datasource_name_to_uid_map)
                # Skip target if datasource resolution fails
                if not resolved_datasource and not expr.startswith(
                        "grafana"):  # Allow grafana expressions like grafana/alerting/list
                    continue

                # Resolve Expression Variables
                resolved_expr = self._resolve_template_variables_in_string(expr, template_vars_dict)
                if not resolved_expr:
                    continue  # Skip empty expressions

                # Build Query Object
                ref_id = letters[query_idx]
                if raw_query:
                    query_obj = {"query": resolved_expr, "refId": ref_id}
                else:
                    query_obj = {"expr": resolved_expr, "refId": ref_id}
                if resolved_datasource:
                    query_obj["datasource"] = resolved_datasource

                all_queries.append(query_obj)
                panel_ref_map[ref_id] = {"panel_id": panel_id, "panel_title": panel_title, "original_expr": expr}
                query_idx += 1

            if query_idx >= len(letters):
                break  # Break outer loop too if limit reached

        return all_queries, panel_ref_map

    def _parse_grafana_response_frames(self, response: dict, panel_ref_map: dict) -> list[PlaybookTaskResult]:
        """Parses the Grafana /api/ds/query response frames into a list of PlaybookTaskResults."""
        all_task_results = []
        if "results" not in response:
            return []

        for ref_id, result_data in response["results"].items():
            panel_info = panel_ref_map.get(ref_id)
            if not panel_info:
                logger.warning(f"Response for ref_id {ref_id} received but no mapping found. Skipping.")
                continue

            ref_id_timeseries = self._parse_frames_for_ref_id(result_data, panel_info, ref_id)

            # After processing all frames for a ref_id, create a TaskResult if data exists
            if ref_id_timeseries:
                panel_title = panel_info.get("panel_title", str(panel_info.get("panel_id")))
                legend_text = panel_title if panel_title else panel_info.get("original_expr", f"Query Ref: {ref_id}")

                timeseries_result = TimeseriesResult(
                    labeled_metric_timeseries=ref_id_timeseries,
                    metric_expression=StringValue(value=legend_text),
                    metric_name=StringValue(value=panel_title),
                )
                task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.TIMESERIES,
                                                 timeseries=timeseries_result)
                all_task_results.append(task_result)

        return all_task_results

    def _parse_frames_for_ref_id(self, result_data: dict, panel_info: dict, ref_id: str) -> list:
        """Parses all frames within a single result_data block for a given ref_id."""
        ref_id_timeseries = []
        if "frames" not in result_data:
            logger.warning(f"No 'frames' found in result for ref_id {ref_id}. Status: {result_data.get('status')}")
            return ref_id_timeseries

        for frame in result_data.get("frames", []):
            try:
                labeled_ts_list = self._parse_single_panel_frame(frame, panel_info, ref_id)
                ref_id_timeseries.extend(labeled_ts_list)
            except Exception as frame_ex:
                logger.error(
                    f"Error processing frame for ref_id {ref_id} in panel {panel_info.get('panel_id')}: {frame_ex}",
                    exc_info=True)
        return ref_id_timeseries

    def _parse_single_panel_frame(self, frame: dict, panel_info: dict, ref_id: str) -> list:
        """Parses a single Grafana data frame into LabeledMetricTimeseries."""
        parsed_timeseries = []
        schema = frame.get("schema", {})
        data = frame.get("data", {})
        if not schema or not data or not data.get("values"):
            logger.warning(f"Skipping incomplete frame for ref_id {ref_id}")
            return parsed_timeseries

        fields = schema.get("fields", [])
        if not fields:
            return parsed_timeseries

        time_idx, value_idx = -1, -1
        label_indices = []
        field_names = []

        for i, field in enumerate(fields):
            field_type = field.get("type")
            field_name = field.get("name")
            field_names.append(field_name)
            if field_type == "time":
                time_idx = i
            elif field_type == "number":
                if value_idx == -1:
                    value_idx = i
                else:
                    label_indices.append(i)
            else:
                label_indices.append(i)

        if time_idx == -1 or value_idx == -1:
            logger.warning(f"Could not find time or value field in frame for ref_id {ref_id}. Skipping frame.")
            return parsed_timeseries

        timestamps = data["values"][time_idx]
        values = data["values"][value_idx]
        num_points = len(timestamps)
        if num_points == 0:
            return parsed_timeseries

        # Frame-level/constant labels
        frame_labels = [
            LabelValuePair(name=StringValue(value="panel_id"), value=StringValue(value=str(panel_info["panel_id"]))),
            LabelValuePair(name=StringValue(value="panel_title"), value=StringValue(value=panel_info["panel_title"])),
            LabelValuePair(name=StringValue(value="ref_id"), value=StringValue(value=ref_id)),
            LabelValuePair(name=StringValue(value="original_expr"),
                           value=StringValue(value=panel_info["original_expr"])),
        ]
        if schema.get("name"):
            frame_labels.append(
                LabelValuePair(name=StringValue(value="series_name"), value=StringValue(value=schema["name"])))

        # Per-point labels
        label_data = {field_names[idx]: data["values"][idx] for idx in label_indices}
        label_field_names = [field_names[idx] for idx in label_indices]

        # Group points into series based on label values
        series_map = {}
        for i in range(num_points):
            point_label_values = tuple(label_data[name][i] for name in label_field_names)
            ts = timestamps[i]
            val = values[i]
            if val is None:
                continue

            if point_label_values not in series_map:
                series_map[point_label_values] = []
            series_map[point_label_values].append((ts, float(val)))

        # Create LabeledMetricTimeseries for each series
        for label_tuple, points in series_map.items():
            metric_labels = list(frame_labels)
            for j, label_value in enumerate(label_tuple):
                metric_labels.append(LabelValuePair(name=StringValue(value=label_field_names[j]),
                                                    value=StringValue(value=str(label_value))))

            datapoints = [
                TimeseriesResult.LabeledMetricTimeseries.Datapoint(timestamp=int(ts), value=DoubleValue(value=val)) for
                ts, val in sorted(points)
            ]

            if datapoints:
                labeled_ts = TimeseriesResult.LabeledMetricTimeseries(metric_label_values=metric_labels,
                                                                      datapoints=datapoints)
                parsed_timeseries.append(labeled_ts)

        return parsed_timeseries

    def execute_all_dashboard_panels(
            self, time_range: TimeRange, grafana_task: Grafana, grafana_connector: ConnectorProto
    ) -> list[PlaybookTaskResult]:
        """Executes all query targets for all panels in a given Grafana dashboard."""
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.execute_all_dashboard_panels
            dashboard_uid = task.dashboard_uid.value
            interval = task.interval.value
            if interval is None:
                interval = 60
            interval_ms = interval * 1000
            grafana_api_processor = self.get_connector_processor(grafana_connector)
            # Assuming panel_ids is a string field in the proto message (comma-separated)
            panel_ids_filter = None
            if task.HasField("panel_ids") and task.panel_ids.value:
                # Split the comma-separated string and remove any leading/trailing whitespace
                panel_ids_filter = [str(int(float(pid.strip()))) for pid in task.panel_ids.value.split(",") if
                                    pid.strip()]

            # 1. Fetch and parse dashboard details
            dashboard_details_response = grafana_api_processor.fetch_dashboard_details(dashboard_uid)
            if not dashboard_details_response or "dashboard" not in dashboard_details_response:
                raise Exception(f"Failed to fetch dashboard details for UID: {dashboard_uid}, {dashboard_details_response}")
            dashboard_dict = dashboard_details_response["dashboard"]

            # 2. Extract default template variable values and override with user-provided ones
            template_vars_dict = self._extract_template_variable_values(dashboard_dict)
            
            # Handle template_variables from the task
            override_template_vars = {}
            if task.HasField("template_variables") and task.template_variables.value:
                try:
                    user_vars = json.loads(task.template_variables.value)
                    if isinstance(user_vars, dict):
                        override_template_vars = user_vars
                    else:
                        logger.warning(f"Template variables from task is not a valid JSON object string: {task.template_variables.value}")
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode template_variables JSON: {task.template_variables.value}")
            
            if override_template_vars:
                template_vars_dict.update(override_template_vars)

            # 3. Create datasource name to UID mapping
            datasource_name_to_uid_map = self._create_datasource_name_to_uid_mapping(grafana_api_processor)

            # 4. Prepare queries for API call, passing the filter
            all_queries, panel_ref_map = self._prepare_grafana_queries(dashboard_dict, template_vars_dict,
                                                                       datasource_name_to_uid_map, panel_ids_filter)

            if not all_queries:
                filter_message = f"matching filter IDs: {panel_ids_filter}" if panel_ids_filter else ""
                logger.warning(
                    f"No valid queries could be prepared for dashboard UID: {dashboard_uid} {filter_message}")
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(
                        output=StringValue(
                            value=f"No valid metric queries could be found for dashboard UID: {dashboard_uid} {filter_message}")
                    ),
                )

            # 4. Execute API call
            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Dashboard UID -> {dashboard_uid}, "
                f"Executing {len(all_queries)} queries across all panels.",
                flush=True,
            )

            formatted_queries = self._format_query_step_interval(all_queries, time_range)
            response = grafana_api_processor.panel_query_datasource_api(tr=time_range, queries=formatted_queries,
                                                                        interval_ms=interval_ms)

            if not response:
                logger.warning(f"No data returned from Grafana API for dashboard UID: {dashboard_uid}")
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(
                        value=f"No data returned from Grafana API for dashboard UID: {dashboard_uid}")),
                )

            # 5. Parse API response into Timeseries results
            all_task_results = self._parse_grafana_response_frames(response, panel_ref_map)

            return all_task_results

        except requests.exceptions.HTTPError as e:
            error_message = f"Error executing Grafana task for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {e}"
            try:
                # Try to get a more specific error from Grafana's response
                error_details = e.response.json()
                if 'message' in error_details:
                    error_message = f"Grafana API error for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {error_details['message']}"
            except (ValueError, AttributeError):
                # Fallback to the default error if response is not JSON or other issue
                pass
            logger.error(error_message, exc_info=True)
            return [PlaybookTaskResult(
                source=self.source, type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_message))
            )]

        except Exception as e:
            logger.error(
                f"Error executing Grafana execute_all_dashboard_panels task for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {e}",
                exc_info=True,
            )
            # Return a text result indicating the error
            error_message = f"Error executing Grafana task for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {e}"
            error_result = PlaybookTaskResult(
                source=self.source, type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_message))
            )
            return [error_result]

    @staticmethod
    def query_dashboard_panel_metric_asset_descriptor(grafana_connector: ConnectorProto,
                                                      filters: Optional[dict] = None):
        try:
            assets: AccountConnectorAssets = asset_manager_facade.get_asset_model_values(
                grafana_connector, SourceModelType.GRAFANA_DASHBOARD, AccountConnectorAssetsModelFilters()
            )
            if not assets:
                logger.warning(
                    f"GrafanaSourceManager.query_dashboard_panel_metric_asset_descriptor:: No assets "
                    f"found for account: {grafana_connector.account_id.value}, connector: {grafana_connector.id.value}"
                )
                return ""
            assets = assets[0]
            grafana_assets: list[GrafanaAssetModel] = assets.grafana.assets
            all_dashboard_asset: list[GrafanaDashboardAssetModel] = [
                grafana_asset.grafana_dashboard for grafana_asset in grafana_assets if
                grafana_asset.type == SourceModelType.GRAFANA_DASHBOARD
            ]
            asset_list_string = ""
            filtered_list = ""
            for asset in all_dashboard_asset:
                dashboard_dict = proto_to_dict(asset.dashboard_json)
                dashboard_id = asset.dashboard_id.value
                dashboard_title = dashboard_dict.get("title", "")
                dashboard_uid = dashboard_dict.get("uid", "")

                # Start building the asset description for this dashboard
                asset_string = f"Dashboard Title: `{dashboard_title}`, Dashboard ID: `{dashboard_id}`, Dashboard UID: `{dashboard_uid}`\n"
                # Extract and format template variables section
                template_vars_section = ""
                template_vars_dict = {}  # Dictionary to store template variable values
                dashboard_templating = dashboard_dict.get("templating", {})
                if isinstance(dashboard_templating, dict) and "list" in dashboard_templating:
                    template_vars = dashboard_templating.get("list", [])
                    if template_vars:
                        template_vars_section = "Template Variables:\n"
                        for var in template_vars:
                            if not isinstance(var, dict):
                                continue

                            var_name = var.get("name", "")
                            if not var_name:
                                continue

                            var_type = var.get("type", "")
                            current = var.get("current", {})

                            if isinstance(current, dict):
                                current_value = current.get("value")

                                # Store the variable value in our dictionary
                                template_vars_dict[var_name] = current_value

                                # Format the current value for display in the asset descriptor
                                value_display = ""
                                if isinstance(current_value, list):
                                    value_display = ", ".join(str(v) for v in current_value)
                                else:
                                    value_display = str(current_value) if current_value is not None else "null"

                                template_vars_section += f"  - ${var_name}: {value_display} (type: {var_type})\n"

                                # Include reference format
                                template_vars_section += f"    Usage in queries: ${{{var_name}}}\n"

                        template_vars_section += "\n"

                if template_vars_section:
                    asset_string += template_vars_section

                # Process panels
                panels = dashboard_dict.get("panels", [])
                for panel in panels:
                    panel_id = panel.get("id") if "id" in panel else 0
                    panel_title = panel.get("title", "unnamed panel")
                    targets = panel.get("targets")
                    if targets:
                        query_exprs = []
                        data_sources = []

                        for target in targets:
                            query = target.get("query", "")
                            expr = target.get("expr", "")
                            datasource = target.get("datasource", {})

                            # Handle datasource format variations
                            datasource_info = {}
                            if isinstance(datasource, dict) and "uid" in datasource:
                                # Get the datasource UID and resolve any template variables
                                uid = datasource.get("uid", "")
                                # Resolve template variables in the UID if present
                                if uid and isinstance(uid, str) and "${" in uid and "}" in uid:
                                    # Extract variable name from ${varname} format
                                    var_name = uid.replace("${", "").replace("}", "")
                                    if var_name in template_vars_dict:
                                        uid = template_vars_dict[var_name]

                                datasource_info = {"uid": uid}
                            elif isinstance(datasource, str):
                                # For backward compatibility with older Grafana versions
                                datasource_name = datasource
                                if "${" in datasource_name and "}" in datasource_name:
                                    var_name = datasource_name.replace("${", "").replace("}", "")
                                    if var_name in template_vars_dict:
                                        datasource_name = template_vars_dict[var_name]
                                        if isinstance(datasource_name, list) and datasource_name:
                                            datasource_name = datasource_name[0]  # Take first value if it's a list

                                datasource_info = {"name": datasource_name}

                            # Also resolve template variables in expressions
                            if expr:
                                var_refs = re.findall(r"\$\{([^}]+)\}", expr)
                                resolved_expr = expr
                                for var_ref in var_refs:
                                    if var_ref in template_vars_dict:
                                        var_value = template_vars_dict[var_ref]
                                        if isinstance(var_value, list) and var_value:
                                            var_value = var_value[0]  # Take first value if it's a list
                                        resolved_expr = resolved_expr.replace(f"${{{var_ref}}}", str(var_value))
                                expr = resolved_expr

                                query_exprs.append(
                                    {
                                        "expr": expr,
                                    }
                                )

                            if query:
                                var_refs = re.findall(r"\$\{([^}]+)\}", query)
                                resolved_query = query
                                for var_ref in var_refs:
                                    if var_ref in template_vars_dict:
                                        var_value = template_vars_dict[var_ref]
                                        if isinstance(var_value, list) and var_value:
                                            var_value = var_value[0]  # Take first value if it's a list
                                        resolved_query = resolved_query.replace(f"${{{var_ref}}}", str(var_value))
                                query = resolved_query
                                query_exprs.append(
                                    {
                                        "query": query,
                                    }
                                )

                            data_sources.append(
                                {
                                    "datasource": datasource_info,
                                }
                            )

                        query_exprs_str = json.dumps(query_exprs, indent=2)
                        data_sources_str = json.dumps(data_sources, indent=2)
                    else:
                        query_exprs_str = ""
                        data_sources_str = ""

                    if query_exprs_str:
                        asset_string += f"Panel Title: `{panel_title}`, Panel ID: `{panel_id}`\n"
                        asset_string += f"  - Queries: {query_exprs_str}\n" if query_exprs_str else ""
                        asset_string += f"  - Query Data Sources: {data_sources_str}\n" if data_sources_str else ""
                        asset_string += "\n"

                asset_string += "\n"
                # Apply filters if specified
                if filters and "dashboard_title" in filters:
                    if filters["dashboard_title"].lower() in dashboard_title.lower():
                        filtered_list += asset_string
                elif filters and "dashboard_titles" in filters and is_partial_match(dashboard_title,
                                                                                    filters["dashboard_titles"]):
                    filtered_list += asset_string

                asset_list_string += asset_string

            result = filtered_list if filtered_list else asset_list_string
            return result
        except Exception as e:
            logger.error(
                f"GrafanaSourceManager.query_dashboard_panel_metric_asset_descriptor:: Error while "
                f"generating asset descriptor for account: {grafana_connector.account_id.value}, connector: "
                f"{grafana_connector.id.value} with error: {e}"
            )
        return ""

    @staticmethod
    def execute_prometheus_datasource_metric_asset_descriptor(grafana_connector: ConnectorProto,
                                                              filters: Optional[dict] = None):
        try:
            assets: AccountConnectorAssets = asset_manager_facade.get_asset_model_values(
                grafana_connector, SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE, AccountConnectorAssetsModelFilters()
            )
            if not assets:
                logger.warning(
                    f"GrafanaSourceManager.execute_prometheus_datasource_metric_asset_descriptor:: No assets "
                    f"found for account: {grafana_connector.account_id.value}, and "
                    f"Connector: {grafana_connector.id.value}"
                )
            assets = assets[0]
            grafana_assets: list[GrafanaAssetModel] = assets.datadog.assets
            all_datasource_asset: list[GrafanaDatasourceAssetModel] = [
                grafana_asset.grafana_prometheus_datasource
                for grafana_asset in grafana_assets
                if grafana_asset.type == SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE
            ]

            asset_list_string = ""
            filtered_list = ""
            for asset in all_datasource_asset:
                datasource_id = asset.datasource_id.value
                datasource_uid = asset.datasource_uid.value
                datasource_name = asset.datasource_name.value
                datasource_type = asset.datasource_type.value
                datasource_orgId = asset.datasource_orgId.value
                datasource_url = asset.datasource_url.value

                if (
                        (filters and "datasource_name" in filters and filters["datasource_name"] in datasource_name)
                        or (filters and "datasource_uid" in filters and filters["datasource_uid"] in datasource_uid)
                        or (filters and "datasource_id" in filters and filters["datasource_id"] in datasource_id)
                ):
                    filtered_list += f"Datasource Name: `{datasource_name}`, Datasource ID: `{datasource_id}`, Datasource UID: {datasource_uid}, Datasource Type: {datasource_type}, Dashboard URL: {datasource_url}, Datasource orgId: {datasource_orgId} \n\n "
                asset_list_string += f"Datasource Name: `{datasource_name}`, Datasource ID: `{datasource_id}`, Datasource UID: {datasource_uid}, Datasource Type: {datasource_type}, Dashboard URL: {datasource_url}, Datasource orgId: {datasource_orgId} \n\n "

            # Return the filtered list if available, otherwise return the full list
            return filtered_list if filtered_list else asset_list_string

        except Exception as e:
            logger.error(
                f"GrafanaSourceManager.execute_prometheus_datasource_metric_asset_descriptor:: Error while "
                f"generating asset descriptor for account: {grafana_connector.account_id.value} and connector: "
                f"{grafana_connector.id.value} with error: {e}"
            )
        return None

    def _calculate_bucket_size_seconds(self, total_seconds):
        """Calculate appropriate bucket size based on duration, aiming for < MAX_DATA_POINTS buckets."""
        if total_seconds <= 0:
            return self.MIN_STEP_SIZE_SECONDS

        ideal_bucket_size = (total_seconds + self.MAX_DATA_POINTS - 1) // self.MAX_DATA_POINTS

        min_bucket_for_duration = self.MIN_STEP_SIZE_SECONDS  # Start with the global minimum
        for duration_threshold, min_bucket_for_threshold in self._INTERVAL_THRESHOLDS_SECONDS:
            if total_seconds >= duration_threshold:
                min_bucket_for_duration = min_bucket_for_threshold
                break  # Found the largest applicable threshold, use its minimum

        # The actual bucket size must be at least the global minimum, the ideal size,
        # and the minimum required for the given duration.
        calculated_bucket_size = max(self.MIN_STEP_SIZE_SECONDS, ideal_bucket_size, min_bucket_for_duration)

        # Round up to the nearest standard bucket size
        for standard_size in self.STANDARD_STEP_SIZES_SECONDS:
            if calculated_bucket_size <= standard_size:
                return standard_size

        # If larger than the largest standard size, return the largest standard size
        return self.STANDARD_STEP_SIZES_SECONDS[-1]

    def _format_query_step_interval(self, queries, time_range: TimeRange):
        """
        Sets the maxDataPoints and calculates intervalMs for Grafana queries
        based on time range duration, thresholds, and standard bucket sizes,
        aiming for approximately MAX_DATA_POINTS data points.
        """
        # Calculate duration in seconds
        total_seconds = time_range.time_lt - time_range.time_geq
        if total_seconds <= 0:
            logger.warning(f"Invalid time range duration ({total_seconds}s), defaulting interval calculation.")
            total_seconds = self.MIN_STEP_SIZE_SECONDS

        interval_s = self._calculate_bucket_size_seconds(total_seconds)
        interval_ms = interval_s * 1000

        # Set maxDataPoints to our target number of buckets
        max_data_points = self.MAX_DATA_POINTS

        for q in queries:
            q["maxDataPoints"] = max_data_points
            q["intervalMs"] = interval_ms

        return queries
