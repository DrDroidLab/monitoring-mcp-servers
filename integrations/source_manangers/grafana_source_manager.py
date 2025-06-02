import logging
import re
import string
from typing import Optional, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue

from integrations.source_api_processors.grafana_api_processor import GrafanaApiProcessor
from integrations.source_manager import SourceManager
from protos.base_pb2 import Source, SourceModelType, TimeRange
from protos.connectors.connector_pb2 import Connector as ConnectorProto
from protos.literal_pb2 import Literal, LiteralType
from protos.playbooks.playbook_commons_pb2 import (
    ApiResponseResult,
    LabelValuePair,
    PlaybookTaskResult,
    PlaybookTaskResultType,
    TextResult,
    TimeseriesResult,
)
from protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
from protos.ui_definition_pb2 import FormField, FormFieldType
from utils.credentilal_utils import generate_credentials_dict
from utils.proto_utils import dict_to_proto, proto_to_dict

logger = logging.getLogger(__name__)


class GrafanaSourceManager(SourceManager):
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
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size(Seconds)"),
                        description=StringValue(value="(Optional)Enter Step Size"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
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
                    {"query": metric_query, "datasource": {"uid": datasource_uid}, "refId": "A", 'rawQuery': True}
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
            raise Exception(f"Error while executing Grafana task: {e}")

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
            raise Exception(f"Error while executing Grafana task: {e}")

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
            template_vars_dict: dict, panel_id
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

        # Grafana API /api/ds/query generally prefers UID.
        resolved_ds = {"uid": str(resolved_uid_or_name)}

        return resolved_ds

    def _prepare_grafana_queries(self, dashboard_dict: dict, template_vars_dict: dict,
                                 panel_ids_filter: Optional[list[str]] = None) -> tuple[list[dict], dict]:
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

                expr = target.get("expr", "")
                target_datasource_info = target.get("datasource")

                # Resolve Datasource
                resolved_datasource = self._resolve_target_datasource(target_datasource_info, datasource_dict,
                                                                      template_vars_dict, panel_id)
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
                raise Exception(f"Failed to fetch dashboard details for UID: {dashboard_uid}")
            dashboard_dict = dashboard_details_response["dashboard"]

            # 2. Extract template variable values
            template_vars_dict = self._extract_template_variable_values(dashboard_dict)

            # 3. Prepare queries for API call, passing the filter
            all_queries, panel_ref_map = self._prepare_grafana_queries(dashboard_dict, template_vars_dict,
                                                                       panel_ids_filter)

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
