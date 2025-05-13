import pytz
import json
import logging
from datetime import datetime, timedelta, date
from typing import Any, Optional

from google.protobuf.wrappers_pb2 import StringValue, DoubleValue, UInt64Value, Int64Value
from google.protobuf.struct_pb2 import Struct

from integrations.source_api_processors.aws_boto_3_api_processor import AWSBoto3ApiProcessor
from protos.base_pb2 import TimeRange, Source, SourceModelType
from protos.connectors.connector_pb2 import Connector as ConnectorProto
from protos.literal_pb2 import LiteralType, Literal
from protos.playbooks.playbook_commons_pb2 import TimeseriesResult, LabelValuePair, PlaybookTaskResult, \
    PlaybookTaskResultType, TableResult, TextResult, ApiResponseResult
from protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
from protos.ui_definition_pb2 import FormField, FormFieldType
from integrations.source_manager import SourceManager
from utils.credentilal_utils import generate_credentials_dict
from utils.proto_utils import proto_to_dict, dict_to_proto

logger = logging.getLogger(__name__)


def convert_datetime_recursive(data):
    """Recursively convert all datetime objects in a dictionary or list to ISO format strings."""
    if isinstance(data, dict):
        for key, value in list(data.items()):
            if isinstance(value, (datetime, date)):
                data[key] = value.isoformat()
            elif isinstance(value, dict) or isinstance(value, list):
                data[key] = convert_datetime_recursive(value)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, (datetime, date)):
                data[i] = item.isoformat()
            elif isinstance(item, dict) or isinstance(item, list):
                data[i] = convert_datetime_recursive(item)
    return data

class CloudwatchSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.CLOUDWATCH
        self.task_proto = Cloudwatch
        self.task_type_callable_map = {
            Cloudwatch.TaskType.ECS_LIST_CLUSTERS: {
                'executor': self.ecs_list_clusters,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'List ECS Clusters',
                'category': 'ECS',
                'form_fields': [],
                'model_types': [SourceModelType.ECS_CLUSTER],
                'permission_checks': {
                    'test_ecs_list_clusters_permission': [{'client_type': 'ecs'}]
                }
            },
            Cloudwatch.TaskType.ECS_LIST_TASKS: {
                'executor': self.ecs_list_tasks,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'List ECS Tasks in Cluster',
                'category': 'ECS',
                'form_fields': [
                    FormField(key_name=StringValue(value="cluster_name"),
                              display_name=StringValue(value="Cluster Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT)
                ],
                'model_types': [SourceModelType.ECS_TASK],
                'permission_checks': {
                    'test_ecs_list_clusters_permission': [{'client_type': 'ecs'}]
                }
            },
            Cloudwatch.TaskType.ECS_GET_TASK_LOGS: {
                'executor': self.ecs_get_task_logs,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get ECS Task Logs',
                'category': 'ECS',
                'form_fields': [
                    FormField(key_name=StringValue(value="cluster_name"),
                              display_name=StringValue(value="Cluster Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="task_definition"),
                              display_name=StringValue(value="Task Definition"),
                              description=StringValue(value="Optional: Filter logs by task definition (e.g., Vidushee-Experiment:4)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="max_lines"),
                              display_name=StringValue(value="Max Lines"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=1000)),
                              form_field_type=FormFieldType.TEXT_FT)
                              ],
                'model_types': [SourceModelType.ECS_TASK],
                'permission_checks': {
                    'test_ecs_list_clusters_permission': [{'client_type': 'ecs'}],
                    'test_logs_describe_log_groups_permission': [{'client_type': 'logs'}]
                }
            },
            Cloudwatch.TaskType.METRIC_EXECUTION: {
                'executor': self.execute_metric_execution,
                'model_types': [SourceModelType.CLOUDWATCH_METRIC],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch a Metric from Cloudwatch',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="namespace"),
                              display_name=StringValue(value="Namespace"),
                              description=StringValue(value='Select Namespace'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="region"),
                              display_name=StringValue(value="Region"),
                              description=StringValue(value='Select Region'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="dimensions"),
                              display_name=StringValue(value="Dimensions"),
                              description=StringValue(value='Select Dimension Name'),
                              is_composite=True,
                              composite_fields=[
                                  FormField(key_name=StringValue(value="name"),
                                            display_name=StringValue(value="Dimension Name"),
                                            description=StringValue(value='Select Dimension Name'),
                                            data_type=LiteralType.STRING,
                                            form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                                  FormField(key_name=StringValue(value="value"),
                                            display_name=StringValue(value="Dimension Value"),
                                            description=StringValue(value='Select Dimension Value'),
                                            data_type=LiteralType.STRING,
                                            form_field_type=FormFieldType.TYPING_DROPDOWN_FT)
                              ],
                              form_field_type=FormFieldType.COMPOSITE_FT),
                    FormField(key_name=StringValue(value="metric_name"),
                              display_name=StringValue(value="Metric"),
                              description=StringValue(value='Add Metric'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_MULTIPLE_FT),
                    FormField(key_name=StringValue(value="statistic"),
                              display_name=StringValue(value="Metric Aggregation"),
                              description=StringValue(value='Select Aggregation Function'),
                              data_type=LiteralType.STRING,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="Average")),
                              valid_values=[Literal(type=LiteralType.STRING, string=StringValue(value="Average")),
                                            Literal(type=LiteralType.STRING, string=StringValue(value="Sum")),
                                            Literal(type=LiteralType.STRING, string=StringValue(value="SampleCount")),
                                            Literal(type=LiteralType.STRING, string=StringValue(value="Maximum")),
                                            Literal(type=LiteralType.STRING, string=StringValue(value="Minimum"))],
                              form_field_type=FormFieldType.DROPDOWN_FT),
                ]
            },
            Cloudwatch.TaskType.FILTER_LOG_EVENTS: {
                'executor': self.execute_filter_log_events,
                'model_types': [SourceModelType.CLOUDWATCH_LOG_GROUP],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Fetch Logs from Cloudwatch',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="region"),
                              display_name=StringValue(value="Region"),
                              description=StringValue(value='Select Region'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="log_group_name"),
                              display_name=StringValue(value="Log Group"),
                              description=StringValue(value='Select Log Group'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="filter_query"),
                              display_name=StringValue(value="Filter Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT)
                ]
            },
            Cloudwatch.TaskType.RDS_GET_SQL_QUERY_PERFORMANCE_STATS: {
                'executor': self.execute_rds_get_sql_query_performance_stats,
                'model_types': [],  # [SourceModelType.RDS_INSTANCES]
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch Sql Query performance stats from Cloudwatch',
                'category': 'Tables',
                'form_fields': [
                    FormField(key_name=StringValue(value="db_resource_uri"),
                              display_name=StringValue(value="Db Resource URI"),
                              description=StringValue(value='Enter DB Resource URI'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ],
                'permission_checks': {
                    'test_pi_describe_dimension_keys_permission': [{'client_type': 'pi'}]
                }
            },
            Cloudwatch.TaskType.FETCH_S3_FILE: {
                'executor': self.execute_fetch_s3_file,
                'model_types': [],
                'result_type': PlaybookTaskResultType.TEXT,
                'display_name': 'Fetch S3 File',
                'category': 'Files',
                'form_fields': [
                    FormField(key_name=StringValue(value="bucket_name"),
                              display_name=StringValue(value="Bucket Name"),
                              description=StringValue(value='Enter the S3 Bucket name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="object_key"),
                              display_name=StringValue(value="File Path"),
                              description=StringValue(value="Enter the File Path in S3 Bucket"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ],
                'permission_checks': {
                    'test_s3_get_object_permission': [{'client_type': 's3'}]
                }
            },
        }

    def get_connector_processor(self, cloudwatch_connector, **kwargs):
        generated_credentials = generate_credentials_dict(cloudwatch_connector.type, cloudwatch_connector.keys)
        generated_credentials['client_type'] = kwargs.get('client_type', 'cloudwatch')
        if 'region' in kwargs:
            generated_credentials['region'] = kwargs.get('region')
        return AWSBoto3ApiProcessor(**generated_credentials)

    def test_connector_processor(self, connector: ConnectorProto, **kwargs):
        cw_processor = self.get_connector_processor(connector, client_type='cloudwatch')
        cw_logs_processor = self.get_connector_processor(connector, client_type='logs')
        try:
            return cw_processor.test_connection() and cw_logs_processor.test_connection()
        except Exception as e:
            raise e

    def execute_metric_execution(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                                 cloudwatch_connector: ConnectorProto):
        try:
            if not cloudwatch_connector:
                raise Exception("Task execution Failed:: No Cloudwatch source found")

            period = 300
            task = cloudwatch_task.metric_execution
            region = task.region.value
            metric_name = task.metric_name.value
            namespace = task.namespace.value
            timeseries_offsets = task.timeseries_offsets
            statistic = ['Average']
            requested_statistic = 'Average'

            if task.statistic and task.statistic.value in ['Average', 'Sum', 'SampleCount', 'Maximum', 'Minimum']:
                statistic = [task.statistic.value]
                requested_statistic = task.statistic.value
            dimensions = [{'Name': td.name.value, 'Value': td.value.value} for td in task.dimensions]

            cloudwatch_boto3_processor = self.get_connector_processor(cloudwatch_connector, client_type='cloudwatch',
                                                                      region=region)

            labeled_metric_timeseries = []

            # Always get current time values
            start_time = datetime.utcfromtimestamp(time_range.time_geq)
            end_time = datetime.utcfromtimestamp(time_range.time_lt)

            current_response = cloudwatch_boto3_processor.cloudwatch_get_metric_statistics(
                namespace, metric_name, start_time, end_time, period, statistic, dimensions
            )
            if not current_response or not current_response['Datapoints']:
                raise Exception("No data returned from Cloudwatch for current time range")

            current_metric_datapoints = [
                TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                    timestamp=int(
                        datetime.fromisoformat(str(item['Timestamp'])).replace(tzinfo=pytz.UTC).timestamp() * 1000),
                    value=DoubleValue(value=item[requested_statistic])
                ) for item in current_response['Datapoints']
            ]

            metric_unit = current_response['Datapoints'][0]['Unit'] if current_response['Datapoints'] else ''

            labeled_metric_timeseries.append(
                TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=[
                        LabelValuePair(name=StringValue(value='namespace'), value=StringValue(value=namespace)),
                        LabelValuePair(name=StringValue(value='statistic'),
                                       value=StringValue(value=requested_statistic)),
                        LabelValuePair(name=StringValue(value='offset_seconds'),
                                       value=StringValue(value='0')),
                    ],
                    unit=StringValue(value=metric_unit),
                    datapoints=current_metric_datapoints
                )
            )

            # Get offset values if specified
            if timeseries_offsets:
                offsets = [offset for offset in timeseries_offsets]
                for offset in offsets:
                    # Use offset directly as seconds
                    adjusted_start_time = start_time - timedelta(seconds=offset)
                    adjusted_end_time = end_time - timedelta(seconds=offset)

                    response = cloudwatch_boto3_processor.cloudwatch_get_metric_statistics(
                        namespace, metric_name, adjusted_start_time, adjusted_end_time, period, statistic, dimensions
                    )
                    if not response or not response['Datapoints']:
                        print(f"No data returned from Cloudwatch for offset {offset} seconds")
                        continue

                    metric_datapoints = [
                        TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=int(datetime.fromisoformat(str(item['Timestamp'])).replace(
                                tzinfo=pytz.UTC).timestamp() * 1000),
                            value=DoubleValue(value=item[requested_statistic])
                        ) for item in response['Datapoints']
                    ]

                    labeled_metric_timeseries.append(
                        TimeseriesResult.LabeledMetricTimeseries(
                            metric_label_values=[
                                LabelValuePair(name=StringValue(value='namespace'), value=StringValue(value=namespace)),
                                LabelValuePair(name=StringValue(value='statistic'),
                                               value=StringValue(value=requested_statistic)),
                                LabelValuePair(name=StringValue(value='offset_seconds'),
                                               value=StringValue(value=str(offset))),
                            ],
                            unit=StringValue(value=metric_unit),
                            datapoints=metric_datapoints
                        )
                    )

            metric_metadata = f"{namespace} for region {region} "
            for i in dimensions:
                metric_metadata += f"{i['Name']}:{i['Value']},  "
            timeseries_result = TimeseriesResult(
                metric_expression=StringValue(value=metric_name),
                metric_name=StringValue(value=metric_metadata),
                labeled_metric_timeseries=labeled_metric_timeseries
            )

            task_result = PlaybookTaskResult(type=PlaybookTaskResultType.TIMESERIES, timeseries=timeseries_result,
                                             source=self.source)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Cloudwatch task: {e}")

    def execute_filter_log_events(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                                  cloudwatch_connector: ConnectorProto):
        try:
            if not cloudwatch_connector:
                raise Exception("Task execution Failed:: No Cloudwatch source found")
            tr_end_time = time_range.time_lt
            end_time = int(tr_end_time * 1000)
            tr_start_time = time_range.time_geq
            start_time = int(tr_start_time * 1000)

            task = cloudwatch_task.filter_log_events
            region = task.region.value
            log_group = task.log_group_name.value
            query_pattern = task.filter_query.value

            logs_boto3_processor = self.get_connector_processor(cloudwatch_connector, client_type='logs', region=region)
            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Region -> {}, Log_Group -> {}, Query -> "
                "{}, Start_Time -> {}, End_Time -> {}".format("Cloudwatch_Logs", cloudwatch_connector.account_id.value,
                                                              region, log_group, query_pattern, start_time, end_time),
                flush=True)

            response = logs_boto3_processor.logs_filter_events(log_group, query_pattern, start_time, end_time)
            if not response:
                raise Exception("No data returned from Cloudwatch Logs")

            table_rows: [TableResult.TableRow] = []
            for item in response:
                table_columns: [TableResult.TableColumn] = []
                for i in item:
                    table_column = TableResult.TableColumn(name=StringValue(value=i['field']),
                                                           value=StringValue(value=i['value']))
                    table_columns.append(table_column)
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)

            result = TableResult(
                raw_query=StringValue(
                    value=f"Execute ```{query_pattern}``` on log group {log_group} in region {region}"),
                rows=table_rows,
                total_count=UInt64Value(value=len(table_rows)),
            )

            task_result = PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=result, source=self.source)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Cloudwatch task: {e}")
        

    def execute_rds_get_sql_query_performance_stats(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                                                    cloudwatch_connector: ConnectorProto):
        try:
            if not cloudwatch_connector:
                raise Exception("Task execution Failed:: No Cloudwatch source found")
            tr_end_time = time_range.time_lt
            end_time = int(tr_end_time * 1000)
            tr_start_time = time_range.time_geq
            start_time = int(tr_start_time * 1000)

            task = cloudwatch_task.rds_get_sql_query_performance_stats
            db_resource_uri = task.db_resource_uri.value
            pi_boto3_processor = self.get_connector_processor(cloudwatch_connector, client_type='pi')
            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, DB -> {}, "
                "Start_Time -> {}, End_Time -> {}".format("RDS_Performance_Stats",
                                                          cloudwatch_connector.account_id.value, db_resource_uri,
                                                          start_time, end_time), flush=True)

            end_time_dt = datetime.utcfromtimestamp(tr_end_time)
            response = pi_boto3_processor.pi_get_long_running_queries(db_resource_uri, end_time_dt)
            if not response:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No Stats returned from RDS DB for: {db_resource_uri}")), source=self.source)

            table_rows: [TableResult.TableRow] = []
            for stat in response:
                table_columns: [TableResult.TableColumn] = []
                for k, v in stat.items():
                    table_column = TableResult.TableColumn(name=StringValue(value=str(k)),
                                                           value=StringValue(value=str(v)))
                    table_columns.append(table_column)
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)

            result = TableResult(
                raw_query=StringValue(value=f"SQL Query Performance Stats for DB {db_resource_uri}"),
                rows=table_rows,
                total_count=UInt64Value(value=len(table_rows)),
            )

            task_result = PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=result, source=self.source)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Cloudwatch task: {e}")
        
    ########################################## ECS Task Functions ####################################
    def ecs_list_clusters(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                          cloudwatch_connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(cloudwatch_connector, client_type='ecs')
            clusters = processor.list_all_clusters()
            
            if not clusters:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No ECS clusters found")),
                    source=self.source
                )
            
            # Convert all datetime objects in the clusters data to strings
            clusters = convert_datetime_recursive(clusters)
            
            response_struct = dict_to_proto({"clusters": clusters}, Struct)
            clusters_output = ApiResponseResult(response_body=response_struct)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=clusters_output
            )
        except Exception as e:
            raise Exception(f"Error while executing ECS list_clusters task: {e}")

    def ecs_list_tasks(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                        cloudwatch_connector: ConnectorProto):
        try:
            task = cloudwatch_task.ecs_list_tasks
            cluster_name = task.cluster_name.value
            
            processor = self.get_connector_processor(cloudwatch_connector, client_type='ecs')
            
            # First, get all clusters to find the ARN for the specified cluster name
            all_clusters = processor.list_all_clusters()
            # Convert all datetime objects in the clusters data to strings
            all_clusters = convert_datetime_recursive(all_clusters)
            cluster_arn = None
            
            for cluster in all_clusters:
                if cluster.get('clusterName') == cluster_name:
                    cluster_arn = cluster.get('clusterArn')
                    break
            
            if not cluster_arn:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"Cluster '{cluster_name}' not found")),
                    source=self.source
                )
            
            tasks = processor.list_tasks_in_cluster(cluster_arn)
            
            if not tasks:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No tasks found in cluster '{cluster_name}'")),
                    source=self.source
                )
            
            # Convert all datetime objects in the tasks data to strings
            tasks = convert_datetime_recursive(tasks)
            
            # Restructure the response to be a dictionary where each key is a task definition
            # and each value contains an array of tasks with that definition
            restructured_tasks = {}
            
            for task in tasks:
                # Extract the task definition from the ARN
                task_definition_arn = task.get('taskDefinitionArn', '')
                # Format: arn:aws:ecs:region:account:task-definition/name:revision
                task_definition = task_definition_arn.split('/')[-1] if '/' in task_definition_arn else 'unknown'
                
                # Add the extracted task definition as a field in the task object
                task['taskDefinition'] = task_definition
                
                # Add the task to the restructured dictionary
                if task_definition not in restructured_tasks:
                    restructured_tasks[task_definition] = []
                
                # Append this task to the array for this task definition
                restructured_tasks[task_definition].append(task)
            
            response_struct = dict_to_proto(restructured_tasks, Struct)
            tasks_output = ApiResponseResult(response_body=response_struct)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=tasks_output
            )
        except Exception as e:
            raise Exception(f"Error while executing ECS list_tasks task: {e}")
            
    def ecs_get_task_logs(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                          cloudwatch_connector: ConnectorProto):
        try:
            task = cloudwatch_task.ecs_get_task_logs
            cluster_name = task.cluster_name.value
            max_lines = task.max_lines.value
            
            # Check if a specific task definition filter was provided
            task_definition_filter = task.task_definition.value if task.HasField('task_definition') else None
            
            processor = self.get_connector_processor(cloudwatch_connector, client_type='ecs')
            
            # Get the logs for the task
            logs = processor.get_task_logs(cluster_name, max_lines)
            # Check if there was an error
            # if "error" in logs and len(logs) == 1:
            #     return PlaybookTaskResult(
            #         type=PlaybookTaskResultType.TEXT,
            #         text=TextResult(output=StringValue(value=logs["error"])),
            #         source=self.source
            #     )
            
            # Get task definitions map using the processor method
            task_def_map = processor.get_task_definitions_map(cluster_name)
            
            # Convert all datetime objects in the logs data to strings
            logs = convert_datetime_recursive(logs)
            
            # Restructure the logs by task definition with simplified structure
            restructured_logs = {}
            for task_id, task_data in logs.items():
                task_def = task_def_map.get(task_id, "unknown")
                
                # Skip if a task definition filter was provided and this task doesn't match
                if task_definition_filter and task_def != task_definition_filter:
                    continue
                
                # If this task definition isn't in the restructured logs yet, add it
                if task_def not in restructured_logs:
                    restructured_logs[task_def] = {}
                
                # For each container in the task
                for container_name, container_data in task_data.items():
                    # Extract just the logs and add them directly under the task definition
                    if "logs" in container_data:
                        # Initialize logs array if it doesn't exist yet
                        if "logs" not in restructured_logs[task_def]:
                            restructured_logs[task_def]["logs"] = []
                        
                        # Add these logs to the combined logs for this task definition
                        restructured_logs[task_def]["logs"].extend(container_data["logs"])
                    
                    # Add other metadata if not already present
                    if "logGroup" not in restructured_logs[task_def] and "logGroup" in container_data:
                        restructured_logs[task_def]["logGroup"] = container_data["logGroup"]
            
            # If a task definition filter was provided but no matching logs were found
            if task_definition_filter and not restructured_logs:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(
                        value=f"No logs found for task definition '{task_definition_filter}' in cluster '{cluster_name}'")),
                    source=self.source
                )
            
            response_struct = dict_to_proto({"logs": restructured_logs}, Struct)
            logs_output = ApiResponseResult(response_body=response_struct)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=logs_output
            )
        except Exception as e:
            raise Exception(f"Error while executing ECS get_task_logs task: {e}")
        
########################################## CW Fetch S3 File Functions ####################################
    def execute_fetch_s3_file(self, time_range: TimeRange, cloudwatch_task: Cloudwatch,
                              cloudwatch_connector: ConnectorProto):
        """Fetches the contents of an S3 file and returns them as a single TextResult."""
        try:
            if not cloudwatch_connector:
                raise Exception("Task execution Failed:: No Cloudwatch source found")

            task = cloudwatch_task.fetch_s3_file
            bucket_name = task.bucket_name.value
            object_key = task.object_key.value

            if not bucket_name or not object_key:
                raise ValueError("Bucket name and object key are required for FETCH_S3_FILE task")
            
            # Get the S3 file contents
            s3_processor = self.get_connector_processor(cloudwatch_connector, client_type='s3', region="us-west-2")
            if not s3_processor:
                raise Exception("Task execution Failed:: No S3 source found")
            
            # Fetch the file contents
            file_contents = s3_processor.download_file_contents_from_s3(bucket_name, object_key)

            if not file_contents:
                raise Exception("Task execution Failed:: No file contents found")
            
            return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(value=file_contents)))

        except Exception as e:
            logger.error(f"Error executing FETCH_S3_FILE for '{cloudwatch_task.fetch_s3_file.bucket_name.value}': {e}", exc_info=True)
            raise Exception(f"Error while fetching S3 file: {e}")