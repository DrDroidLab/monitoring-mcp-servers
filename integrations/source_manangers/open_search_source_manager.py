import json

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from integrations.source_api_processors.open_search_api_processor import OpenSearchApiProcessor
from integrations.source_manager import SourceManager
from protos.base_pb2 import Source, SourceModelType, TimeRange
from protos.connectors.connector_pb2 import Connector as ConnectorProto
from protos.literal_pb2 import LiteralType, Literal
from protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType, TableResult, PlaybookTaskResult
from protos.playbooks.source_task_definitions.open_search_task_pb2 import OpenSearch
from protos.ui_definition_pb2 import FormField, FormFieldType
from utils.credentilal_utils import generate_credentials_dict


class OpenSearchSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.OPEN_SEARCH
        self.task_proto = OpenSearch
        self.task_type_callable_map = {
            OpenSearch.TaskType.QUERY_LOGS: {
                'executor': self.execute_query_logs,
                'model_types': [SourceModelType.OPEN_SEARCH_INDEX],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Query Logs from an OpenSearch Index',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="index"),
                              display_name=StringValue(value="Index"),
                              description=StringValue(value='Select Index'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="query_dsl"),
                              display_name=StringValue(value="Query DSL"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Enter Limit"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=2000)),
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="offset"),
                              display_name=StringValue(value="Enter Offset"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=0)),
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
        }

    def get_connector_processor(self, os_connector, **kwargs):
        generated_credentials = generate_credentials_dict(os_connector.type, os_connector.keys)
        return OpenSearchApiProcessor(**generated_credentials)

    def execute_query_logs(self, time_range: TimeRange, os_task: OpenSearch,
                           os_connector: ConnectorProto) -> PlaybookTaskResult:
        try:
            if not os_connector:
                raise ValueError("Task execution Failed:: No OpenSearch source found")

            query_logs = os_task.query_logs

            index = query_logs.index.value
            query_dsl = query_logs.query_dsl.value.strip()
            limit = query_logs.limit.value if query_logs.limit.value else 2000
            offset = query_logs.offset.value if query_logs.offset.value else 0
            sort_desc = query_logs.sort_desc.value if query_logs.sort_desc.value else ""
            timestamp_field = query_logs.timestamp_field.value if query_logs.timestamp_field.value else ""

            if not index:
                raise ValueError("Task execution Failed:: No index found")

            os_client = self.get_connector_processor(os_connector)

            sort = []
            if timestamp_field:
                sort.append({timestamp_field: "desc"})
            if sort_desc:
                sort.append({sort_desc: "desc"})
            sort.append({"_score": "desc"})

            parsed_query_dsl = json.loads(query_dsl)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            parsed_query_dsl["query"]
                        ]
                    }
                },
                "size": limit,
                "from": offset,
                "sort": sort
            }

            if timestamp_field:
                query["query"]["bool"]["must"].append({
                    "range": {
                        timestamp_field: {
                            "gte": time_range.time_geq * 1000,
                            "lt": time_range.time_lt * 1000
                        }
                    }
                })

            result = os_client.query(index, query)

            if 'hits' not in result or not result['hits']['hits']:
                print(f"No hits found. Raw result: {json.dumps(result, indent=2)}", flush=True)
                raise ValueError(f"No data found for the query: {query_dsl}")

            hits = result['hits']['hits']
            count_result = len(hits)
            total_hits = result['hits']['total']['value']
            table_rows = []
            for hit in hits:
                table_columns = []
                for column, value in hit['_source'].items():
                    table_column = TableResult.TableColumn(name=StringValue(value=column),
                                                           value=StringValue(value=str(value)))
                    table_columns.append(table_column)
                table_rows.append(TableResult.TableRow(columns=table_columns))

            table = TableResult(raw_query=StringValue(value=f"Execute ```{query_dsl}``` on index {index}"),
                                total_count=UInt64Value(value=total_hits),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=table, source=self.source)

        except Exception as e:
            raise Exception(f"Error while executing OpenSearch task: {str(e)}")
