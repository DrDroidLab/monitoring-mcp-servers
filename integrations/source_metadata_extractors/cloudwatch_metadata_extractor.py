import logging

from integrations.source_metadata_extractor import SourceMetadataExtractor
from integrations.source_api_processors.aws_boto_3_api_processor import AWSBoto3ApiProcessor
from protos.base_pb2 import Source, SourceModelType
from utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class CloudwatchSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, region, aws_access_key=None, aws_secret_key=None,
                 aws_assumed_role_arn=None):
        self.__region = region
        self.__aws_access_key = aws_access_key
        self.__aws_secret_key = aws_secret_key
        self.__aws_assumed_role_arn = aws_assumed_role_arn

        super().__init__(request_id, connector_name, Source.CLOUDWATCH)

    @log_function_call
    def extract_metric(self):
        model_type = SourceModelType.CLOUDWATCH_METRIC
        model_data = {}
        try:
            cloudwatch_boto3_processor = AWSBoto3ApiProcessor('cloudwatch', self.__region, self.__aws_access_key,
                                                              self.__aws_secret_key, self.__aws_assumed_role_arn)
            all_metrics = cloudwatch_boto3_processor.cloudwatch_list_metrics()
            for metric in all_metrics:
                namespace = f"{metric['Namespace']}"
                namespace_map = model_data.get(namespace, {})
                region_map = namespace_map.get(self.__region, {})
                metric_map = region_map.get(metric['MetricName'], {})
                dimension_map = metric_map.get('Dimensions', {})
                for dimension in metric['Dimensions']:
                    dimension_values = dimension_map.get(dimension['Name'], [])
                    dimension_values.append(dimension['Value'])
                    dimension_map[dimension['Name']] = list(set(dimension_values))
                region_map[metric['MetricName']] = {'Dimensions': dimension_map,
                                                    'DimensionNames': list(dimension_map.keys())}
                namespace_map[self.__region] = region_map
                model_data[namespace] = namespace_map
                if len(model_data) >= 10:
                    self.create_or_update_model_metadata(model_type, model_data)
                    model_data = {}
        except Exception as e:
            logger.error(f'Error extracting metrics: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_log_groups(self):
        model_type = SourceModelType.CLOUDWATCH_LOG_GROUP
        model_data = {}
        cloudwatch_boto3_processor = AWSBoto3ApiProcessor('logs', self.__region, self.__aws_access_key,
                                                          self.__aws_secret_key, self.__aws_session_token)
        try:
            all_log_groups = cloudwatch_boto3_processor.logs_describe_log_groups()
            model_data[self.__region] = {'log_groups': all_log_groups}
            if len(model_data) >= 10:
                self.create_or_update_model_metadata(model_type, model_data)
                model_data = {}
        except Exception as e:
            logger.error(f'Error extracting log groups: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
