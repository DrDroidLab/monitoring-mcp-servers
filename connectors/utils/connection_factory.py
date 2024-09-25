from integrations.source_processors.aws_boto_3_api_processor import AWSBoto3ApiProcessor
from integrations.source_processors.eks_api_processor import EKSApiProcessor


class ConnectionFactory:

    @staticmethod
    def get_aws_cloudwatch_client(client_type: str, region: str, aws_access_key: str = None, aws_secret_key: str = None,
                                  aws_assumed_role_arn: str = None):
        return AWSBoto3ApiProcessor(client_type, region, aws_access_key, aws_secret_key, aws_assumed_role_arn)

    @staticmethod
    def get_eks_client(region: str, aws_access_key: str, aws_secret_key: str, k8_role_arn: str,
                       aws_assumed_role_arn: str = None):
        return EKSApiProcessor(region, aws_access_key, aws_secret_key, k8_role_arn, aws_assumed_role_arn)
