import logging
import os
from datetime import timedelta

from azure.identity import DefaultAzureCredential
from azure.mgmt.loganalytics import LogAnalyticsManagementClient
from azure.monitor.query import LogsQueryClient, MetricsQueryClient
from azure.mgmt.resource import ResourceManagementClient

from integrations.processor import Processor

logger = logging.getLogger(__name__)


def query_response_to_dict(response):
    """
    Function to convert query response tables to dictionaries.
    """
    result = {}
    for table in response.tables:
        result[table.name] = []
        for row in table.rows:
            row_dict = {}
            for i, column in enumerate(table.columns):
                row_dict[column] = str(row[i])
            result[table.name].append(row_dict)
    return result


class AzureApiProcessor(Processor):
    client = None

    def __init__(self, subscription_id, tenant_id, client_id, client_secret):
        self.__subscription_id = subscription_id
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__tenant_id = tenant_id

        if not self.__client_id or not self.__client_secret or not self.__tenant_id:
            raise Exception("Azure Connection Error:: Missing client_id, client_secret, or tenant_id")

    def get_credentials(self):
        """
        Function to fetch Azure credentials using client_id, client_secret, and tenant_id.
        """
        os.environ['AZURE_CLIENT_ID'] = self.__client_id
        os.environ['AZURE_CLIENT_SECRET'] = self.__client_secret
        os.environ['AZURE_TENANT_ID'] = self.__tenant_id
        credential = DefaultAzureCredential()
        return credential

    def test_connection(self):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                raise Exception("Azure Connection Error:: Failed to get credentials")
            log_analytics_client = LogAnalyticsManagementClient(credentials, self.__subscription_id)
            workspaces = log_analytics_client.workspaces.list()
            if not workspaces:
                raise Exception("Azure Connection Error:: No Workspaces Found")
            az_workspaces = []
            for workspace in workspaces:
                az_workspaces.append(workspace.as_dict())
            if len(az_workspaces) > 0:
                return True
            else:
                raise Exception("Azure Connection Error:: No Workspaces Found")
        except Exception as e:
            raise e

    def fetch_workspaces(self):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            log_analytics_client = LogAnalyticsManagementClient(credentials, self.__subscription_id)
            workspaces = log_analytics_client.workspaces.list()
            if not workspaces:
                logger.error("Azure Connection Error:: No Workspaces Found")
                return None
            return [workspace.as_dict() for workspace in workspaces]
        except Exception as e:
            logger.error(f"Failed to fetch workspaces with error: {e}")
            return None

    def fetch_resources(self):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            resource_client = ResourceManagementClient(credentials, self.__subscription_id)
            metrics_client = MetricsQueryClient(credentials)
            resources = resource_client.resources.list()
            if not resources:
                logger.error("Azure Connection Error:: No Resources Found")
                return None

            # Only include resources that actually produce metrics
            metric_producing_types = [
                "Microsoft.Compute/virtualMachines",
                "Microsoft.ContainerService/managedClusters",
                "Microsoft.Network/loadBalancers",
                "Microsoft.Network/applicationGateways",
                "Microsoft.Storage/storageAccounts",
                "Microsoft.Sql/servers/databases",
                "Microsoft.Web/sites",
                "Microsoft.Insights/components"
            ]
            valid_resources = []
            for resource in resources:
                if resource.type in metric_producing_types:
                    resource_dict = resource.as_dict()
                    resource_id = resource.id
                    try:
                        metric_names = metrics_client.list_metric_definitions(resource_id)
                        metric_names = [metric.name for metric in metric_names]
                    except Exception as e:
                        logger.error(f"Failed to fetch azure  metrics for resource {resource_id} with error: {e}")
                        metric_names = []
                    resource_dict["available_metrics"] = {"metric_names": metric_names}
                    valid_resources.append(resource_dict)
            return valid_resources
        except Exception as e:
            logger.error(f"Failed to fetch azure resources with error: {e}")
            return None

    def query_log_analytics(self, workspace_id, query, timespan=timedelta(hours=4)):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            client = LogsQueryClient(credentials)
            response = client.query_workspace(workspace_id, query, timespan=timespan)
            if not response:
                logger.error(f"Failed to query log analytics with error: {response.text}")
                return None
            results = query_response_to_dict(response)
            return results
        except Exception as e:
            logger.error(f"Failed to query log analytics with error: {e}")
            return None
