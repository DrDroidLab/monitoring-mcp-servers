from asset_manager.extractor.azure_metadata_extractor import AzureConnectorMetadataExtractor
from asset_manager.extractor.clickhouse_metadata_extractor import ClickhouseSourceMetadataExtractor
from asset_manager.extractor.cloudwatch_metadata_extractor import CloudwatchSourceMetadataExtractor
from asset_manager.extractor.datadog_metadata_extractor import DatadogSourceMetadataExtractor
from asset_manager.extractor.eks_metadata_extractor import EksSourceMetadataExtractor
from asset_manager.extractor.elastic_search_metadata_extractor import ElasticSearchSourceMetadataExtractor
from asset_manager.extractor.gke_metadata_extractor import GkeSourceMetadataExtractor
from asset_manager.extractor.grafana_metadata_extractor import GrafanaSourceMetadataExtractor
from asset_manager.extractor.grafana_vpc_metadata_extractor import GrafanaVpcSourceMetadataExtractor
from asset_manager.extractor.metadata_extractor import SourceMetadataExtractor
from asset_manager.extractor.newrelic_metadata_extractor import NewrelicSourceMetadataExtractor
from protos.base_pb2 import Source


class SourceMetadataExtractorFacade:

    def __init__(self):
        self._map = {}

    def register(self, source: Source, metadata_extractor: SourceMetadataExtractor.__class__):
        self._map[source] = metadata_extractor

    def get_connector_metadata_extractor_class(self, connector_type: Source):
        if connector_type not in self._map:
            raise ValueError(f'No metadata extractor found for connector type: {connector_type}')
        return self._map[connector_type]


source_metadata_extractor_facade = SourceMetadataExtractorFacade()
source_metadata_extractor_facade.register(Source.NEW_RELIC, NewrelicSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.DATADOG, DatadogSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.DATADOG_OAUTH, DatadogSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.CLOUDWATCH, CloudwatchSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.GRAFANA, GrafanaSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.GRAFANA_VPC, GrafanaVpcSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.CLICKHOUSE, ClickhouseSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.EKS, EksSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.AZURE, AzureConnectorMetadataExtractor)
source_metadata_extractor_facade.register(Source.GKE, GkeSourceMetadataExtractor)
source_metadata_extractor_facade.register(Source.ELASTIC_SEARCH, ElasticSearchSourceMetadataExtractor)
