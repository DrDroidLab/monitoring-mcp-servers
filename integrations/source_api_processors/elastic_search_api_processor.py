import logging

from elasticsearch import Elasticsearch

from integrations.processor import Processor

logger = logging.getLogger(__name__)


class ElasticSearchApiProcessor(Processor):
    client = None

    # Thresholds in seconds (descending order) mapped to ES interval strings
    _INTERVAL_THRESHOLDS = [
        (31536001, "1d"),   # > 1 year
        (2592001, "12h"),  # > 30 days
        (604801, "1h"),    # > 7 days
        (86401, "10m"),     # > 1 day
        (43201, "5m"),     # > 12 hours
        (21601, "5m"),    # > 6 hours
        (3601, "30s"),      # > 1 hour
        (1800, "30s"),      # >= 30 minutes
        (1200, "10s"),      # >= 20 minutes
    ]
    _DEFAULT_INTERVAL = "10s" # Interval for ranges < 20 minutes

    def __init__(self, protocol: str, host: str, port: str, api_key_id: str, api_key: str, verify_certs: bool = False):
        self.protocol = protocol
        self.host = host
        self.port = int(port) if port else 9200
        self.verify_certs = verify_certs
        self.__api_key_id = api_key_id
        self.__api_key = api_key

    def get_connection(self):
        try:
            client = Elasticsearch(
                [f"{self.protocol}://{self.host}:{self.port}"],
                api_key=(self.__api_key_id, self.__api_key),
                verify_certs=self.verify_certs
            )
            return client
        except Exception as e:
            logger.error(f"Exception occurred while creating elasticsearch connection with error: {e}")
            raise e

    def test_connection(self):
        try:
            connection = self.get_connection()
            indices = connection.indices.get_alias()
            connection.close()
            if len(list(indices.keys())) > 0:
                return True
            else:
                raise Exception("Elasticsearch Connection Error:: No indices found in elasticsearch")
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch indices with error: {e}")
            raise e

    def fetch_indices(self):
        try:
            connection = self.get_connection()
            indices = connection.indices.get_alias()
            connection.close()
            return list(indices.keys())
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch indices with error: {e}")
            raise e

    def query(self, index, query):
        try:
            connection = self.get_connection()
            result = connection.search(index=index, body=query, pretty=True)
            connection.close()
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch data with error: {e}")
            raise e

    def get_document(self, index, doc_id):
        try:
            connection = self.get_connection()
            result = connection.get(index=index, id=doc_id, pretty=True, preference="_primary_first")
            connection.close()
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch data with error: {e}")
            raise e
    def get_cluster_health(self):
        try:
            connection = self.get_connection()
            result = connection.cluster.health()
            connection.close()
            
            # Convert ObjectApiResponse to dict
            if hasattr(result, 'body'):
                # For Elasticsearch 8.x client
                return result.body
            elif hasattr(result, 'meta'):
                # Alternative approach for some client versions
                return dict(result)
            else:
                # Fallback for older client versions
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch cluster health with error: {e}")
            raise e
            
    def get_nodes_stats(self):
        try:
            connection = self.get_connection()
            result = connection.nodes.stats()
            connection.close()
            
            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch nodes stats with error: {e}")
            raise e

    def get_cat_indices(self):
        try:
            connection = self.get_connection()
            result = connection.cat.indices(v=True, format="json")
            connection.close()
            
            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch cat indices with error: {e}")
            raise e

    def get_cat_thread_pool_search(self):
        try:
            connection = self.get_connection()
            result = connection.cat.thread_pool(thread_pool_patterns="search", v=True, format="json")
            connection.close()
            
            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch cat thread pool search with error: {e}")
            raise e
        
    def _calculate_histogram_interval(self, start_time, end_time):
        """
        Calculate the appropriate histogram interval based on the time range.
        Uses standard intervals defined in _INTERVAL_THRESHOLDS.
        
        Args:
            start_time (int): Start time in epoch seconds
            end_time (int): End time in epoch seconds
            
        Returns:
            str: Elasticsearch interval string (e.g., "30s", "1h", "1d")
        """
        time_range_seconds = end_time - start_time
        
        for threshold, interval in self._INTERVAL_THRESHOLDS:
            if time_range_seconds >= threshold:
                return interval
                
        return self._DEFAULT_INTERVAL

    def fetch_monitoring_cluster_stats(self, start_time=None, end_time=None):
        client = self.get_connection()
        try:
            # Calculate appropriate interval based on time range
            interval = self._calculate_histogram_interval(start_time, end_time)
            
            result = client.search(
                index=".monitoring-es-*",
                body = {
                "size": 0,
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": start_time,
                            "lt": end_time,
                            "format": "epoch_second"
                        }
                    }
                },
                "aggs": {
                    "per_minute": {
                        "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": "30s"#interval
                        },
                        "aggs": {
                            "query_total": {
                                "max": {
                                    "field": "indices_stats._all.total.search.query_total"
                                }
                            },
                            "search_rate": {
                                "derivative": {
                                    "buckets_path": "query_total",
                                    "unit": "second",
                                }
                            },
                            "index_total": {
                                "max": {
                                    "field": "indices_stats._all.total.indexing.index_total"
                                }
                            },
                            "indexing_rate": {
                                "derivative": {
                                    "buckets_path": "index_total",
                                    "unit": "second"
                                }
                            },
                            "index_primary": {
                                "max": {
                                    "field": "indices_stats._all.primaries.indexing.index_total"
                                }
                            },
                            "indexing_rate_primary": {
                                "derivative": {
                                    "buckets_path": "index_primary",
                                    "unit": "second"
                                }
                            },
                            "query_total_sum": {
                                "sum": {
                                    "field": "indices_stats._all.total.search.query_total"
                                }
                            },
                            "query_time_sum": {
                                "sum": {
                                    "field": "indices_stats._all.total.search.query_time_in_millis"
                                }
                            },
                            "index_primary_sum": {
                                "sum": {
                                    "field": "indices_stats._all.primaries.indexing.index_total"
                                }
                                },
                            "index_time": {
                                "sum": {
                                "field": "elasticsearch.index.primaries.indexing.index_time_in_millis"
                                }
                            }
                        }
                    }
                }
            }
            )
            client.close()
            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Error fetching monitoring data: {e}")
            raise e