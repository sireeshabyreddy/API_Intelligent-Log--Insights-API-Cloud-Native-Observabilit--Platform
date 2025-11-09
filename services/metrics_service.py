import os
from azure.cosmos import CosmosClient
import logging
from config import *
DB_NAME = "MetricsDB"
CONTAINER_NAME = "Metrics"


metrics_container = None

if COSMOS_CONNECTION_STRING:
    try:
        cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
        db = cosmos_client.get_database_client(DB_NAME)
        metrics_container = db.get_container_client(CONTAINER_NAME)
        logging.info("Connected to Metrics Cosmos DB successfully.")
    except Exception as e:
        logging.error(f"Metrics Cosmos DB initialization failed: {str(e)}")
else:
    logging.warning("COSMOS_CONNECTION_STRING not set. Metrics endpoints will not work.")


# ----------------- QUERY FUNCTIONS -----------------
def get_metrics_by_service(service_name: str, top: int = 50):
    if not metrics_container:
        return []
    query = f"SELECT * FROM c WHERE c.service = '{service_name}' ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items[:top]

def get_metrics_summary(top: int = 50):
    if not metrics_container:
        return []
    query = f"SELECT * FROM c ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items[:top]

def get_metrics_anomalies(top: int = 50):
    if not metrics_container:
        return []
    query = "SELECT * FROM c WHERE c.error_rate_percent > 5 ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items[:top]
def get_avg_cpu_by_service(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT VALUE AVG(c.cpu_sum) FROM c WHERE c.service = '{service_name}'"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items[0] if items else 0


def get_avg_memory_by_service(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT VALUE AVG(c.memory_sum / c.total_logs) FROM c WHERE c.service = '{service_name}'"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items[0] if items else 0

def get_max_latency_by_service(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT VALUE MAX(c.max_latency) FROM c WHERE c.service = '{service_name}'"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items[0] if items else 0

def get_error_rate_by_service(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT c.error_rate_percent FROM c WHERE c.service = '{service_name}' ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    return items

def get_top_users_by_requests(service_name: str, top: int = 5):
    if not metrics_container:
        return []
    
    query = f"SELECT c.user_requests FROM c WHERE c.service = '{service_name}'"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    
    # Aggregate all user requests
    aggregate_requests = {}
    for item in items:
        for user_id, count in item.get("user_requests", {}).items():
            aggregate_requests[user_id] = aggregate_requests.get(user_id, 0) + count
    
    # Sort by request count descending
    sorted_users = sorted(aggregate_requests.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_users[:top]

def get_component_distribution(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT c.component_count FROM c WHERE c.service = '{service_name}' ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    if items:
        return items[0].get("component_count", {})
    return {}

def get_namespace_distribution(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT c.namespace_count FROM c WHERE c.service = '{service_name}' ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    if items:
        return items[0].get("namespace_count", {})
    return {}

def get_semantic_anomalies(service_name: str):
    if not metrics_container:
        return []
    query = f"SELECT c.semantic_anomalies FROM c WHERE c.service = '{service_name}' ORDER BY c.id DESC"
    items = list(metrics_container.query_items(query=query, enable_cross_partition_query=True))
    if items:
        return items[0].get("semantic_anomalies", 0)
    return 0
