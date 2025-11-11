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

