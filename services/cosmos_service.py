# services/cosmos_service.py
from azure.cosmos import CosmosClient, PartitionKey

import config  # your config.py with Key Vault secrets
import logging

# --------------------------
# Cosmos DB Configuration
# --------------------------
COSMOS_DB_NAME = "IntelligentLogsDB"
COSMOS_CONTAINER_NAME = "ProcessedLogs"

# Fetch credentials from firm_config (Key Vault)
try:
    COSMOS_URL = config.secret_client.get_secret("CosmosDB-URI").value
    COSMOS_KEY = config.secret_client.get_secret("CosmosDB-PrimaryKey").value

    client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)
    db = client.get_database_client(COSMOS_DB_NAME)
    container = db.get_container_client(COSMOS_CONTAINER_NAME)
    logging.info(" Cosmos DB connection initialized successfully.")
except Exception as e:
    logging.error(f" Failed to initialize Cosmos DB: {e}")
    container = None

# --------------------------
# Query Functions
# --------------------------
def query_logs_by_service(service_name: str, top: int = 50):
    """Fetch logs for a specific service."""
    if not container:
        return []
    query = f"SELECT * FROM c WHERE c.service = '{service_name}'"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    return items[:top]

def query_logs_by_level(log_level: str, top: int = 50):
    """Fetch logs filtered by level (error/warn/info)."""
    if not container:
        return []
    query = f"SELECT * FROM c WHERE LOWER(c.level) = '{log_level.lower()}'"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    return items[:top]

def query_anomalous_logs(top: int = 50):
    """Fetch logs marked as anomalies."""
    if not container:
        return []
    query = "SELECT * FROM c WHERE c.anomaly_flag = true"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    return items[:top]
