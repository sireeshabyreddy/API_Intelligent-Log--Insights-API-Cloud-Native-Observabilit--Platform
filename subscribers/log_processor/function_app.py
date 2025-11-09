import azure.functions as func
import json
import logging
import os
import uuid
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# ===============================
# CONFIGURATION
# ===============================
KEYVAULT_NAME = os.getenv("KEYVAULT_NAME", "sireeshakv2025")
VAULT_URL = f"https://{KEYVAULT_NAME}.vault.azure.net/"

COSMOS_DB_NAME = "IntelligentLogsDB"
COSMOS_CONTAINER_NAME = "ProcessedLogs"

# ===============================
# GLOBAL VARIABLES
# ===============================
COSMOS_URL = os.getenv("COSMOS_URL")
COSMOS_KEY = os.getenv("COSMOS_KEY")
container = None

# ===============================
# HELPER FUNCTIONS
# ===============================
def fetch_cosmos_secrets(retries=3):
    """Fetch CosmosDB secrets from Key Vault with retry."""
    global COSMOS_URL, COSMOS_KEY
    for attempt in range(retries):
        try:
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
            COSMOS_URL = secret_client.get_secret("CosmosDB-URI").value
            COSMOS_KEY = secret_client.get_secret("CosmosDB-PrimaryKey").value
            logging.info(" Retrieved CosmosDB secrets from Key Vault.")
            return True
        except Exception as e:
            logging.error(f"Attempt {attempt+1}: Failed to fetch secrets - {e}")
    return False


def get_cosmos_container():
    """Initialize or return existing Cosmos container."""
    global container
    if container:
        return container

    if not COSMOS_URL or not COSMOS_KEY:
        if not fetch_cosmos_secrets():
            logging.error("Cannot initialize Cosmos container: secrets unavailable.")
            return None

    try:
        client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)
        database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
        container = database.create_container_if_not_exists(
            id=COSMOS_CONTAINER_NAME,
            partition_key=PartitionKey(path="/service"),
            offer_throughput=400
        )
        logging.info(" Cosmos container initialized successfully.")
        return container
    except Exception as e:
        logging.error(f"Failed to initialize Cosmos DB: {e}")
        return None


def enrich_json_log(log):
    """Handle structured logs (JsonLogsDataTable)."""
    enriched = log.copy()
    enriched["processed_timestamp"] = datetime.utcnow().isoformat()

    # Ensure partition key exists
    if "service" not in enriched or not enriched["service"]:
        enriched["service"] = "unknown_service"

    # Add unique ID if not present
    if "id" not in enriched:
        enriched["id"] = str(uuid.uuid4())

    level = enriched.get("Level", "").lower()
    if "error" in level:
        enriched["severity_score"] = 10
    elif "warn" in level:
        enriched["severity_score"] = 6
    else:
        enriched["severity_score"] = 3

    response_time = enriched.get("response_ms", 0)
    enriched["anomaly_flag"] = response_time > 2000 if response_time else False
    enriched["source_type"] = "structured"
    return enriched


def store_in_cosmos(enriched_log):
    c = get_cosmos_container()
    if not c:
        logging.error("Cosmos container not initialized. Skipping log storage.")
        return

    if "service" not in enriched_log or not enriched_log["service"]:
        enriched_log["service"] = "unknown_service"

    try:
        json_str = json.dumps(enriched_log)
        enriched_log = json.loads(json_str)
    except Exception as e:
        logging.error(f"Cannot insert: not JSON serializable - {e}")
        return

    try:
        c.upsert_item(enriched_log)
        logging.info(f"💾 Log stored in Cosmos DB (source: {enriched_log.get('source_type', 'unknown')}).")
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"❌ Cosmos DB Error: {e.status_code} - {e.message}")
    except Exception as e:
        logging.error(f"❌ Failed to store log in Cosmos DB: {e}")


# ===============================
# AZURE FUNCTION
# ===============================
app = func.FunctionApp()

@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="log-events",
    subscription_name="log-processing-subscriber",
    connection="SERVICE_BUS_CONNECTION"
)
def process_logs(msg: func.ServiceBusMessage):
    try:
        log_str = msg.get_body().decode('utf-8')
        log_data = json.loads(log_str)
        logging.info(f"📩 Received log: {log_data}")

        # Ignore unstructured logs
        if "raw" in log_data:
            logging.warning("⚠️ Skipping unstructured log (raw_s detected).")
            return

        # Structured log: enrich and store
        enriched = enrich_json_log(log_data)
        store_in_cosmos(enriched)

        logging.info("✅ Structured log processed and stored successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to process log: {e}")
