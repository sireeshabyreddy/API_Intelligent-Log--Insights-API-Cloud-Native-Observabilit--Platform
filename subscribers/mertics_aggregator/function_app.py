import azure.functions as func
import logging
import json
import os
from datetime import datetime, timezone
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

# ----------------- CONFIGURATION -----------------
DB_NAME = "MetricsDB"
CONTAINER_NAME = "Metrics"
ERROR_THRESHOLD_PERCENT = 5
BATCH_WRITE_LIMIT = 50

# ----------------- COSMOS DB INITIALIZATION -----------------
COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
metrics_container = None

if COSMOS_CONNECTION_STRING:
    try:
        cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
        db = cosmos_client.get_database_client(DB_NAME)
        metrics_container = db.get_container_client(CONTAINER_NAME)
        logging.info("Connected to Cosmos DB successfully.")
    except Exception as e:
        logging.error(f"Cosmos DB initialization failed: {str(e)}")
else:
    logging.warning("COSMOS_CONNECTION_STRING not set. Metrics will not be persisted.")

# ----------------- BLOB STORAGE INITIALIZATION -----------------
BLOB_CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER", "alerts-logs")
blob_service_client = None
container_client = None

if BLOB_CONNECTION_STRING:
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER)
        try:
            container_client.create_container()
        except Exception:
            pass  # container already exists
        logging.info("Connected to Blob Storage successfully.")
    except Exception as e:
        logging.error(f"Blob Storage initialization failed: {str(e)}")
else:
    logging.warning("BLOB_CONNECTION_STRING not set. Alerts will not be persisted.")

# ----------------- FUNCTION APP -----------------
app = func.FunctionApp()

# ----------------- GLOBAL BATCH -----------------
metric_batch = {}

# ----------------- HELPER FUNCTIONS -----------------
def compute_derived_metrics(metric_doc: dict) -> dict:
    total = metric_doc.get("total_logs", 1) or 1  # avoid division by zero
    metric_doc["avg_cpu"] = round(metric_doc.get("cpu_sum", 0) / total, 2)
    metric_doc["avg_memory"] = round(metric_doc.get("memory_sum", 0) / total, 2)
    metric_doc["avg_latency"] = round(metric_doc.get("latency_sum", 0) / total, 2)
    metric_doc["error_rate_percent"] = round((metric_doc.get("error_logs", 0) / total) * 100, 2)
    metric_doc["throughput_per_min"] = total
    return metric_doc

def log_alert(metric_doc: dict, service: str):
    if metric_doc.get("error_rate_percent", 0) > ERROR_THRESHOLD_PERCENT and container_client:
        alert_msg = (
            f"ALERT: Service {service} exceeded error threshold! "
            f"Error rate: {metric_doc['error_rate_percent']:.2f}% | "
            f"Time window: {metric_doc['id']}\n"
        )
        blob_name = f"{service}_{datetime.utcnow().strftime('%Y-%m-%d')}.log"
        blob_client = container_client.get_blob_client(blob_name)
        try:
            existing = blob_client.download_blob().readall().decode('utf-8')
        except Exception:
            existing = ""
        blob_client.upload_blob(existing + alert_msg, overwrite=True)
        logging.warning(alert_msg)

def update_metric_doc(metric_doc: dict, log: dict) -> dict:
    level = log.get("level", "").lower()
    metric_doc["total_logs"] = metric_doc.get("total_logs", 0) + 1
    metric_doc["error_logs"] = metric_doc.get("error_logs", 0) + (1 if level == "error" else 0)
    metric_doc["warning_logs"] = metric_doc.get("warning_logs", 0) + (1 if level == "warning" else 0)

    metric_doc["cpu_sum"] = metric_doc.get("cpu_sum", 0) + log.get("cpu_percent", 0)
    metric_doc["max_cpu"] = max(metric_doc.get("max_cpu", 0), log.get("cpu_percent", 0))

    metric_doc["memory_sum"] = metric_doc.get("memory_sum", 0) + log.get("memory_mb", 0)
    metric_doc["max_memory"] = max(metric_doc.get("max_memory", 0), log.get("memory_mb", 0))

    metric_doc["latency_sum"] = metric_doc.get("latency_sum", 0) + log.get("response_ms", 0)
    metric_doc["max_latency"] = max(metric_doc.get("max_latency", 0), log.get("response_ms", 0))

    metric_doc["bytes_in"] = metric_doc.get("bytes_in", 0) + log.get("bytes_in", 0)
    metric_doc["bytes_out"] = metric_doc.get("bytes_out", 0) + log.get("bytes_out", 0)

    metric_doc["transactions_sum"] = metric_doc.get("transactions_sum", 0) + (log.get("amount") or 0)
    metric_doc["transactions_count"] = metric_doc.get("transactions_count", 0) + (1 if "amount" in log else 0)

    user_id = str(log.get("user_id", "unknown"))
    user_requests = metric_doc.get("user_requests", {})
    user_requests[user_id] = user_requests.get(user_id, 0) + 1
    metric_doc["user_requests"] = user_requests

    err_code = log.get("error_code")
    error_code_freq = metric_doc.get("error_code_freq", {})
    if err_code:
        error_code_freq[err_code] = error_code_freq.get(err_code, 0) + 1
    metric_doc["error_code_freq"] = error_code_freq

    comp = log.get("component")
    component_count = metric_doc.get("component_count", {})
    if comp:
        component_count[comp] = component_count.get(comp, 0) + 1
    metric_doc["component_count"] = component_count

    ns = log.get("k8s_namespace")
    namespace_count = metric_doc.get("namespace_count", {})
    if ns:
        namespace_count[ns] = namespace_count.get(ns, 0) + 1
    metric_doc["namespace_count"] = namespace_count

    metric_doc["semantic_anomalies"] = metric_doc.get("semantic_anomalies", 0)

    return compute_derived_metrics(metric_doc)

def upsert_metrics_batch():
    global metric_batch
    if metrics_container and metric_batch:
        for doc_id, doc in metric_batch.items():
            try:
                metrics_container.upsert_item(doc)
            except Exception as e:
                logging.error(f"Failed to upsert metric doc {doc_id}: {str(e)}")
        metric_batch = {}

# ----------------- SERVICE BUS TRIGGER -----------------
@app.service_bus_topic_trigger(
    arg_name="azservicebus",
    subscription_name="metrics-subscriber",
    topic_name="log-events",
    connection="SERVICE_BUS_CONNECTION"
)
def metricsaggregator(azservicebus: func.ServiceBusMessage):
    global metric_batch
    try:
        message_body = azservicebus.get_body().decode('utf-8')
        msg = json.loads(message_body)

        log = msg.get("log", {})
        service = log.get("service", "unknown")

        timestamp_str = log.get("timestamp")
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")) if timestamp_str else datetime.utcnow()
        except ValueError:
            timestamp = datetime.utcnow()

        metric_id = f"{service}-{timestamp.strftime('%Y-%m-%d-%H-%M')}"
        metric_doc = metric_batch.get(metric_id, {
            "id": metric_id,
            "service": service,
            "total_logs": 0,
            "error_logs": 0,
            "warning_logs": 0,
            "cpu_sum": 0,
            "max_cpu": 0,
            "memory_sum": 0,
            "max_memory": 0,
            "latency_sum": 0,
            "max_latency": 0,
            "bytes_in": 0,
            "bytes_out": 0,
            "transactions_sum": 0,
            "transactions_count": 0,
            "user_requests": {},
            "error_code_freq": {},
            "component_count": {},
            "namespace_count": {},
            "semantic_anomalies": 0
        })

        metric_doc = update_metric_doc(metric_doc, log)
        metric_batch[metric_id] = metric_doc

        log_alert(metric_doc, service)
        logging.info(f"Processed metrics for service: {service}, window: {metric_id}")

        if len(metric_batch) >= BATCH_WRITE_LIMIT:
            upsert_metrics_batch()

    except json.JSONDecodeError:
        logging.error("Failed to decode message body as JSON")
    except Exception as e:
        logging.error(f"Failed to process message: {str(e)}")
