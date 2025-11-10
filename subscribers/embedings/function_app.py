import azure.functions as func
import logging
import os
import uuid
import json
import traceback
import re
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI

# -------------------------------
# Azure Function App initialization
# -------------------------------
app = func.FunctionApp()

# -------------------------------
# Environment Variables
# -------------------------------
service_name = os.getenv("SEARCH_SERVICE_NAME")
api_key = os.getenv("SEARCH_API_KEY")
index_name = os.getenv("SEARCH_INDEX_NAME", "log-vector-index")
azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
azure_openai_key = os.getenv("AZURE_OPENAI_KEY")
embedding_model = "text-embedding-ada-002"

# Validate environment variables
missing_vars = [
    v for v in ["SEARCH_SERVICE_NAME", "SEARCH_API_KEY", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_KEY"]
    if not os.getenv(v)
]
if missing_vars:
    logging.error(f"Missing environment variables: {', '.join(missing_vars)}")

# -------------------------------
# Initialize clients once
# -------------------------------
search_client = SearchClient(
    endpoint=f"https://{service_name}.search.windows.net",
    index_name=index_name,
    credential=AzureKeyCredential(api_key)
)

azure_openai_client = AzureOpenAI(
    api_key=azure_openai_key,
    azure_endpoint=azure_openai_endpoint,
    api_version=os.getenv("OPENAI_API_VERSION", "2024-06-01")
)

# -------------------------------
# Helper: Generate embedding
# -------------------------------
def generate_embedding(text: str):
    if not text:
        return []
    try:
        response = azure_openai_client.embeddings.create(
            model=embedding_model,
            input=text
        )
        return response.data[0].embedding
    except Exception:
        logging.error(f"Failed to generate embedding:\n{traceback.format_exc()}")
        return []

# -------------------------------
# Helper: Extract JSON from text
# -------------------------------
def extract_json_from_text(raw_text: str):
    """
    Scan each line for JSON objects; if plain text, wrap as log dict.
    """
    json_objects = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            # Normalize "Message" key if exists
            if "Message" in obj:
                obj["message"] = obj.pop("Message")
            json_objects.append({"log": obj})
        except json.JSONDecodeError:
            json_objects.append({"log": {"message": line}})
    return json_objects

# -------------------------------
# Helper: Enrich numeric & timestamp fields
# -------------------------------
def enrich_log_fields(log_data: dict):
    text = log_data.get("message", "")

    # Extract timestamp (ISO format)
    ts_match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)", text)
    if ts_match:
        log_data["timestamp"] = ts_match.group(1)

    # Example numeric extraction
    cpu_match = re.search(r"cpu(?:_percent)?=(\d+)", text, re.IGNORECASE)
    if cpu_match:
        log_data["cpu_percent"] = int(cpu_match.group(1))

    mem_match = re.search(r"mem(?:_mb)?=(\d+)", text, re.IGNORECASE)
    if mem_match:
        log_data["memory_mb"] = int(mem_match.group(1))

    resp_match = re.search(r"resp(?:onse)?_ms=(\d+)", text, re.IGNORECASE)
    if resp_match:
        log_data["response_ms"] = int(resp_match.group(1))

    return log_data

# -------------------------------
# Helper: Parse raw log message
# -------------------------------
def parse_log_message(raw_message: str):
    """
    Extract level, service, and message from raw log line.
    Example:
    "2025-11-09T08:27:03.466385Z DEBUG analytics: txn=- resp_ms=445 ..."
    """
    parsed = {"level": "", "service": "", "message": ""}

    # Remove timestamp
    try:
        _, rest = raw_message.split("Z", 1)
    except ValueError:
        rest = raw_message

    # Match LEVEL SERVICE: MESSAGE
    m = re.match(r"\s*(\w+)\s+([^\s:]+):\s*(.*)", rest.strip())
    if m:
        parsed["level"] = m.group(1)
        parsed["service"] = m.group(2)
        parsed["message"] = m.group(3)
    else:
        parsed["message"] = rest.strip()

    return parsed

# -------------------------------
# Helper: Prepare text for embedding
# -------------------------------
def clean_log_text(log_data: dict):
    parts = [
        log_data.get("service", ""),
        log_data.get("level", ""),
        log_data.get("message", "")
    ]
    cleaned = " ".join([p for p in parts if p and p != "-"]).strip()
    return cleaned

# -------------------------------
# Service Bus Topic Trigger
# -------------------------------

@app.function_name(name="vector-func")
@app.service_bus_topic_trigger(
    arg_name="azservicebus",
    subscription_name="vector-embedding-subscriber",
    topic_name="log-events",
    connection="SERVICE_BUS_CONNECTION"
)
def Vectorembeddingsfunc(azservicebus: func.ServiceBusMessage):
    logging.info("🚀 Processing new message from Service Bus topic...")

    # Decode message
    try:
        message_body = azservicebus.get_body().decode('utf-8')
        logging.info(f"Received message: {message_body[:200]}...")
        
        try:
            # Try to parse JSON
            message_json = json.loads(message_body)
            log_dict = message_json.get("log", {})
            raw_message = log_dict.get("message") or log_dict.get("Message") or ""
            log_type = message_json.get("rule", "")
        except json.JSONDecodeError:
            # Plain text message (treat as single line)
            raw_message = message_body.strip()
            log_type = "text"
        
        if not raw_message:
            logging.warning(f"Skipping empty message: {azservicebus.message_id}")
            return

    except Exception:
        logging.error(f"Failed to decode message:\n{traceback.format_exc()}")
        return

    # Parse and enrich
    parsed = parse_log_message(raw_message)
    log_data = {
        "id": str(uuid.uuid4()),
        "message": parsed.get("message", ""),
        "level": parsed.get("level", ""),
        "service": parsed.get("service", ""),
        "timestamp": None,
        "cpu_percent": None,
        "memory_mb": None,
        "response_ms": None,
        "log_type": log_type,
        "error_code": "",
        "tags": [],
        "trace_id": "",
        "span_id": "",
        "user_id": "",
        "client_ip": "",
        "host": "",
        "transaction_id": "",
        "amount": None,
        "currency": "",
        "k8s_namespace": "",
        "bytes_in": 0,
        "bytes_out": 0,
        "component": ""
    }
    log_data = enrich_log_fields(log_data)

    # Generate embedding
    text_to_embed = clean_log_text(log_data)
    embedding = generate_embedding(text_to_embed)
    if not embedding:
        logging.warning(f"Skipping document {log_data['id']} due to empty embedding.")
        return
    log_data["log_vector"] = embedding

    # Upload to Azure Cognitive Search
    try:
        result = search_client.upload_documents(documents=[log_data])
        if result[0].succeeded:
            logging.info(f"Document uploaded successfully: {log_data['id']}")
        else:
            logging.error(f"Failed to upload document: {result[0].error_message}")
    except Exception:
        logging.error(f"Error uploading to Azure Cognitive Search:\n{traceback.format_exc()}")
