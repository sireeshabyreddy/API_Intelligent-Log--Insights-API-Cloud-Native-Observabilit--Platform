import os
import json
import hashlib
import datetime
import logging
from fastapi import FastAPI, UploadFile, Form, Query, HTTPException
from fastapi.responses import JSONResponse
import asyncio
from services.vector_service import semantic_search_logs
from log_analytics import send_logs
from blob_storage import upload_to_blob
from service_bus import send_to_servicebus
from servicebus_setup import ensure_topic, ensure_subscriptions
from parse_logs  import load_dedup_cache, save_dedup_cache
import services.cosmos_service as cosmos_service
from services.cosmos_logs_router import router
from services.Metrics_routes import metrics_router  
# -----------------------
# FastAPI app
# -----------------------
app = FastAPI(title="Log Ingest + ServiceBus API", version="4.0")

# -----------------------
# Startup event
# -----------------------
@app.on_event("startup")
def startup_event():
    ensure_topic()
    ensure_subscriptions()




@app.get("/")
def read_root():
    return {"message": "Welcome! Your app is running."}

# -----------------------
# Semantic Search Endpoint
# -----------------------
@app.get("/semantic-search/logs/")
def search_logs(
    query: str = Query(..., min_length=1, description="Search query string"),
    top_k: int = Query(5, ge=1, le=50, description="Number of top results to return")
):
    results = semantic_search_logs(query, top_k)
    return {"query": query, "top_k": top_k, "results": results}
# -----------------------
# Upload Logs Endpoint
# -----------------------
@app.post("/send-logs/")
async def send_logs_api(
    file: UploadFile,
    log_type: str = Form(...),
    format: str = Form("json"),
    skip_dedup: bool = Query(False, description="Set True to skip deduplication"),
    reset_cache: bool = Query(False)
):
    """
    Upload a .txt, .json, or .jsonl file and send logs to Azure Log Analytics.
    Deduplication is applied per log_type unless skip_dedup=True.
    """
    # Reset dedup cache if requested
    if reset_cache:
        dedup_file = f"dedup_state_{log_type}.json"
        if os.path.exists(dedup_file):
            os.remove(dedup_file)
            logging.info(f"♻️ Dedup cache reset for {log_type}")

    content = await file.read()
    text = content.decode().strip()
    raw_batch = []

    # ---- PARSING ----
    if format.lower() == "text":
        raw_batch = [{"Message": line.strip(), "Time": datetime.datetime.utcnow().isoformat() + "Z"}
                     for line in text.splitlines()]
    elif format.lower() == "json":
        try:
            data = json.loads(text)
            raw_batch = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            # JSONL fallback
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    raw_batch.append(json.loads(line))
                except Exception:
                    raw_batch.append({"raw": line})
    else:
        raise HTTPException(status_code=400, detail="Invalid format. Use 'text', 'json', or 'jsonl'.")

    if not raw_batch:
        return {"status": "skipped", "message": "No logs to process."}

    # ---- DEDUPLICATION ----
    if skip_dedup:
        unique_batch = raw_batch
        logging.info(f"⚡ Skipping deduplication. Sending all {len(unique_batch)} logs.")
    else:
        sent_hashes = load_dedup_cache(log_type)
        unique_batch = []
        new_hashes = set()
        for log in raw_batch:
            log_str = json.dumps(log, sort_keys=True)
            log_hash = hashlib.sha256(log_str.encode()).hexdigest()
            if log_hash not in sent_hashes:
                unique_batch.append(log)
                new_hashes.add(log_hash)
        save_dedup_cache(log_type, sent_hashes | new_hashes)
        logging.info(f"✅ {len(unique_batch)} new logs (deduplicated).")

    if not unique_batch:
        return {"status": "skipped", "message": f"No new logs for {log_type} (all duplicates)."}

    # ---- Send to Log Analytics ----
    result = await send_logs(unique_batch, log_type)

    # ---- Upload raw file to Blob Storage ----
    if asyncio.iscoroutinefunction(upload_to_blob):
        blob_url = await upload_to_blob(file.filename, content)

    else:
        blob_url = upload_to_blob(file.filename, content)

    # ---- Send to Service Bus ----
    sb_result = await send_to_servicebus(unique_batch, log_type)
    
    return {
        "status": "success",
        "log_type": log_type,
        "records_sent": len(unique_batch),
        "azure_response": result,
        "blob_url": blob_url,
        "servicebus_result": sb_result
    }

app.include_router(router)
app.include_router(metrics_router)
