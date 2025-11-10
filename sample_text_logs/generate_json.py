import json
import random
import uuid
from datetime import datetime, timedelta
import os

NUM_FILES = 10   # Number of JSON files to create
NUM_LOGS = 50    # Number of log entries per file
OUTPUT_DIR = "logs"  # Folder to save JSON files

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

services = ["db", "frontend", "worker", "inventory", "billing", "orders", "kube-proxy", "shipping"]
levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
log_types = ["cloud_container", "transaction", "system", "application", "security"]
error_codes = ["NET_01", "NET_02", "NET_03", "E1001", "E1002", "ERR001", "PAY_01"]
tags_pool = ["cache", "ingest", "search", "payment", "retry", "db", "auth"]

def random_ip():
    return ".".join(str(random.randint(1, 255)) for _ in range(4))

def random_user():
    return str(random.randint(100000, 999999)) if random.random() > 0.3 else None

def random_tags():
    return random.sample(tags_pool, random.randint(0, 3))

def random_timestamp():
    base_time = datetime.utcnow()
    delta = timedelta(minutes=random.randint(0, 10000))
    return (base_time - delta).isoformat() + "Z"

def random_transaction():
    return "txn-" + uuid.uuid4().hex[:12]

def random_amount():
    return round(random.uniform(10.0, 1000.0), 2)

for file_index in range(1, NUM_FILES + 1):
    logs = []
    for _ in range(NUM_LOGS):
        log = {
            "timestamp": random_timestamp(),
            "service": random.choice(services),
            "level": random.choice(levels),
            "log_type": random.choice(log_types),
            "message": random.choice([
                "Cache miss for key",
                "Service endpoint updated",
                "Transaction rolled back",
                "Unhandled exception in handler",
                "System shutdown initiated",
                "Payment captured",
                "Failed login attempt",
                "Database connection error"
            ]),
            "trace_id": uuid.uuid4().hex,
            "span_id": uuid.uuid4().hex[:16],
            "user_id": random_user(),
            "client_ip": random_ip(),
            "host": random.choice([f"host-{random.randint(1,500)}", None]),
            "error_code": random.choice(error_codes),
            "tags": random_tags()
        }

        # Randomly add optional fields
        if random.random() > 0.5:
            log["transaction_id"] = random_transaction()
            log["amount"] = random_amount()
            log["currency"] = random.choice(["USD", "INR", "EUR"])
        if random.random() > 0.7:
            log["k8s_namespace"] = random.choice(["default", "production"])
            log["pod"] = f"pod-{random.randint(1,1000)}"

        logs.append(log)

    file_path = os.path.join(OUTPUT_DIR, f"logs_file_{file_index}.json")
    with open(file_path, "w") as f:
        json.dump(logs, f, indent=4)

    print(f"Generated {file_path}")
