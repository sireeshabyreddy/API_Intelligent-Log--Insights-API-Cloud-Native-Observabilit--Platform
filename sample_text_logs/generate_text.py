import random
import datetime
import uuid
import os
from faker import Faker

fake = Faker()

# Configuration
NUM_FILES = 10        # Number of text files to generate
LOGS_PER_FILE = 50    # Number of logs per file
OUTPUT_DIR = "generated_logs"  # Directory to save files

# Log components
services = ["db", "frontend", "billing", "worker", "inventory", "auth-service",
            "kube-proxy", "payments", "analytics", "api-gateway", "orders"]
levels = ["INFO", "DEBUG", "WARN", "ERROR", "CRITICAL"]
log_types = ["system", "application", "cloud_container", "transaction", "security", "network", "error"]
messages = [
    "Service endpoint updated",
    "NullReferenceException in module",
    "Transaction committed",
    "Failed login attempt",
    "CPU usage at 95% for 5m",
    "OutOfMemoryException",
    "Payment captured",
    "Policy updated",
    "Suspicious token reuse detected",
    "Slow GC pause detected",
    "Database connection timeout",
    "Hardware temperature threshold exceeded",
    "Pod restarted by kubelet",
]

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to generate a single log line
def generate_log():
    timestamp = datetime.datetime.utcnow().isoformat() + "Z"
    service = random.choice(services)
    level = random.choice(levels)
    log_type = random.choice(log_types)
    message = random.choice(messages)
    trace = uuid.uuid4().hex[:24]
    span = uuid.uuid4().hex[:16]
    user = fake.random_int(min=100000, max=999999) if random.random() > 0.7 else "-"
    host = f"host-{fake.random_int(1, 500)}" if random.random() > 0.5 else "-"
    ip = fake.ipv4_public()
    txn = f"txn-{uuid.uuid4().hex[:8]}" if random.random() > 0.6 else "-"
    resp_ms = random.randint(20, 3000)

    log_line = f"{timestamp} {level} {service}: txn={txn} resp_ms={resp_ms} - {message} user={user} ip={ip} host={host} trace={trace}"
    return log_line

# Generate multiple files
for file_index in range(1, NUM_FILES + 1):
    file_path = os.path.join(OUTPUT_DIR, f"logs_{file_index}.txt")
    with open(file_path, "w") as f:
        for _ in range(LOGS_PER_FILE):
            f.write(generate_log() + "\n")
    print(f"Generated {file_path}")

print("\nAll log files generated successfully!")
