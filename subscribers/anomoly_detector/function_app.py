import azure.functions as func
import json
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ======================================================
# CONFIGURATION
# ======================================================
LOGS_TOPIC_NAME = "log-events"
ALERTS_TOPIC_NAME = "alerts-events"

# Thresholds for structured anomalies
CPU_HIGH = 90
CPU_MED = 75
RESP_HIGH = 3000
RESP_MED = 2000
MEM_HIGH = 16000  # MB
BYTES_IN_HIGH = 10_000_000
BYTES_OUT_HIGH = 10_000_000

# Pattern-based anomaly thresholds
PATTERN_WINDOW_MINUTES = 5
ERROR_COUNT_THRESHOLD = 5
FAILED_LOGIN_KEYWORDS = ["failed login", "login failed", "authentication failed"]

# In-memory recent logs for pattern detection
recent_logs = defaultdict(list)

app = func.FunctionApp()


# ======================================================
# HELPER FUNCTIONS
# ======================================================
def detect_structured_anomaly(log: dict) -> (bool, str):
    """Detect anomalies using your JSON schema column names."""
    try:
        cpu = float(log.get("cpu_percent_d_d") or log.get("cpu_percent_d") or log.get("cpu_percent") or 0)
        response = float(log.get("response_ms_d_d") or log.get("response_ms_d") or log.get("response_ms") or 0)
        memory = float(log.get("memory_mb_d_d") or log.get("memory_mb_d") or log.get("memory_mb") or 0)
        bytes_in = float(log.get("bytes_in_d") or log.get("bytes_in") or 0)
        bytes_out = float(log.get("bytes_out_d") or log.get("bytes_out") or 0)
        level = str(log.get("Level") or log.get("level") or "").lower()

        if cpu > CPU_HIGH:
            return True, f"High CPU usage: {cpu}%"
        if cpu > CPU_MED and response > RESP_MED:
            return True, f"CPU {cpu}% + response {response}ms"
        if response > RESP_HIGH:
            return True, f"High response time: {response}ms"
        if memory > MEM_HIGH:
            return True, f"High memory usage: {memory} MB"
        if bytes_in > BYTES_IN_HIGH or bytes_out > BYTES_OUT_HIGH:
            return True, f"High network bytes in/out: {bytes_in}/{bytes_out}"
        if any(lvl in level for lvl in ["error", "critical", "warn", "warning"]):
            return True, f"High severity log: {level}"
        return False, ""
    except Exception as e:
        logging.error(f"Structured anomaly detection error: {e}")
        return False, "Exception in detection"


def detect_unstructured_anomaly(raw_log: str) -> (bool, str):
    keywords = [
        "error", "failed", "fail", "critical", "exception",
        "timeout", "alert", "crash", "not responding", "issue"
    ]
    text = raw_log.lower()
    for kw in keywords:
        if kw in text:
            return True, f"Keyword matched: {kw}"
    return False, ""


def detect_pattern_anomaly(log: dict) -> (bool, str):
    """Detect repeated failures based on user/client/service in PATTERN_WINDOW_MINUTES."""
    try:
        now = datetime.utcnow()
        key = log.get("user_id_s") or log.get("client_ip_s") or log.get("service_s_s") or "unknown"
        # Clean old logs
        recent_logs[key] = [ts for ts in recent_logs[key] if now - ts < timedelta(minutes=PATTERN_WINDOW_MINUTES)]

        msg = str(log.get("Message") or log.get("message") or "").lower()
        level = str(log.get("Level") or log.get("level") or "").lower()

        if any(kw in msg for kw in FAILED_LOGIN_KEYWORDS) or any(lvl in level for lvl in ["error", "critical", "warn", "warning"]):
            recent_logs[key].append(now)

        if len(recent_logs[key]) >= ERROR_COUNT_THRESHOLD:
            return True, f"Repeated failures for {key} in last {PATTERN_WINDOW_MINUTES} min"
        return False, ""
    except Exception as e:
        logging.error(f"Pattern anomaly check error: {e}")
        return False, "Exception in pattern detection"


def send_alert(alert_data: dict):
    """Send alert to Service Bus topic."""
    connection_str = os.environ.get("SERVICE_BUS_CONNECTION_ALERTS")
    if not connection_str:
        logging.error("SERVICE_BUS_CONNECTION_ALERTS not set in environment!")
        return

    try:
        with ServiceBusClient.from_connection_string(connection_str) as client:
            sender = client.get_topic_sender(topic_name=ALERTS_TOPIC_NAME)
            with sender:
                sender.send_messages(ServiceBusMessage(json.dumps(alert_data)))
                logging.info(f"🚨 Alert sent for {alert_data.get('service_s_s','unknown')}")
    except Exception as e:
        logging.error(f"Failed to send alert: {e}")


# ======================================================
# MAIN FUNCTION
# ======================================================
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name=LOGS_TOPIC_NAME,
    subscription_name="anomaly-detection-subscriber",
    connection="SERVICE_BUS_CONNECTION"
)
def anomaly_detector(msg: func.ServiceBusMessage):
    """Detect anomalies in structured or raw logs and send alerts."""
    try:
        log_str = msg.get_body().decode("utf-8")
        logging.info(f"📩 Received log: {log_str}")

        try:
            log_data = json.loads(log_str)
        except json.JSONDecodeError:
            log_data = None

        if isinstance(log_data, dict) and "log" in log_data:
            log_data = log_data["log"]

        alerts = []

        # ---------- Unstructured logs ----------
        if not isinstance(log_data, dict):
            detected, reason = detect_unstructured_anomaly(log_str)
            if detected:
                alerts.append({
                    "alert_type": "UNSTRUCTURED_ANOMALY",
                    "message": log_str[:300],
                    "detected_reason": reason,
                    "timestamp": datetime.utcnow().isoformat()
                })

        # ---------- Structured logs ----------
        else:
            detected_struct, reason_struct = detect_structured_anomaly(log_data)
            if detected_struct:
                alerts.append({
                    "alert_type": "STRUCTURED_ANOMALY",
                    **log_data,
                    "detected_reason": reason_struct
                })

            detected_pattern, reason_pattern = detect_pattern_anomaly(log_data)
            if detected_pattern:
                alerts.append({
                    "alert_type": "PATTERN_ANOMALY",
                    **log_data,
                    "detected_reason": reason_pattern
                })

        # Send alerts
        for alert in alerts:
            logging.warning(f"⚠️ Anomaly detected: {alert}")
            send_alert(alert)

        if not alerts:
            logging.info("✅ No anomaly detected.")

    except Exception as e:
        logging.error(f"Failed to process log: {e}")
