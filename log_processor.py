import logging
import json
import os
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from config import workspace_id, servicebus_conn_str, topic_name, blob_service_client
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# --------------------
# Initialize client
# --------------------
credential = DefaultAzureCredential()
log_client = LogsQueryClient(credential)

# -------- KQL QUERIES --------
JSON_KQLS = [
    # 1. Critical or Error logs from key services
    "JsonLogsTab_CL | where Level in ('CRITICAL', 'ERROR') and service_s in ('auth-api', 'payments', 'orders', 'billing')",

    # 2. High CPU usage with warnings or errors
    "JsonLogsTab_CL | where cpu_percent_d > 85 and Level in ('WARN', 'ERROR')",

    # 3. High memory usage with warnings or errors
    "JsonLogsTab_CL | where memory_mb_d > 4096 and Level in ('WARN', 'ERROR')",

    # 4. High latency or slow response logs
    "JsonLogsTab_CL | where response_ms_d > 2000 and Level in ('WARN', 'ERROR')",

    # 5. Authentication or privilege related warnings/errors
    "JsonLogsTab_CL | where Level in ('WARN', 'ERROR') and (Message contains 'login' or Message contains 'privilege')",

    # 6. Logs with explicit error codes and severe levels
    "JsonLogsTab_CL | where isnotempty(error_code_s) and Level in ('ERROR', 'CRITICAL')",

    # 7. Failed transactions
    "JsonLogsTab_CL | where transaction_id_s != '' and Message contains 'Transaction failed'",

    # 8. Pod or container failures (OOMKilled, restarts)
    "JsonLogsTab_CL | where Level in ('ERROR','CRITICAL') and isnotempty(pod_s) and (Message contains 'OOMKilled' or Message contains 'Pod restarted')",

    # 9. High network I/O
    "JsonLogsTab_CL | where bytes_in_d > 1000000 or bytes_out_d > 1000000",

    # 10. All critical/error level logs
    "JsonLogsTab_CL | where Level in ('ERROR','CRITICAL')"
]


TEXT_KQLS = [
    "TextLogsTable_CL | where raw_s contains 'Kernel panic' or raw_s contains 'OutOfMemoryException'",
    "TextLogsTable_CL | where raw_s contains 'Transaction failed' or raw_s contains 'Payment captured'",
    "TextLogsTable_CL | where raw_s contains 'login' or raw_s contains 'privilege'",
    "TextLogsTable_CL | where raw_s contains 'Latency' or raw_s contains 'Load' or raw_s contains 'CPU'",
    "TextLogsTable_CL | where raw_s contains 'startup' or raw_s contains 'shutdown'",
    "TextLogsTable_CL | where raw_s contains 'Pod restarted' or raw_s contains 'cloud_container' or raw_s contains 'ContainerOOMKilled'",
    "TextLogsTable_CL | where raw_s contains 'Transaction committed'",
    "TextLogsTable_CL | where raw_s contains 'token' or raw_s contains 'JWT validation'",
    "TextLogsTable_CL | where raw_s contains 'ingest' or raw_s contains 'cache'",
    "TextLogsTable_CL | where raw_s contains 'ERROR' or raw_s contains 'CRITICAL'"
]

# -------- Log processing logic --------
def process_logs_logic():
    logging.info(f"🚀 Log processing started at {datetime.utcnow()}")

    def run_kql(query):
        try:
            response = log_client.query_workspace(
                workspace_id=workspace_id,
                query=query,
                timespan=timedelta(hours=1)
            )

            if response.status == LogsQueryStatus.SUCCESS and response.tables:
                table = response.tables[0]
                columns = [col.name if hasattr(col, "name") else col for col in table.columns]

                rows = [dict(zip(columns, row)) for row in table.rows]  # ✅ convert to list of dicts
                logging.info(f" KQL returned {len(rows)} rows.")
                return rows
            else:
                logging.warning(f" KQL returned no data or partial error: {response.partial_error}")
                return []
        except Exception as e:
            logging.error(f" KQL Query failed for query:\n{query}\nError: {e}")
            return []


    def send_to_servicebus_internal(log_rows, rule_name):
        #ensure_topic()
        #ensure_subscriptions()
        if not log_rows:
            return

        try:
            # ✅ Create ServiceBusClient from connection string
            servicebus_client = ServiceBusClient.from_connection_string(conn_str=servicebus_conn_str)
            
            # ✅ Open a sender for the topic
            with servicebus_client.get_topic_sender(topic_name=topic_name) as sender:
                for row in log_rows:
                    message = ServiceBusMessage(json.dumps({
                        "rule": rule_name,
                        "log": row
                    }, default=str))
                    sender.send_messages(message)
            
            logging.info(f"📬 Sent {len(log_rows)} messages for {rule_name} to Service Bus.")

        except Exception as e:
           logging.error(f"Service Bus send failed: {e}")

    
    # Process JSON logs
    for idx, kql in enumerate(JSON_KQLS, start=1):
        logs = run_kql(kql)
        send_to_servicebus_internal(logs, f"JsonRule_{idx}")
        

    # Process Text logs
    for idx, kql in enumerate(TEXT_KQLS, start=1):
        logs = run_kql(kql)
        send_to_servicebus_internal(logs, f"TextRule_{idx}")
        

    logging.info("🎯 Log processing completed successfully.")
