import json
from azure.servicebus.aio import ServiceBusClient, ServiceBusSender
from azure.servicebus import ServiceBusMessage
from config import servicebus_conn_str
from servicebus_setup import ensure_topic
import asyncio

def chunk_list(data, chunk_size=100):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

async def send_to_servicebus(batch, rule_name):
    if not batch:
        return {"status": "skipped", "message": "No logs to send."}

    try:
        topic_name = ensure_topic()
        print(f"🚀 Preparing to send {len(batch)} logs to topic '{topic_name}'")

        async with ServiceBusClient.from_connection_string(servicebus_conn_str) as client:
            async with client.get_topic_sender(topic_name=topic_name) as sender:
                total_sent = 0
                for chunk in chunk_list(batch, 100):
                    sb_batch = await sender.create_message_batch()
                    for log in chunk:
                        msg = ServiceBusMessage(json.dumps({
                            "rule": rule_name,
                            "log": log
                        }, default=str))
                        try:
                            sb_batch.add_message(msg)
                        except ValueError:
                            # batch full, send current batch and start new
                            await sender.send_messages(sb_batch)
                            sb_batch = await sender.create_message_batch()
                            sb_batch.add_message(msg)

                    await sender.send_messages(sb_batch)
                    total_sent += len(chunk)
                    print(f"✅ Sent {len(chunk)} messages to topic '{topic_name}'")

        return {"status": "success", "count": total_sent}

    except Exception as e:
        print(f"❌ Failed to send messages: {e}")
        return {"status": "error", "message": str(e)}
