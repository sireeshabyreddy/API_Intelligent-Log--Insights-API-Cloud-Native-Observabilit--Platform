import os
from azure.mgmt.servicebus import ServiceBusManagementClient

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.servicebus import ServiceBusManagementClient
from azure.mgmt.servicebus.models import SBSubscription, SBTopic
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from config import *

# ===== Initialize Service Bus management client =====
sb_mgmt_client = ServiceBusManagementClient(credential, subscription_id)

subscriber_names = [
    "log-processing-subscriber",
    "anomaly-detection-subscriber",
    "vector-embedding-subscriber",
    "metrics-subscriber",
    
]

# Ensure topic exists
def ensure_topic():
    topics = sb_mgmt_client.topics.list_by_namespace(resource_group, namespace_name)
    topic_names = [t.name for t in topics]
    if topic_name not in topic_names:
        sb_mgmt_client.topics.create_or_update(resource_group, namespace_name, topic_name, SBTopic())
        print(f"✅ Created topic '{topic_name}'")
    else:
        print(f"Topic '{topic_name}' already exists")
    return topic_name

# Ensure subscribers exist
def ensure_subscriptions():
    existing_subs = sb_mgmt_client.subscriptions.list_by_topic(resource_group, namespace_name, topic_name)
    existing_names = [s.name for s in existing_subs]

    for sub in subscriber_names:
        if sub not in existing_names:
            sb_mgmt_client.subscriptions.create_or_update(
                resource_group,
                namespace_name,
                topic_name,
                sub,
                SBSubscription()
            )
            print(f"✅ Created subscription '{sub}'")
        else:
            print(f"Subscription '{sub}' already exists")

# ===== Send logs to Service Bus =====
