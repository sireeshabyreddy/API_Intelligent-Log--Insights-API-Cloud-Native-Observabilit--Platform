import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient

load_dotenv()

keyvault_name = os.getenv("KEYVAULT_NAME")
vault_url = f"https://{keyvault_name}.vault.azure.net"
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=vault_url, credential=credential)

# Fetch secrets
workspace_id = secret_client.get_secret("LogAnalyticsWorkspaceId").value
shared_key = secret_client.get_secret("LogAnalyticsPrimaryKey").value
storage_connection_string = secret_client.get_secret("BlobStorageConnectionString").value
# servicebus_conn_str = secret_client.get_secret("ServiceBusConnectionString").value
# servicebus_queue_name = secret_client.get_secret("ServiceBusQueueName").value

# Initialize clients
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
# servicebus_client = ServiceBusClient.from_connection_string(servicebus_conn_str, logging_enable=True)
subscription_id = secret_client.get_secret("AzureSubscriptionId").value
resource_group = secret_client.get_secret("ServiceBusResourceGroup").value
namespace_name = secret_client.get_secret("ServiceBusNamespace").value
topic_name = secret_client.get_secret("ServiceBusTopic").value
servicebus_conn_str = secret_client.get_secret("ServiceBusConnectionString").value
DEDUP_CACHE_FOLDER="cache"
# Azure Cognitive Search
SEARCH_SERVICE_NAME = "intelligent-log-search"

SEARCH_API_KEY = secret_client.get_secret("search-api-key").value
SEARCH_INDEX_NAME = secret_client.get_secret("search-index-name").value

# ---------------- Azure OpenAI ----------------
AZURE_OPENAI_ENDPOINT = secret_client.get_secret("azure-openai-endpoint").value
AZURE_OPENAI_KEY = secret_client.get_secret("azure-openai-key").value
OPENAI_EMBEDDING_MODEL = secret_client.get_secret("openai-embedding-model").value
OPENAI_API_VERSION = secret_client.get_secret("openai-api-version").value

# ---------------- Cosmos DB ----------------
COSMOS_CONNECTION_STRING = secret_client.get_secret("cosmos-connection-string").value