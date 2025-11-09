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
SEARCH_API_KEY = "COwADWfHZzwCWNaz5l2e9jyw4ct2ADemNXBPYe4u69AzSeAKr4BQ"
SEARCH_INDEX_NAME = "log-vector-index"

# Azure OpenAI
AZURE_OPENAI_ENDPOINT = "https://logs-vector-ai.openai.azure.com/"
AZURE_OPENAI_KEY = "3UH0zbXFpdyjKx0jsKqpe1F4Z6M7tHXsqvAKJyCw6YMJMdMDAGNuJQQJ99BKACYeBjFXJ3w3AAABACOGyYji"
OPENAI_EMBEDDING_MODEL = "text-embedding-ada-002"
OPENAI_API_VERSION="2023-05-15"
COSMOS_CONNECTION_STRING="AccountEndpoint=https://metricsdatabse.documents.azure.com:443/;AccountKey=BVmnqMiF25adqQom2xTbBbGA9ao4ia0L7tji0mywIA6Ne8zoJPpeBFPBw2Vqzl5qFoMJ8oYlQNLpACDbDq0FpQ==;"