import requests
from azure.identity import DefaultAzureCredential

subscription_id = "c913da64-f292-4a02-bd9c-eb36627e33be"
resource_group = "LogInsightsRG"
workspace_name = "LogInsightsWorkspace"
operation_id = "purge-70e6680d-512a-467d-baf7-eca63dcff595"

status_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.OperationalInsights/workspaces/{workspace_name}/purge/{operation_id}?api-version=2025-07-01"

credential = DefaultAzureCredential()
token = credential.get_token("https://management.azure.com/.default").token
headers = {"Authorization": f"Bearer {token}"}

response = requests.get(status_url, headers=headers)
print(response.status_code, response.json())
