import requests
from azure.identity import DefaultAzureCredential

# ------------------------------
# Direct values
# ------------------------------
subscription_id = "c913da64-f292-4a02-bd9c-eb36627e33be"
resource_group = "LogInsightsRG"
workspace_name = "LogInsightsWorkspace"
table_name = "TextLogsDataTable_CL"  # table to purge

# ------------------------------
# Build purge URL
# ------------------------------
purge_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.OperationalInsights/workspaces/{workspace_name}/purge?api-version=2025-07-01"

# ------------------------------
# Payload (use catch-all TimeGenerated filter)
# ------------------------------
payload = {
    "table": table_name,
    "filters": [
        {
            "column": "TimeGenerated",
            "operator": ">",
            "value": "1900-01-01T00:00:00Z"
        }
    ]
}

# ------------------------------
# Authenticate using Azure SDK
# ------------------------------
credential = DefaultAzureCredential()
token = credential.get_token("https://management.azure.com/.default").token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ------------------------------
# Send purge request
# ------------------------------
response = requests.post(purge_url, headers=headers, json=payload)
print(response.status_code, response.text)
