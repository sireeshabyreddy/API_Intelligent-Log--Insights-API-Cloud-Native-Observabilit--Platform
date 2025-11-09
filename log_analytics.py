import json, base64, hmac, hashlib, datetime
import httpx
from config import workspace_id, shared_key

def build_signature(workspace_id, shared_key, date, content_length,
                    method='POST', content_type='application/json', resource='/api/logs'):
    x_headers = 'x-ms-date:' + date
    string_to_hash = f'{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}'
    bytes_to_hash = string_to_hash.encode('utf-8')
    decoded_key = base64.b64decode(shared_key)
    hashed = hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(hashed).decode()
    return f'SharedKey {workspace_id}:{signature}'

async def send_logs(batch, log_type):
    """
    Async function to send logs to Azure Log Analytics.
    """
    body = json.dumps(batch)
    content_length = len(body)
    rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    signature = build_signature(workspace_id, shared_key, rfc1123date, content_length)
    
    uri = f'https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01'
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': signature,
        'Log-Type': log_type,
        'x-ms-date': rfc1123date
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(uri, content=body, headers=headers)

    if response.status_code == 200:
        return {"status": "success", "count": len(batch)}
    return {"status": "error", "code": response.status_code, "message": response.text}
