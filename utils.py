import datetime
import hashlib
import hmac
import base64
import json

def build_signature(workspace_id, shared_key, date, content_length,
                    method='POST', content_type='application/json', resource='/api/logs'):
    x_headers = 'x-ms-date:' + date
    string_to_hash = f'{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}'
    bytes_to_hash = bytes(string_to_hash, encoding='utf-8')
    decoded_key = base64.b64decode(shared_key)
    hashed = hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(hashed).decode()
    return f'SharedKey {workspace_id}:{signature}'

def parse_logs(text, format_type="json"):
    batch = []
    if format_type.lower() == "text":
        lines = text.splitlines()
        batch = [{"Message": line.strip(), "Time": datetime.datetime.utcnow().isoformat() + "Z"} for line in lines]
    elif format_type.lower() == "json":
        try:
            data = json.loads(text)
            batch = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            for line in text.splitlines():
                if not line.strip():
                    continue
                try:
                    batch.append(json.loads(line))
                except Exception:
                    batch.append({"raw": line})
    return batch
