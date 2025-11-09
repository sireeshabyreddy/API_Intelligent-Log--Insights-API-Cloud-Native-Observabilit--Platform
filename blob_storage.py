import asyncio
from azure.storage.blob.aio import BlobServiceClient
from config import storage_connection_string


async def upload_to_blob(file_name: str, content: str):
    blob_service_client = None  # define outside try
    try:
        # Create async blob service client
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client("raw-logs")

        # Create container if not exists
        try:
            await container_client.create_container()
        except Exception:
            pass  # container may already exist

        # Upload blob asynchronously
        blob_client = container_client.get_blob_client(file_name)
        await blob_client.upload_blob(content, overwrite=True)

        print(f"✅ Uploaded '{file_name}' to Blob Storage.")
        return blob_client.url

    except Exception as e:
        print(f"❌ Blob upload failed: {e}")
        return None
    finally:
        # Close async connection if it was created
        if blob_service_client:
            await blob_service_client.close()
