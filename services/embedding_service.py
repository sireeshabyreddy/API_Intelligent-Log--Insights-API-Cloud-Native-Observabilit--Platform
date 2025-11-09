import logging
from openai import AzureOpenAI
from config import AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, OPENAI_API_VERSION

embedding_model = "text-embedding-ada-002"

def generate_embedding(text: str):
    if not text:
        return []

    try:
        client = AzureOpenAI(
            api_key=AZURE_OPENAI_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=OPENAI_API_VERSION
        )

        response = client.embeddings.create(
            model=embedding_model,
            input=text
        )
        embedding = response.data[0].embedding
        return embedding

    except Exception as e:
        logging.error(f"Failed to generate embedding: {e}")
        return []
