from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
import numpy as np
from config import SEARCH_SERVICE_NAME, SEARCH_API_KEY, SEARCH_INDEX_NAME
from .embedding_service import generate_embedding

# Initialize Azure Search client
search_client = SearchClient(
    endpoint=f"https://{SEARCH_SERVICE_NAME}.search.windows.net",
    index_name=SEARCH_INDEX_NAME,
    credential=AzureKeyCredential(SEARCH_API_KEY)
)

def cosine_similarity(vec1, vec2):
    vec1, vec2 = np.array(vec1), np.array(vec2)
    if vec1.shape != vec2.shape or np.linalg.norm(vec1) == 0 or np.linalg.norm(vec2) == 0:
        return 0.0
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

def semantic_search_logs(query: str, top_k: int = 5):
    vector = generate_embedding(query)
    if not vector:
        return []

    # Fetch documents from Azure Search
    documents = search_client.search(search_text="*", top=1000)

    results = []
    for doc in documents:
        doc_vector = doc.get("log_vector")
        if doc_vector:
            similarity = cosine_similarity(vector, doc_vector)
            results.append({
                "id": doc.get("id"),
                "message": doc.get("message"),
                "service": doc.get("service"),
                "level": doc.get("level"),
                "similarity": similarity
            })

    # Sort by similarity
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]
