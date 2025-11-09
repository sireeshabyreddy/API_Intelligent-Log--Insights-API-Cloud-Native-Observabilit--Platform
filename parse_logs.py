import json
import os
import hashlib
from typing import List, Dict, Set, Any
from config import DEDUP_CACHE_FOLDER

os.makedirs(DEDUP_CACHE_FOLDER, exist_ok=True)


def load_dedup_cache(log_type: str) -> Set[str]:
    """Load deduplication cache for a given log type."""
    file_path = os.path.join(DEDUP_CACHE_FOLDER, f"dedup_state_{log_type}.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_dedup_cache(log_type: str, hashes: Set[str]) -> None:
    """Save deduplication cache for a given log type."""
    file_path = os.path.join(DEDUP_CACHE_FOLDER, f"dedup_state_{log_type}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(list(hashes), f)
    except Exception as e:
        print(f"⚠️ Failed to save dedup cache: {e}")


def deduplicate_logs(raw_batch: List[Dict[str, Any]], log_type: str) -> List[Dict[str, Any]]:
    """
    Deduplicate logs based on SHA256 hash of JSON string.
    Returns list of unique logs.
    """
    sent_hashes = load_dedup_cache(log_type)
    unique_batch = []
    new_hashes = set()

    for log in raw_batch:
        log_str = json.dumps(log, sort_keys=True)
        log_hash = hashlib.sha256(log_str.encode('utf-8')).hexdigest()
        if log_hash not in sent_hashes:
            unique_batch.append(log)
            new_hashes.add(log_hash)

    save_dedup_cache(log_type, sent_hashes | new_hashes)
    return unique_batch
