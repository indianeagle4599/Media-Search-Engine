"""Data access helpers for the Streamlit UI."""

import os
from pathlib import Path

import streamlit as st

from ui.config import UPLOAD_ROOT


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@st.cache_resource(show_spinner=False)
def get_mongo_database():
    import pymongo

    client = pymongo.MongoClient(get_required_env("MONGO_URL"))
    return client[get_required_env("MONGO_DB_NAME")]


def get_mongo_collection():
    return get_mongo_database()[get_required_env("MONGO_COLLECTION_NAME")]


@st.cache_resource(show_spinner=False)
def get_search_history_collection():
    from ui.config import SEARCH_HISTORY_COLLECTION

    collection_name = os.getenv(
        "MEDIA_SEARCH_HISTORY_COLLECTION",
        SEARCH_HISTORY_COLLECTION,
    )
    collection = get_mongo_database()[collection_name]
    collection.create_index([("history_user", 1), ("created_at", -1)])
    collection.create_index([("history_user", 1), ("search_key", 1)], unique=True)
    return collection


@st.cache_resource(show_spinner=False)
def get_chroma_client():
    from utils.chroma import get_chroma_client as create_chroma_client

    return create_chroma_client(path=os.getenv("CHROMA_URL"))


def get_query_results(query: str, top_n: int) -> tuple[list[str], dict[str, list]]:
    from utils.chroma import query_all_collections

    normalized_query = query.strip().lower()
    ranked_queries = query_all_collections(
        chroma_client=get_chroma_client(),
        query_texts=[normalized_query],
        n_results=top_n,
    )
    result = ranked_queries.get(normalized_query) or {}
    return result.get("ids", []), result


def get_entries(entry_ids: list[str]) -> dict:
    if not entry_ids:
        return {}

    from utils.mongo import find_dict_objects

    return find_dict_objects(entry_ids, get_mongo_collection())


def get_upload_root() -> Path:
    return Path(os.getenv("MEDIA_UPLOAD_ROOT", UPLOAD_ROOT)).resolve()


def normalize_path(path: str | Path) -> str:
    return str(Path(path).resolve()).replace("\\", "/")


def entry_has_description(entry: dict) -> bool:
    return bool((entry.get("description") or {}).get("content"))


def get_entry_chroma_index_date(entry: dict) -> str:
    metadata = entry.get("metadata") or {}
    dates = metadata.get("dates") or {}
    value = dates.get("chroma_indexed_at")
    if value:
        return str(value)

    indexing = entry.get("indexing") or {}
    return str(indexing.get("chroma_indexed_at") or "")


def entry_has_chroma_index(entry: dict) -> bool:
    return bool(get_entry_chroma_index_date(entry))


def entry_is_fully_indexed(entry: dict) -> bool:
    return entry_has_description(entry) and entry_has_chroma_index(entry)


def get_entry_upload_date(entry: dict) -> str:
    metadata = entry.get("metadata") or {}
    dates = metadata.get("dates") or {}
    return str(metadata.get("uploaded_at") or dates.get("index_date") or "")


def get_entry_creation_date(entry: dict) -> str:
    dates = (entry.get("metadata") or {}).get("dates") or {}
    return str(dates.get("true_creation_date") or dates.get("master_date") or "")


def is_uploaded_entry(entry: dict) -> bool:
    file_path = str((entry.get("metadata") or {}).get("file_path") or "")
    if not file_path:
        return False
    return normalize_path(file_path).startswith(normalize_path(get_upload_root()))


def normalize_entry(entry: dict) -> dict:
    return {**entry, "_id": str(entry["_id"])}


def list_uploaded_entries() -> list[dict]:
    return [
        normalize_entry(entry)
        for entry in get_mongo_collection().find({})
        if is_uploaded_entry(entry)
    ]


def dedupe_entries_by_hash(entries: list[dict]) -> list[dict]:
    chosen: dict[str, dict] = {}
    for entry in entries:
        file_hash = str((entry.get("metadata") or {}).get("file_hash") or "")
        if not file_hash:
            continue

        current = chosen.get(file_hash)
        if current is None:
            chosen[file_hash] = entry
            continue

        current_date = get_entry_upload_date(current)
        candidate_date = get_entry_upload_date(entry)
        if candidate_date > current_date:
            chosen[file_hash] = entry
            continue
        if candidate_date == current_date:
            if entry_is_fully_indexed(entry) and not entry_is_fully_indexed(current):
                chosen[file_hash] = entry
                continue
            if entry_has_description(entry) and not entry_has_description(current):
                chosen[file_hash] = entry

    return list(chosen.values())


def get_uploaded_entry_by_hash(file_hash: str) -> dict | None:
    matches = [
        normalize_entry(entry)
        for entry in get_mongo_collection().find({"metadata.file_hash": file_hash})
    ]
    deduped = dedupe_entries_by_hash(matches)
    return deduped[0] if deduped else None
