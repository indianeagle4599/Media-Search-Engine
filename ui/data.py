"""
data.py

Streamlit-facing data access helpers for MongoDB, ChromaDB, and uploaded media.
"""

import os
from pathlib import Path

import streamlit as st

from ui.config import UPLOAD_ROOT
from utils.chroma import (
    delete_entry_ids,
    get_chroma_client as create_chroma_client,
    query_collections,
)
from utils.mongo import (
    delete_uploaded_documents_by_hash,
    find_dict_objects,
    get_mongo_collection,
    rename_uploaded_documents_by_hash,
)
from utils.retrieval import SearchManifest


def streamlit_runtime_active() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


@st.cache_resource(show_spinner=False)
def get_chroma_client():
    return create_chroma_client(path=os.getenv("CHROMA_URL"))


def clear_chroma_client_cache() -> None:
    get_chroma_client.clear()


def get_query_results(
    query: str,
    top_n: int,
    search_options: dict | None = None,
    include_debug: bool = False,
) -> tuple[list[str], dict[str, list]]:
    normalized_query = query.strip().lower()
    try:
        ranked_queries = query_collections(
            chroma_client=get_chroma_client(),
            query_texts=[normalized_query],
            n_results=top_n,
            search_options=search_options,
            include_debug=include_debug,
        )
    except Exception:
        clear_chroma_client_cache()
        ranked_queries = query_collections(
            chroma_client=get_chroma_client(),
            query_texts=[normalized_query],
            n_results=top_n,
            search_options=search_options,
            include_debug=include_debug,
        )
    result = ranked_queries.get(normalized_query) or {}
    return result.get("ids", []), result


def manifest_source_options() -> list[tuple[str, str]]:
    return [
        (source_id, config.get("label") or source_id)
        for source_id, config in SearchManifest.SOURCES.items()
        if config.get("advanced_exposure", True)
    ]


def get_entries(entry_ids: list[str]) -> dict:
    if not entry_ids:
        return {}
    return find_dict_objects(entry_ids, get_mongo_collection())


def get_upload_root() -> Path:
    return Path(os.getenv("MEDIA_UPLOAD_ROOT", UPLOAD_ROOT)).resolve()


def normalize_path(path: str | Path) -> str:
    return str(Path(path).resolve()).replace("\\", "/")


def uploaded_entry_file_hash(entry: dict) -> str:
    return str((entry.get("metadata") or {}).get("file_hash") or "")


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


def dedupe_entries_by_hash(entries: list[dict]) -> list[dict]:
    chosen: dict[str, dict] = {}
    for entry in entries:
        file_hash = str((entry.get("metadata") or {}).get("file_hash") or "")
        if not file_hash:
            chosen[str(entry.get("_id") or "")] = entry
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


def _get_uploaded_entries_snapshot_impl() -> dict[str, object]:
    entries = [
        normalize_entry(entry)
        for entry in get_mongo_collection().find({})
        if is_uploaded_entry(entry)
    ]
    deduped_entries = dedupe_entries_by_hash(entries)
    return {
        "entries": deduped_entries,
        "by_hash": {
            str((entry.get("metadata") or {}).get("file_hash") or ""): entry
            for entry in deduped_entries
            if str((entry.get("metadata") or {}).get("file_hash") or "")
        },
    }


@st.cache_data(show_spinner=False, ttl=5, max_entries=1)
def _get_uploaded_entries_snapshot_cached() -> dict[str, object]:
    return _get_uploaded_entries_snapshot_impl()


def get_uploaded_entries_snapshot() -> dict[str, object]:
    if streamlit_runtime_active():
        return _get_uploaded_entries_snapshot_cached()
    return _get_uploaded_entries_snapshot_impl()


def _get_gallery_entries_snapshot_impl() -> list[dict]:
    return [
        normalize_entry(entry)
        for entry in get_mongo_collection().find({})
        if str((entry.get("metadata") or {}).get("file_path") or "")
    ]


@st.cache_data(show_spinner=False, ttl=5, max_entries=1)
def _get_gallery_entries_snapshot_cached() -> list[dict]:
    return _get_gallery_entries_snapshot_impl()


def get_gallery_entries_snapshot() -> list[dict]:
    if streamlit_runtime_active():
        return _get_gallery_entries_snapshot_cached()
    return _get_gallery_entries_snapshot_impl()


def clear_uploaded_entries_cache() -> None:
    _get_uploaded_entries_snapshot_cached.clear()
    _get_gallery_entries_snapshot_cached.clear()


def list_uploaded_entries() -> list[dict]:
    return list(get_uploaded_entries_snapshot().get("entries", []))


def list_gallery_entries() -> list[dict]:
    return list(get_gallery_entries_snapshot())


def get_uploaded_entry_by_hash(file_hash: str) -> dict | None:
    return get_uploaded_entries_snapshot().get("by_hash", {}).get(str(file_hash) or "")


def rename_uploaded_entry(file_hash: str, file_name: str) -> tuple[list[str], str]:
    cleaned_hash = str(file_hash or "").strip()
    cleaned_name = Path((file_name or "").replace("\\", "/")).name.strip()
    if not cleaned_hash:
        raise ValueError("Uploaded file hash is missing.")
    if not cleaned_name:
        raise ValueError("File name cannot be empty.")

    entry_ids = rename_uploaded_documents_by_hash(cleaned_hash, cleaned_name)
    clear_uploaded_entries_cache()
    return entry_ids, cleaned_name


def delete_uploaded_entry(file_hash: str, *, clear_cache: bool = True) -> list[str]:
    cleaned_hash = str(file_hash or "").strip()
    if not cleaned_hash:
        return []

    entry_ids, file_paths = delete_uploaded_documents_by_hash(cleaned_hash)
    if not entry_ids:
        return []

    try:
        clear_chroma_client_cache()
        delete_entry_ids(get_chroma_client(), entry_ids)

        upload_root = normalize_path(get_upload_root())
        upload_root_path = get_upload_root()
        for file_path in file_paths:
            normalized_path = normalize_path(file_path)
            if not normalized_path.startswith(upload_root):
                continue

            path = Path(file_path)
            if path.is_file():
                path.unlink()

            parent = path.parent
            while parent != upload_root_path and parent.is_dir():
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
    finally:
        clear_chroma_client_cache()
        if clear_cache:
            clear_uploaded_entries_cache()
    return entry_ids
