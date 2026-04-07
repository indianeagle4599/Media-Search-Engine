"""Upload page and local upload storage helpers."""

import hashlib
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.config import IMAGE_EXTENSIONS, UPLOAD_ROOT, VIDEO_EXTENSIONS
from ui.data import get_chroma_client, get_mongo_collection, get_upload_collection
from utils.ingest import IngestConfig, entry_id_for_file, ingest_files


DEFAULT_API_NAME = "gemini"
DEFAULT_MODEL_NAME = "gemini-2.5-flash-lite"


def upload_root() -> Path:
    return Path(os.getenv("MEDIA_UPLOAD_ROOT", UPLOAD_ROOT))


def clean_filename(name: str) -> str:
    return Path((name or "upload").replace("\\", "/")).name


def upload_batch_id() -> str:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def save_uploaded_files(uploaded_files) -> tuple[str, list[dict]]:
    batch_id = upload_batch_id()
    day = datetime.now().astimezone().date().isoformat()
    destination_dir = upload_root() / day
    destination_dir.mkdir(parents=True, exist_ok=True)

    saved_records = []
    upload_collection = get_upload_collection()
    now = datetime.now(timezone.utc)

    for uploaded_file in uploaded_files:
        payload = uploaded_file.getvalue()
        if not payload:
            continue

        original_filename = clean_filename(uploaded_file.name)
        ext = Path(original_filename).suffix.lstrip(".").lower()
        file_hash = hash_bytes(payload)
        stored_filename = f"{file_hash}.{ext}" if ext else file_hash
        stored_path = (destination_dir / stored_filename).resolve()
        if not stored_path.exists():
            stored_path.write_bytes(payload)

        record = {
            "_id": f"{batch_id}_{file_hash}_{uuid.uuid4().hex[:8]}",
            "batch_id": batch_id,
            "created_at": now,
            "status": "uploaded",
            "file_hash": file_hash,
            "original_filename": original_filename,
            "stored_filename": stored_filename,
            "stored_path": str(stored_path).replace("\\", "/"),
            "upload_day": day,
            "ext": ext,
            "size_bytes": len(payload),
        }
        upload_collection.update_one(
            {"_id": record["_id"]},
            {"$set": record},
            upsert=True,
        )
        saved_records.append(record)

    return batch_id, saved_records


def get_batch_records(batch_id: str) -> list[dict]:
    records = list(
        get_upload_collection()
        .find({"batch_id": batch_id})
        .sort("created_at", 1)
    )
    for record in records:
        record["_id"] = str(record["_id"])
    return records


def get_recent_uploads(limit: int = 50) -> list[dict]:
    records = list(
        get_upload_collection()
        .find({})
        .sort("created_at", -1)
        .limit(limit)
    )
    for record in records:
        record["_id"] = str(record["_id"])
    return records


def build_ingest_config() -> IngestConfig:
    api_key = os.getenv("GEM_API_KEY")
    if not api_key:
        raise RuntimeError("Missing required environment variable: GEM_API_KEY")

    from google import genai

    return IngestConfig(
        api_name=os.getenv("MEDIA_API_NAME", DEFAULT_API_NAME),
        model_name=os.getenv("MEDIA_MODEL_NAME", DEFAULT_MODEL_NAME),
        mongo_collection=get_mongo_collection(),
        chroma_client=get_chroma_client(),
        genai_client=genai.Client(api_key=api_key),
        update_existing_metadata=False,
    )


def ingest_upload_batch(batch_id: str):
    records = [
        record
        for record in get_batch_records(batch_id)
        if record.get("status") not in {"duplicate_existing", "chroma_indexed"}
    ]
    if not records:
        return None

    config = build_ingest_config()
    file_paths = [record["stored_path"] for record in records]
    metadata_overrides = {
        record["stored_path"]: {
            "file_name": record["original_filename"],
            "upload": {
                "upload_record_id": record["_id"],
                "batch_id": record["batch_id"],
                "stored_filename": record["stored_filename"],
                "original_filename": record["original_filename"],
            },
        }
        for record in records
    }

    result = ingest_files(
        file_paths=file_paths,
        config=config,
        metadata_overrides=metadata_overrides,
    )
    update_upload_statuses(records=records, result=result, config=config)
    return result


def update_upload_statuses(records: list[dict], result, config: IngestConfig) -> None:
    upload_collection = get_upload_collection()
    now = datetime.now(timezone.utc)
    duplicate_keys = set(result.duplicate_existing_keys)
    chroma_keys = set(result.chroma_indexed_keys)
    populated_keys = set(result.populated_keys)
    metadata_keys = set(result.metadata_updated_keys).union(result.missing_keys)

    for record in records:
        entry_id = entry_id_for_file(record["file_hash"], config)
        status = "failed"
        if entry_id in duplicate_keys:
            status = "duplicate_existing"
        elif entry_id in chroma_keys:
            status = "chroma_indexed"
        elif entry_id in populated_keys:
            status = "described"
        elif entry_id in metadata_keys:
            status = "metadata_indexed"

        upload_collection.update_one(
            {"_id": record["_id"]},
            {
                "$set": {
                    "status": status,
                    "media_entry_id": entry_id,
                    "indexed_at": now,
                }
            },
        )


def records_table(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for record in records:
        rows.append(
            {
                "status": record.get("status"),
                "original_filename": record.get("original_filename"),
                "stored_path": record.get("stored_path"),
                "media_entry_id": record.get("media_entry_id", ""),
            }
        )
    return pd.DataFrame(rows)


def render_upload_page() -> None:
    st.subheader("Upload Media")
    st.caption(
        "Uploads are saved locally first. Use Index uploaded files to add the "
        "current batch to MongoDB and ChromaDB."
    )

    accepted_types = sorted(IMAGE_EXTENSIONS | VIDEO_EXTENSIONS)
    uploaded_files = st.file_uploader(
        "Choose media files",
        type=accepted_types,
        accept_multiple_files=True,
    )

    save_col, index_col = st.columns(2)
    with save_col:
        if st.button(
            "Save uploads",
            disabled=not uploaded_files,
            use_container_width=True,
        ):
            batch_id, saved_records = save_uploaded_files(uploaded_files)
            st.session_state["last_upload_batch_id"] = batch_id
            st.success(f"Saved {len(saved_records)} file(s).")

    current_batch_id = st.session_state.get("last_upload_batch_id")
    with index_col:
        if st.button(
            "Index uploaded files",
            disabled=not current_batch_id,
            use_container_width=True,
            type="primary",
        ):
            try:
                with st.spinner("Indexing uploaded files..."):
                    ingest_upload_batch(current_batch_id)
                st.success("Upload batch indexing completed.")
            except Exception as exc:
                st.error("Upload indexing failed.")
                st.exception(exc)

    if current_batch_id:
        st.markdown(f"**Current batch:** `{current_batch_id}`")
        st.dataframe(
            records_table(get_batch_records(current_batch_id)),
            use_container_width=True,
            hide_index=True,
        )

    recent = get_recent_uploads()
    if recent:
        with st.expander("Recent uploads", expanded=False):
            st.dataframe(
                records_table(recent),
                use_container_width=True,
                hide_index=True,
            )
