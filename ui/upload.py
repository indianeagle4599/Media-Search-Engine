"""Upload page for metadata-first media storage and pending analysis."""

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

from ui.config import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS
from ui.data import (
    dedupe_entries_by_hash,
    entry_has_description,
    get_chroma_client,
    get_entry_creation_date,
    get_entry_upload_date,
    get_mongo_collection,
    get_upload_root,
    get_uploaded_entry_by_hash,
    list_uploaded_entries,
    normalize_path,
)
from utils.chroma import delete_entry_ids
from utils.ingest import IngestConfig, entry_id_for_file, ingest_files


DEFAULT_API_NAME = "gemini"
DEFAULT_MODEL_NAME = "gemini-2.5-flash-lite"
ACTION_IGNORE = "ignore"
ACTION_REUPLOAD = "reupload"
STATUS_STORED = "stored"
STATUS_REUPLOADED = "re_uploaded"
STATUS_IGNORED = "ignored"
STATUS_INDEXED = "indexed"
STATUS_RATE_LIMITED = "rate_limited"
STATUS_FAILED = "failed"
RESULT_STATUS_LABELS = {
    STATUS_STORED: "stored",
    STATUS_REUPLOADED: "re-uploaded",
    STATUS_IGNORED: "ignored",
    STATUS_INDEXED: "indexed",
    STATUS_RATE_LIMITED: "rate limited",
    STATUS_FAILED: "failed",
}
DEFAULT_DUPLICATE_REASON = "Duplicate already exists. Ignored by default."


def clean_filename(name: str) -> str:
    return Path((name or "upload").replace("\\", "/")).name


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def upload_storage_path(file_hash: str, ext: str) -> Path:
    root = get_upload_root()
    root.mkdir(parents=True, exist_ok=True)

    existing = sorted(root.glob(f"{file_hash}.*"))
    if existing:
        return existing[0].resolve()

    stored_name = f"{file_hash}.{ext}" if ext else file_hash
    return (root / stored_name).resolve()


def ensure_stored_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(payload)


def build_ingest_config(run_analysis: bool) -> IngestConfig:
    genai_client = None
    if run_analysis:
        api_key = os.getenv("GEM_API_KEY")
        if not api_key:
            raise RuntimeError("Missing required environment variable: GEM_API_KEY")

        from google import genai

        genai_client = genai.Client(api_key=api_key)

    return IngestConfig(
        api_name=os.getenv("MEDIA_API_NAME", DEFAULT_API_NAME),
        model_name=os.getenv("MEDIA_MODEL_NAME", DEFAULT_MODEL_NAME),
        mongo_collection=get_mongo_collection(),
        chroma_client=get_chroma_client() if run_analysis else None,
        genai_client=genai_client,
        update_existing_metadata=False,
    )


def metadata_override(original_filename: str, uploaded_at: datetime) -> dict:
    return {
        "file_name": clean_filename(original_filename),
        "uploaded_at": uploaded_at.astimezone().isoformat(),
    }


def classify_uploaded_files(uploaded_files) -> tuple[list[dict], int]:
    selections = []
    duplicate_in_selection_count = 0
    seen_hashes: set[str] = set()

    for uploaded_file in uploaded_files or []:
        payload = uploaded_file.getvalue()
        if not payload:
            continue

        file_hash = hash_bytes(payload)
        if file_hash in seen_hashes:
            duplicate_in_selection_count += 1
            continue

        seen_hashes.add(file_hash)
        original_filename = clean_filename(uploaded_file.name)
        existing_entry = get_uploaded_entry_by_hash(file_hash)
        selections.append(
            {
                "file_hash": file_hash,
                "payload": payload,
                "original_filename": original_filename,
                "existing_entry": existing_entry,
                "default_action": (
                    ACTION_IGNORE if existing_entry else ""
                ),
            }
        )

    return selections, duplicate_in_selection_count


def selection_action(selection: dict, action_overrides: dict[str, str] | None = None) -> str:
    if not selection.get("existing_entry"):
        return ""
    overrides = action_overrides or {}
    return overrides.get(selection["file_hash"], selection["default_action"])


def delete_existing_upload(file_hash: str) -> list[str]:
    collection = get_mongo_collection()
    entry_ids = [
        str(document["_id"])
        for document in collection.find({"metadata.file_hash": file_hash})
    ]
    if not entry_ids:
        return []

    delete_entry_ids(get_chroma_client(), entry_ids)
    collection.delete_many({"_id": {"$in": entry_ids}})
    return entry_ids


def store_selected_uploads(
    selections: list[dict],
    action_overrides: dict[str, str] | None = None,
    seen_at: datetime | None = None,
) -> list[dict]:
    seen_at = seen_at or datetime.now(timezone.utc)
    config = build_ingest_config(run_analysis=False)
    file_paths = []
    overrides: dict[str, dict] = {}
    stored_items = []
    results = []

    for selection in selections:
        file_hash = selection["file_hash"]
        action = selection_action(selection, action_overrides)
        existing_entry = selection.get("existing_entry")

        if existing_entry and action != ACTION_REUPLOAD:
            results.append(
                {
                    "filename": selection["original_filename"],
                    "file_hash": file_hash,
                    "status": STATUS_IGNORED,
                    "reason": DEFAULT_DUPLICATE_REASON,
                    "entry_id": str(existing_entry.get("_id") or ""),
                }
            )
            continue

        if existing_entry:
            delete_existing_upload(file_hash)

        ext = Path(selection["original_filename"]).suffix.lstrip(".").lower()
        stored_path = upload_storage_path(file_hash, ext)
        ensure_stored_file(stored_path, selection["payload"])
        normalized_path = normalize_path(stored_path)
        file_paths.append(normalized_path)
        overrides[normalized_path] = metadata_override(
            selection["original_filename"], seen_at
        )
        stored_items.append(
            {
                "file_hash": file_hash,
                "filename": selection["original_filename"],
                "status": STATUS_REUPLOADED if existing_entry else STATUS_STORED,
            }
        )

    if not file_paths:
        return results

    ingest_files(
        file_paths=file_paths,
        config=config,
        metadata_overrides=overrides,
    )
    for item in stored_items:
        results.append(
            {
                "filename": item["filename"],
                "file_hash": item["file_hash"],
                "status": item["status"],
                "reason": "",
                "entry_id": entry_id_for_file(item["file_hash"], config),
            }
        )
    return results


def pending_upload_entries() -> list[dict]:
    entries = [
        entry
        for entry in dedupe_entries_by_hash(list_uploaded_entries())
        if not entry_has_description(entry)
    ]
    return sorted(entries, key=lambda entry: get_entry_upload_date(entry))


def analysis_overrides(entries: list[dict]) -> dict[str, dict]:
    overrides = {}
    for entry in entries:
        metadata = entry.get("metadata") or {}
        file_path = str(metadata.get("file_path") or "")
        if not file_path:
            continue

        override = {}
        if metadata.get("file_name"):
            override["file_name"] = metadata["file_name"]
        if metadata.get("uploaded_at"):
            override["uploaded_at"] = metadata["uploaded_at"]
        overrides[file_path] = override
    return overrides


def analyze_pending_uploads() -> list[dict]:
    pending_entries = pending_upload_entries()
    if not pending_entries:
        return []

    config = build_ingest_config(run_analysis=True)
    result = ingest_files(
        file_paths=[
            str((entry.get("metadata") or {}).get("file_path") or "")
            for entry in pending_entries
        ],
        config=config,
        metadata_overrides=analysis_overrides(pending_entries),
    )

    rows = []
    indexed_keys = set(result.chroma_indexed_keys)
    rate_limited_keys = set(result.rate_limited_keys)
    failed_keys = set(result.failed_keys)

    for entry in pending_entries:
        entry_id = str(entry["_id"])
        reason = ""
        status = STATUS_FAILED
        if entry_id in indexed_keys:
            status = STATUS_INDEXED
        elif entry_id in rate_limited_keys:
            status = STATUS_RATE_LIMITED
            reason = (
                result.error_details.get(entry_id, {}).get("reason")
                or "Gemini quota reached while generating descriptions."
            )
        elif entry_id in failed_keys:
            reason = (
                result.error_details.get(entry_id, {}).get("reason")
                or "Analysis failed."
            )
        else:
            reason = "Analysis did not complete."

        rows.append(
            {
                "filename": (entry.get("metadata") or {}).get("file_name") or entry_id,
                "file_hash": (entry.get("metadata") or {}).get("file_hash") or "",
                "status": status,
                "reason": reason,
                "entry_id": entry_id,
            }
        )

    return rows


def selection_rows(
    selections: list[dict],
    action_overrides: dict[str, str] | None = None,
) -> list[dict]:
    rows = []
    for selection in selections:
        existing_entry = selection.get("existing_entry")
        action = selection_action(selection, action_overrides)
        rows.append(
            {
                "filename": selection["original_filename"],
                "status": "duplicate" if existing_entry else "new",
                "action": action or "store",
                "existing_entry_id": str(existing_entry.get("_id") or "")
                if existing_entry
                else "",
            }
        )
    return rows


def results_table(rows: list[dict]) -> list[dict]:
    return [
        {
            "filename": row.get("filename", ""),
            "status": RESULT_STATUS_LABELS.get(row.get("status"), row.get("status", "")),
            "reason": row.get("reason", ""),
            "entry_id": row.get("entry_id", ""),
        }
        for row in rows
    ]


def pending_table(entries: list[dict]) -> list[dict]:
    rows = []
    for entry in entries:
        metadata = entry.get("metadata") or {}
        rows.append(
            {
                "filename": metadata.get("file_name") or str(entry.get("_id") or ""),
                "uploaded_at": get_entry_upload_date(entry)[:19].replace("T", " "),
                "creation_date": get_entry_creation_date(entry)[:10],
                "entry_id": str(entry.get("_id") or ""),
            }
        )
    return rows


def render_duplicate_controls(
    selections: list[dict],
    action_overrides: dict[str, str],
) -> None:
    duplicates = [selection for selection in selections if selection.get("existing_entry")]
    if not duplicates:
        return

    st.markdown("**Duplicate handling**")
    st.caption(
        "Duplicates are ignored by default. Choose Re-upload only when you want to replace the old searchable record with a fresh metadata-first upload."
    )
    for selection in duplicates:
        file_hash = selection["file_hash"]
        current_action = action_overrides.get(file_hash, ACTION_IGNORE)
        label = st.selectbox(
            selection["original_filename"],
            ["Ignore duplicate", "Re-upload"],
            index=0 if current_action == ACTION_IGNORE else 1,
            key=f"duplicate_action_{file_hash}",
        )
        action_overrides[file_hash] = (
            ACTION_REUPLOAD if label == "Re-upload" else ACTION_IGNORE
        )


def render_upload_page() -> None:
    st.subheader("Upload Media")
    st.caption(
        "Store uploads into `image_data` immediately, then analyze pending items when VLM capacity is available."
    )

    accepted_types = sorted(IMAGE_EXTENSIONS | VIDEO_EXTENSIONS)
    uploaded_files = st.file_uploader(
        "Choose media files",
        type=accepted_types,
        accept_multiple_files=True,
    )
    selections, duplicate_in_selection_count = classify_uploaded_files(uploaded_files)
    action_overrides = st.session_state.setdefault("upload_duplicate_actions", {})
    pending_entries = pending_upload_entries()

    if selections:
        render_duplicate_controls(selections, action_overrides)
        st.dataframe(
            selection_rows(selections, action_overrides),
            hide_index=True,
        )
        if duplicate_in_selection_count:
            st.caption(
                f"Ignoring {duplicate_in_selection_count} repeated file(s) in the current selection."
            )

    store_col, analyze_col = st.columns(2)
    with store_col:
        if st.button(
            "Store uploads",
            disabled=not selections,
        ):
            try:
                with st.spinner("Storing files and indexing metadata..."):
                    results = store_selected_uploads(
                        selections=selections,
                        action_overrides=action_overrides,
                    )
                st.session_state["upload_store_results"] = results
                stored_count = sum(
                    1
                    for row in results
                    if row["status"] in {STATUS_STORED, STATUS_REUPLOADED}
                )
                ignored_count = sum(1 for row in results if row["status"] == STATUS_IGNORED)
                message = f"Stored {stored_count} file(s)."
                if ignored_count:
                    message += f" Ignored {ignored_count} duplicate file(s)."
                st.success(message)
            except Exception as exc:
                st.error("Storing uploads failed.")
                st.exception(exc)

    with analyze_col:
        if st.button(
            "Analyze pending",
            disabled=not pending_entries,
            type="primary",
        ):
            try:
                with st.spinner("Analyzing pending uploads..."):
                    results = analyze_pending_uploads()
                st.session_state["upload_analysis_results"] = results
                indexed_count = sum(1 for row in results if row["status"] == STATUS_INDEXED)
                if indexed_count:
                    st.success(f"Indexed {indexed_count} file(s).")
                else:
                    st.warning("No pending files were fully indexed.")
            except Exception as exc:
                st.error("Pending upload analysis failed.")
                st.exception(exc)

    stored_results = st.session_state.setdefault("upload_store_results", [])
    if stored_results:
        st.markdown("**Store results**")
        st.dataframe(results_table(stored_results), hide_index=True)

    latest_pending = pending_upload_entries()
    st.markdown("**Pending analysis**")
    if latest_pending:
        st.caption(
            f"{len(latest_pending)} uploaded file(s) are stored in MongoDB but still missing generated descriptions."
        )
        st.dataframe(
            pending_table(latest_pending),
            hide_index=True,
        )
    else:
        st.info("No pending uploads.")

    analysis_results = st.session_state.setdefault("upload_analysis_results", [])
    if analysis_results:
        st.markdown("**Latest analysis results**")
        st.dataframe(
            results_table(analysis_results),
            hide_index=True,
        )
