"""
upload.py

Upload page for storing media, describing stored items, and indexing described items.
"""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

from ui.config import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS
from ui.data import (
    clear_chroma_client_cache,
    clear_uploaded_entries_cache,
    delete_uploaded_entry,
    get_chroma_client,
    get_entry_creation_date,
    get_known_entry_by_hash,
    get_entry_processing_status,
    get_entry_upload_date,
    get_upload_root,
    list_gallery_entries,
    normalize_path,
)
from utils.mongo import get_mongo_collection
from utils.ingest import (
    DEFAULT_DESCRIPTION_RIGOR,
    STATUS_DESCRIBED,
    STATUS_INDEXED,
    STATUS_STORED,
    build_ingest_config_from_env,
    entry_id_for_file,
    ingest_files,
)

DEFAULT_UPLOAD_DESCRIPTION_RIGOR = "very low"
ACTION_IGNORE = "ignore"
ACTION_REUPLOAD = "reupload"
STATUS_REUPLOADED = "re_uploaded"
STATUS_IGNORED = "ignored"
STATUS_RATE_LIMITED = "rate_limited"
STATUS_FAILED = "failed"
RESULT_STATUS_LABELS = {
    STATUS_STORED: "stored",
    STATUS_DESCRIBED: "described",
    STATUS_INDEXED: "indexed",
    STATUS_REUPLOADED: "re-uploaded",
    STATUS_IGNORED: "ignored",
    STATUS_RATE_LIMITED: "rate limited",
    STATUS_FAILED: "failed",
}
DEFAULT_DUPLICATE_REASON = "Duplicate already exists. Ignored by default."


def clean_filename(name: str) -> str:
    return Path((name or "upload").replace("\\", "/")).name


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def upload_folder_name(uploaded_at: datetime) -> str:
    return uploaded_at.astimezone().strftime("%Y%m%d")


def upload_storage_path(file_hash: str, ext: str, uploaded_at: datetime) -> Path:
    root = get_upload_root()
    root.mkdir(parents=True, exist_ok=True)

    existing = sorted(root.rglob(f"{file_hash}.*"))
    if existing:
        return existing[0].resolve()

    dated_root = root / upload_folder_name(uploaded_at)
    stored_name = f"{file_hash}.{ext}" if ext else file_hash
    return (dated_root / stored_name).resolve()


def ensure_stored_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(payload)


def build_ingest_config(*, run_description: bool, run_indexing: bool):
    return build_ingest_config_from_env(
        mongo_collection=get_mongo_collection(),
        chroma_client=get_chroma_client() if run_indexing else None,
        update_existing_metadata=False,
        run_analysis=run_description,
        default_description_rigor=(
            DEFAULT_UPLOAD_DESCRIPTION_RIGOR
            if run_description
            else DEFAULT_DESCRIPTION_RIGOR
        ),
        require_api_key=run_description,
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
        existing_entry = get_known_entry_by_hash(file_hash)
        selections.append(
            {
                "file_hash": file_hash,
                "payload": payload,
                "original_filename": original_filename,
                "existing_entry": existing_entry,
                "default_action": (ACTION_IGNORE if existing_entry else ""),
            }
        )

    return selections, duplicate_in_selection_count


def selection_action(
    selection: dict, action_overrides: dict[str, str] | None = None
) -> str:
    if not selection.get("existing_entry"):
        return ""
    overrides = action_overrides or {}
    return overrides.get(selection["file_hash"], selection["default_action"])


def store_selected_uploads(
    selections: list[dict],
    action_overrides: dict[str, str] | None = None,
    seen_at: datetime | None = None,
) -> list[dict]:
    seen_at = seen_at or datetime.now(timezone.utc)
    config = build_ingest_config(run_description=False, run_indexing=False)
    file_paths = []
    overrides: dict[str, dict] = {}
    stored_items = []
    results = []
    mutated_existing_uploads = False

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
            delete_uploaded_entry(file_hash, clear_cache=False)
            mutated_existing_uploads = True

        ext = Path(selection["original_filename"]).suffix.lstrip(".").lower()
        stored_path = upload_storage_path(file_hash, ext, seen_at)
        ensure_stored_file(stored_path, selection["payload"])
        normalized_path = normalize_path(stored_path)
        print(
            "Stored upload:",
            f"{selection['original_filename']} -> {normalized_path}",
        )
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
        if mutated_existing_uploads:
            clear_uploaded_entries_cache()
        return results

    try:
        ingest_files(
            file_paths=file_paths,
            config=config,
            metadata_overrides=overrides,
        )
    except Exception:
        clear_uploaded_entries_cache()
        raise
    clear_uploaded_entries_cache()
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


def pending_upload_entries(
    entries: list[dict] | None = None,
    statuses: set[str] | None = None,
) -> list[dict]:
    source_entries = list_gallery_entries() if entries is None else entries
    entries = [
        entry
        for entry in source_entries
        if get_entry_processing_status(entry) != STATUS_INDEXED
    ]
    if statuses:
        entries = [
            entry for entry in entries if get_entry_processing_status(entry) in statuses
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


def describe_pending_uploads(
    pending_entries: list[dict],
    progress_callback=None,
) -> list[dict]:
    if not pending_entries:
        return []

    config = build_ingest_config(run_description=True, run_indexing=False)
    config.progress_callback = progress_callback
    try:
        result = ingest_files(
            file_paths=[
                str((entry.get("metadata") or {}).get("file_path") or "")
                for entry in pending_entries
            ],
            config=config,
            metadata_overrides=analysis_overrides(pending_entries),
        )
    finally:
        clear_chroma_client_cache()
        clear_uploaded_entries_cache()

    rows = []
    rate_limited_keys = set(result.rate_limited_keys)
    failed_keys = set(result.failed_keys)

    for entry in pending_entries:
        entry_id = str(entry["_id"])
        reason = ""
        result_entry = {"_id": entry_id, **(result.descriptions.get(entry_id) or {})}
        status = get_entry_processing_status(result_entry)
        if entry_id in rate_limited_keys:
            status = STATUS_RATE_LIMITED
            reason = (
                result.error_details.get(entry_id, {}).get("reason")
                or "Gemini quota reached while generating descriptions."
            )
        elif entry_id in failed_keys:
            status = STATUS_FAILED
            reason = (
                result.error_details.get(entry_id, {}).get("reason")
                or "Analysis failed."
            )

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


def index_described_uploads(pending_entries: list[dict]) -> list[dict]:
    if not pending_entries:
        return []

    config = build_ingest_config(run_description=False, run_indexing=True)
    try:
        result = ingest_files(
            file_paths=[
                str((entry.get("metadata") or {}).get("file_path") or "")
                for entry in pending_entries
            ],
            config=config,
            metadata_overrides=analysis_overrides(pending_entries),
        )
    finally:
        clear_uploaded_entries_cache()

    rows = []
    indexed_keys = set(result.chroma_indexed_keys)
    failed_keys = set(result.failed_keys)

    for entry in pending_entries:
        entry_id = str(entry["_id"])
        reason = ""
        status = STATUS_INDEXED if entry_id in indexed_keys else STATUS_DESCRIBED
        if entry_id in failed_keys:
            status = STATUS_FAILED
            reason = (
                result.error_details.get(entry_id, {}).get("reason")
                or "Indexing failed."
            )
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
                "file_hash": selection["file_hash"],
                "filename": selection["original_filename"],
                "status": "duplicate" if existing_entry else "new",
                "existing_entry_id": (
                    str(existing_entry.get("_id") or "") if existing_entry else ""
                ),
                "re_upload": bool(existing_entry and action == ACTION_REUPLOAD),
            }
        )
    return rows


def update_duplicate_actions_from_rows(
    edited_rows: list[dict],
    action_overrides: dict[str, str],
) -> None:
    duplicate_hashes = {
        str(row.get("file_hash") or "")
        for row in edited_rows or []
        if row.get("existing_entry_id")
    }
    for file_hash in duplicate_hashes:
        action_overrides.pop(file_hash, None)

    for row in edited_rows or []:
        file_hash = str(row.get("file_hash") or "")
        if not file_hash or not row.get("existing_entry_id"):
            continue
        action_overrides[file_hash] = (
            ACTION_REUPLOAD if bool(row.get("re_upload")) else ACTION_IGNORE
        )


def results_table(rows: list[dict]) -> list[dict]:
    return [
        {
            "filename": row.get("filename", ""),
            "status": RESULT_STATUS_LABELS.get(
                row.get("status"), row.get("status", "")
            ),
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
                "status": RESULT_STATUS_LABELS.get(
                    get_entry_processing_status(entry),
                    get_entry_processing_status(entry),
                ),
                "uploaded_at": get_entry_upload_date(entry)[:19].replace("T", " "),
                "creation_date": get_entry_creation_date(entry)[:10],
                "entry_id": str(entry.get("_id") or ""),
            }
        )
    return rows


def render_selection_table(
    selections: list[dict],
    action_overrides: dict[str, str],
) -> None:
    rows = selection_rows(selections, action_overrides)
    if not rows:
        return

    if any(row["status"] == "duplicate" for row in rows):
        st.markdown("**Upload selection**")
        st.caption(
            "Duplicates are ignored by default. Tick `re_upload` only when you want to replace the old searchable record with a fresh metadata-first upload."
        )
        duplicate_hashes = [
            str(row["file_hash"] or "")
            for row in rows
            if row["status"] == "duplicate" and row.get("file_hash")
        ]
        if duplicate_hashes and st.button(
            "Select all duplicates for re-upload",
            key="upload_select_all_duplicates",
        ):
            for file_hash in duplicate_hashes:
                action_overrides[file_hash] = ACTION_REUPLOAD
            st.rerun()
        rows = selection_rows(selections, action_overrides)
        edited_rows = st.data_editor(
            rows,
            hide_index=True,
            disabled=["filename", "status", "existing_entry_id"],
            column_order=["filename", "status", "existing_entry_id", "re_upload"],
            key="upload_selection_table",
        )
        if hasattr(edited_rows, "to_dict"):
            edited_rows = edited_rows.to_dict("records")
        update_duplicate_actions_from_rows(edited_rows, action_overrides)
        return

    st.dataframe(
        [{"filename": row["filename"], "status": row["status"]} for row in rows],
        hide_index=True,
    )


def render_upload_page() -> None:
    st.subheader("Upload Media")
    st.caption(
        "Store files first, then describe stored items, then index described items."
    )

    accepted_types = sorted(IMAGE_EXTENSIONS | VIDEO_EXTENSIONS)
    uploaded_files = st.file_uploader(
        "Choose media files",
        type=accepted_types,
        accept_multiple_files=True,
    )
    selections, duplicate_in_selection_count = classify_uploaded_files(uploaded_files)
    action_overrides = st.session_state.setdefault("upload_duplicate_actions", {})
    uploaded_entries = list_gallery_entries()
    pending_entries = pending_upload_entries(uploaded_entries)
    stored_entries = pending_upload_entries(uploaded_entries, {STATUS_STORED})
    described_entries = pending_upload_entries(uploaded_entries, {STATUS_DESCRIBED})
    indexed_count = sum(
        1
        for entry in uploaded_entries
        if get_entry_processing_status(entry) == STATUS_INDEXED
    )

    if selections:
        render_selection_table(selections, action_overrides)
        if duplicate_in_selection_count:
            st.caption(
                f"Ignoring {duplicate_in_selection_count} repeated file(s) in the current selection."
            )

    store_col, describe_col, index_col = st.columns(3)
    with store_col:
        if st.button(
            "Store uploads",
            disabled=not selections,
        ):
            try:
                with st.spinner("Storing files and metadata..."):
                    results = store_selected_uploads(
                        selections=selections,
                        action_overrides=action_overrides,
                    )
                uploaded_entries = list_gallery_entries()
                pending_entries = pending_upload_entries(uploaded_entries)
                stored_entries = pending_upload_entries(
                    uploaded_entries, {STATUS_STORED}
                )
                described_entries = pending_upload_entries(
                    uploaded_entries, {STATUS_DESCRIBED}
                )
                indexed_count = sum(
                    1
                    for entry in uploaded_entries
                    if get_entry_processing_status(entry) == STATUS_INDEXED
                )
                st.session_state["upload_store_results"] = results
                stored_count = sum(
                    1
                    for row in results
                    if row["status"] in {STATUS_STORED, STATUS_REUPLOADED}
                )
                ignored_count = sum(
                    1 for row in results if row["status"] == STATUS_IGNORED
                )
                message = f"Stored {stored_count} file(s)."
                if ignored_count:
                    message += f" Ignored {ignored_count} duplicate file(s)."
                st.success(message)
            except Exception as exc:
                st.error("Storing uploads failed.")
                st.exception(exc)

    with describe_col:
        if st.button(
            "Describe stored",
            disabled=not stored_entries,
            type="primary",
        ):
            progress_status = st.empty()

            def update_progress(payload: dict) -> None:
                if payload.get("stage") != "description_batch":
                    return
                total = int(payload.get("total", 0) or 0)
                completed = int(payload.get("completed", 0) or 0)
                remaining = int(payload.get("remaining", 0) or 0)
                batch_total = int(payload.get("batch_size", 0) or 0)
                batch_succeeded = int(payload.get("batch_succeeded", 0) or 0)
                progress_status.info(
                    f"{completed} of {total} image(s) described successfully. "
                    f"{remaining} remaining. Current batch: {batch_succeeded}/{batch_total}."
                )

            try:
                with st.spinner("Describing stored uploads..."):
                    results = describe_pending_uploads(
                        stored_entries,
                        progress_callback=update_progress,
                    )
                uploaded_entries = list_gallery_entries()
                pending_entries = pending_upload_entries(uploaded_entries)
                stored_entries = pending_upload_entries(
                    uploaded_entries, {STATUS_STORED}
                )
                described_entries = pending_upload_entries(
                    uploaded_entries, {STATUS_DESCRIBED}
                )
                indexed_count = sum(
                    1
                    for entry in uploaded_entries
                    if get_entry_processing_status(entry) == STATUS_INDEXED
                )
                progress_status.empty()
                st.session_state["upload_description_results"] = results
                described_count = sum(
                    1 for row in results if row["status"] == STATUS_DESCRIBED
                )
                if described_count:
                    st.success(f"Described {described_count} file(s).")
                else:
                    st.warning("No stored files were described.")
            except Exception as exc:
                progress_status.empty()
                st.error("Pending description failed.")
                st.exception(exc)

    with index_col:
        if st.button(
            "Index described",
            disabled=not described_entries,
        ):
            try:
                with st.spinner("Indexing described uploads..."):
                    results = index_described_uploads(described_entries)
                uploaded_entries = list_gallery_entries()
                pending_entries = pending_upload_entries(uploaded_entries)
                stored_entries = pending_upload_entries(
                    uploaded_entries, {STATUS_STORED}
                )
                described_entries = pending_upload_entries(
                    uploaded_entries, {STATUS_DESCRIBED}
                )
                indexed_count = sum(
                    1
                    for entry in uploaded_entries
                    if get_entry_processing_status(entry) == STATUS_INDEXED
                )
                st.session_state["upload_index_results"] = results
                indexed_count = sum(
                    1 for row in results if row["status"] == STATUS_INDEXED
                )
                if indexed_count:
                    st.success(f"Indexed {indexed_count} file(s).")
                else:
                    st.warning("No described files were indexed.")
            except Exception as exc:
                st.error("Pending indexing failed.")
                st.exception(exc)

    stored_results = st.session_state.setdefault("upload_store_results", [])
    if stored_results:
        st.markdown("**Store results**")
        st.dataframe(results_table(stored_results), hide_index=True)

    st.markdown("**Processing queue**")
    st.caption(
        f"{len(stored_entries)} stored, {len(described_entries)} described, {indexed_count} indexed."
    )
    if pending_entries:
        st.caption(
            f"{len(stored_entries)} stored, {len(described_entries)} described, {len(pending_entries)} total not yet indexed."
        )
        st.dataframe(
            pending_table(pending_entries),
            hide_index=True,
        )
    else:
        st.info("No pending uploads.")

    description_results = st.session_state.setdefault("upload_description_results", [])
    if description_results:
        st.markdown("**Latest description results**")
        st.dataframe(
            results_table(description_results),
            hide_index=True,
        )

    index_results = st.session_state.setdefault("upload_index_results", [])
    if index_results:
        st.markdown("**Latest indexing results**")
        st.dataframe(
            results_table(index_results),
            hide_index=True,
        )
