"""Uploaded media gallery page."""

import html
import random

import streamlit as st

from ui.components import (
    clear_selected_entry_id,
    detail_dialog,
    get_selected_entry_id,
    render_media_card,
)
from ui.config import IMAGE_EXTENSIONS
from ui.data import (
    entry_is_fully_indexed,
    get_entries,
    get_entry_creation_date,
    get_entry_upload_date,
    list_gallery_entries,
)
from ui.formatting import get_entry_display_fields


DEFAULT_SORT = "Upload date (newest first)"
SORT_OPTIONS = [
    DEFAULT_SORT,
    "Upload date (oldest first)",
    "Creation date (newest first)",
    "Creation date (oldest first)",
    "Filename (A-Z)",
    "Filename (Z-A)",
    "Random sample",
]


def build_gallery_record(entry: dict) -> dict:
    entry_id = str(entry["_id"])
    _, file_path, file_name, ext = get_entry_display_fields(entry_id, entry)
    return {
        "_id": entry_id,
        "file_path": file_path,
        "file_name": file_name,
        "ext": ext,
        "uploaded_at": get_entry_upload_date(entry),
        "creation_date": get_entry_creation_date(entry),
        "status": "indexed" if entry_is_fully_indexed(entry) else "pending_indexing",
    }


def sort_gallery_records(records: list[dict], sort_by: str) -> list[dict]:
    if sort_by == DEFAULT_SORT:
        return sorted(records, key=lambda record: record.get("uploaded_at") or "", reverse=True)
    if sort_by == "Upload date (oldest first)":
        return sorted(records, key=lambda record: record.get("uploaded_at") or "")
    if sort_by == "Creation date (newest first)":
        ordered = sorted(records, key=lambda record: record.get("creation_date") or "", reverse=True)
        return sorted(ordered, key=lambda record: record.get("creation_date") is None)
    if sort_by == "Creation date (oldest first)":
        ordered = sorted(records, key=lambda record: record.get("creation_date") or "")
        return sorted(ordered, key=lambda record: record.get("creation_date") is None)
    if sort_by == "Filename (A-Z)":
        return sorted(records, key=lambda record: str(record.get("file_name") or "").lower())
    if sort_by == "Filename (Z-A)":
        return sorted(records, key=lambda record: str(record.get("file_name") or "").lower(), reverse=True)
    if sort_by == "Random sample":
        return random.sample(records, k=len(records))
    return records


def get_gallery_records(limit: int = 48, sort_by: str = DEFAULT_SORT) -> list[dict]:
    records = [build_gallery_record(entry) for entry in list_gallery_entries()]
    records = [record for record in records if record.get("ext") in IMAGE_EXTENSIONS]
    records = sort_gallery_records(records, sort_by)
    if int(limit) <= 0:
        return records
    return records[: int(limit)]


def gallery_metadata_markup(record: dict) -> str:
    rows = [
        ("Status", str(record.get("status", "unknown")).replace("_", " ").title())
    ]
    uploaded_at = str(record.get("uploaded_at") or "")[:19].replace("T", " ")
    if uploaded_at:
        rows.append(("Uploaded", uploaded_at))
    creation_date = str(record.get("creation_date") or "")[:10]
    if creation_date:
        rows.append(("Created", creation_date))
    row_markup = "".join(
        '<div class="gallery-card__meta-row">'
        f'<span class="gallery-card__meta-label">{html.escape(label)}</span>'
        f'<span class="gallery-card__meta-value">{html.escape(value)}</span>'
        "</div>"
        for label, value in rows
    )
    return f'<div class="gallery-card__meta">{row_markup}</div>'


def render_gallery_grid(records: list[dict], columns: int = 4) -> None:
    if not records:
        st.info("No uploaded images found yet.")
        return

    for row_start in range(0, len(records), columns):
        row_columns = st.columns(columns, gap="medium")
        for offset, column in enumerate(row_columns):
            index = row_start + offset
            if index >= len(records):
                continue

            record = records[index]
            with column:
                render_media_card(
                    file_path=record.get("file_path", ""),
                    file_name=record.get("file_name", "Uploaded image"),
                    ext=record.get("ext", ""),
                    detail_entry_id=str(record["_id"]),
                    detail_title=record.get("file_name", "Uploaded image"),
                    overlay_details_html=gallery_metadata_markup(record),
                )


def render_gallery_detail() -> None:
    selected_entry_id = get_selected_entry_id()
    if not selected_entry_id:
        return

    entries = get_entries([selected_entry_id])
    entry = entries.get(selected_entry_id)
    if not entry:
        clear_selected_entry_id()
        return

    detail_dialog(
        entry_id=selected_entry_id,
        entry=entry,
        close_label="Back to gallery",
    )


def render_gallery_page() -> None:
    st.subheader("Gallery")
    st.caption(
        "Browse image entries stored in MongoDB. Files stay pending until both descriptions and Chroma indexing are complete."
    )

    sort_col, limit_col = st.columns([2, 1])
    with sort_col:
        sort_by = st.selectbox("Sort by", SORT_OPTIONS, index=0)
    all_records = get_gallery_records(limit=0, sort_by=sort_by)
    with limit_col:
        base_options = [24, 48, 96, 144, 200]
        limit_options = [option for option in base_options if option < len(all_records)]
        limit_options.append("All")
        limit = st.selectbox("Images to show", limit_options, index=0)

    records = all_records if limit == "All" else all_records[: int(limit)]
    render_gallery_grid(records)
    render_gallery_detail()
