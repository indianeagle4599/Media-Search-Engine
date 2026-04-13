"""
gallery.py

Gallery page for browsing Mongo-backed media entries with filters and multiple result views.
"""

import html
import random

import streamlit as st

from ui.components import (
    clear_selected_entry_id,
    detail_dialog,
    dialog_options,
    get_selected_entry_id,
    render_media_list_row,
    render_media_card,
)
from ui.config import IMAGE_EXTENSIONS
from ui.data import (
    get_entries,
    get_entry_creation_date,
    get_entry_processing_status,
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
VIEW_OPTIONS = ["Grid", "Compact list", "Details list"]


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
        "status": get_entry_processing_status(entry),
    }


def sort_gallery_records(records: list[dict], sort_by: str) -> list[dict]:
    if sort_by == DEFAULT_SORT:
        return sorted(
            records, key=lambda record: record.get("uploaded_at") or "", reverse=True
        )
    if sort_by == "Upload date (oldest first)":
        return sorted(records, key=lambda record: record.get("uploaded_at") or "")
    if sort_by == "Creation date (newest first)":
        ordered = sorted(
            records, key=lambda record: record.get("creation_date") or "", reverse=True
        )
        return sorted(ordered, key=lambda record: record.get("creation_date") is None)
    if sort_by == "Creation date (oldest first)":
        ordered = sorted(records, key=lambda record: record.get("creation_date") or "")
        return sorted(ordered, key=lambda record: record.get("creation_date") is None)
    if sort_by == "Filename (A-Z)":
        return sorted(
            records, key=lambda record: str(record.get("file_name") or "").lower()
        )
    if sort_by == "Filename (Z-A)":
        return sorted(
            records,
            key=lambda record: str(record.get("file_name") or "").lower(),
            reverse=True,
        )
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


def filter_gallery_records(
    records: list[dict],
    *,
    file_name_query: str = "",
    statuses: list[str] | None = None,
    extensions: list[str] | None = None,
) -> list[dict]:
    name_query = str(file_name_query or "").strip().lower()
    allowed_statuses = {str(value) for value in (statuses or []) if str(value)}
    allowed_extensions = {
        str(value).lower().lstrip(".") for value in (extensions or []) if str(value)
    }

    filtered = []
    for record in records:
        file_name = str(record.get("file_name") or "")
        status = str(record.get("status") or "")
        ext = str(record.get("ext") or "").lower()
        if name_query and name_query not in file_name.lower():
            continue
        if allowed_statuses and status not in allowed_statuses:
            continue
        if allowed_extensions and ext not in allowed_extensions:
            continue
        filtered.append(record)
    return filtered


def active_gallery_filter_count() -> int:
    count = 0
    if str(st.session_state.get("gallery_filter_name") or "").strip():
        count += 1
    if st.session_state.get("gallery_filter_status"):
        count += 1
    if st.session_state.get("gallery_filter_extensions"):
        count += 1
    return count


def sync_gallery_filter_draft() -> None:
    st.session_state["gallery_filter_name_draft"] = str(
        st.session_state.get("gallery_filter_name") or ""
    )
    st.session_state["gallery_filter_status_draft"] = list(
        st.session_state.get("gallery_filter_status") or []
    )
    st.session_state["gallery_filter_extensions_draft"] = list(
        st.session_state.get("gallery_filter_extensions") or []
    )


def reset_gallery_filters() -> None:
    st.session_state["gallery_filter_name"] = ""
    st.session_state["gallery_filter_status"] = []
    st.session_state["gallery_filter_extensions"] = []


def reset_gallery_filter_draft() -> None:
    st.session_state["gallery_filter_name_draft"] = ""
    st.session_state["gallery_filter_status_draft"] = []
    st.session_state["gallery_filter_extensions_draft"] = []


def apply_gallery_filter_draft() -> None:
    st.session_state["gallery_filter_name"] = str(
        st.session_state.get("gallery_filter_name_draft") or ""
    )
    st.session_state["gallery_filter_status"] = list(
        st.session_state.get("gallery_filter_status_draft") or []
    )
    st.session_state["gallery_filter_extensions"] = list(
        st.session_state.get("gallery_filter_extensions_draft") or []
    )


def render_gallery_filter_body(
    status_options: list[str],
    ext_options: list[str],
) -> None:
    st.caption("Filter the Mongo-backed gallery result set.")
    st.text_input(
        "Filename contains",
        key="gallery_filter_name_draft",
        placeholder="Filter by name",
    )
    st.multiselect(
        "Status",
        status_options,
        key="gallery_filter_status_draft",
    )
    st.multiselect(
        "Extensions",
        ext_options,
        key="gallery_filter_extensions_draft",
    )
    reset_col, apply_col = st.columns(2, gap="small")
    with reset_col:
        if st.button("Reset", key="gallery_filters_reset", width="stretch"):
            reset_gallery_filter_draft()
            st.rerun()
    with apply_col:
        if st.button("Apply filters", key="gallery_filters_apply", width="stretch"):
            apply_gallery_filter_draft()
            st.rerun()


if hasattr(st, "dialog"):

    @st.dialog("Gallery Filters", **dialog_options(width="small"))
    def gallery_filters_dialog(
        status_options: list[str],
        ext_options: list[str],
    ) -> None:
        render_gallery_filter_body(status_options, ext_options)

else:

    def gallery_filters_dialog(
        status_options: list[str],
        ext_options: list[str],
    ) -> None:
        with st.expander("Gallery filters", expanded=True):
            render_gallery_filter_body(status_options, ext_options)


def gallery_metadata_markup(record: dict) -> str:
    rows = [("Status", str(record.get("status", "unknown")).replace("_", " ").title())]
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


def render_gallery_list(records: list[dict], *, detailed: bool = False) -> None:
    if not records:
        st.info("No uploaded images found yet.")
        return

    for record in records:
        meta_lines = [
            " · ".join(
                value
                for value in [
                    str(record.get("status", "unknown")).replace("_", " ").title(),
                    str(record.get("uploaded_at") or "")[:19].replace("T", " "),
                    f".{record.get('ext', '')}" if record.get("ext") else "",
                ]
                if value
            )
        ]
        if detailed and record.get("creation_date"):
            meta_lines.append(f"Created: {str(record.get('creation_date') or '')[:10]}")
        render_media_list_row(
            file_path=record.get("file_path", ""),
            file_name=record.get("file_name", "Uploaded image"),
            ext=record.get("ext", ""),
            title=record.get("file_name", "Uploaded image"),
            meta_lines=meta_lines,
            body_text=record.get("file_path", "") if detailed else "",
            detail_entry_id=str(record["_id"]),
            detail_title=record.get("file_name", "Uploaded image"),
            button_key_prefix="gallery_list_detail",
            preview_ratio=1.3 if detailed else 0.75,
            preview_width=200 if detailed else 72,
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

    label_sort_col, label_view_col, label_filter_col, label_limit_col = st.columns(
        [2.2, 1.2, 0.9, 1]
    )
    with label_sort_col:
        st.caption("Sort by")
    with label_view_col:
        st.caption("View")
    with label_filter_col:
        st.caption("Filters")
    with label_limit_col:
        st.caption("Images to show")

    sort_col, view_col, filter_col, limit_col = st.columns([2.2, 1.2, 0.9, 1])
    with sort_col:
        sort_by = st.selectbox(
            "Sort by",
            SORT_OPTIONS,
            index=0,
            label_visibility="collapsed",
        )
    with view_col:
        view_mode = st.selectbox(
            "View",
            VIEW_OPTIONS,
            key="gallery_view",
            label_visibility="collapsed",
        )
    all_records = get_gallery_records(limit=0, sort_by=sort_by)
    status_options = sorted(
        {record.get("status", "") for record in all_records if record.get("status")}
    )
    ext_options = sorted(
        {record.get("ext", "") for record in all_records if record.get("ext")}
    )
    with filter_col:
        open_filters = st.button(
            f"Filters ({active_gallery_filter_count()})",
            key="gallery_open_filters",
            width="stretch",
        )
    if open_filters:
        sync_gallery_filter_draft()
        gallery_filters_dialog(status_options, ext_options)
    with limit_col:
        base_options = [24, 48, 96, 144, 200]
        limit_options = [option for option in base_options if option < len(all_records)]
        limit_options.append("All")
        limit = st.selectbox(
            "Images to show",
            limit_options,
            index=0,
            label_visibility="collapsed",
        )

    filtered_records = filter_gallery_records(
        all_records,
        file_name_query=str(st.session_state.get("gallery_filter_name") or ""),
        statuses=list(st.session_state.get("gallery_filter_status") or []),
        extensions=list(st.session_state.get("gallery_filter_extensions") or []),
    )
    records = filtered_records if limit == "All" else filtered_records[: int(limit)]
    st.caption(f"Showing {len(records)} of {len(all_records)} image(s).")
    if view_mode == "Grid":
        render_gallery_grid(records)
    else:
        render_gallery_list(records, detailed=view_mode == "Details list")
    render_gallery_detail()
