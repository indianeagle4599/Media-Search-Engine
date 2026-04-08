"""Uploaded media gallery page."""

import streamlit as st

from ui.config import IMAGE_EXTENSIONS
from ui.data import get_upload_collection


DEFAULT_SORT = "Date uploaded (newest first)"
SORT_OPTIONS = {
    DEFAULT_SORT: {"created_at": -1},
    "Date uploaded (oldest first)": {"created_at": 1},
    "Original filename (A-Z)": {"original_filename": 1},
    "Original filename (Z-A)": {"original_filename": -1},
    "Status (A-Z)": {"status": 1, "created_at": -1},
    "Random sample": None,
}


def get_gallery_records(limit: int = 48, sort_by: str = DEFAULT_SORT) -> list[dict]:
    pipeline = [
        {
            "$match": {
                "ext": {"$in": sorted(IMAGE_EXTENSIONS)},
                "stored_path": {"$exists": True, "$ne": ""},
            }
        },
        {"$sort": {"created_at": -1}},
        {"$group": {"_id": "$file_hash", "record": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$record"}},
    ]

    sort_spec = SORT_OPTIONS.get(sort_by, SORT_OPTIONS[DEFAULT_SORT])
    if sort_spec is None:
        pipeline.append({"$sample": {"size": int(limit)}})
    else:
        pipeline.extend(
            [
                {"$sort": sort_spec},
                {"$limit": int(limit)},
            ]
        )

    records = list(get_upload_collection().aggregate(pipeline))
    for record in records:
        record["_id"] = str(record["_id"])
    return records


def render_gallery_grid(records: list[dict], columns: int = 4) -> None:
    if not records:
        st.info("No uploaded images found yet.")
        return

    from ui.components import render_result_preview

    for row_start in range(0, len(records), columns):
        row_columns = st.columns(columns, gap="medium")
        for offset, column in enumerate(row_columns):
            index = row_start + offset
            if index >= len(records):
                continue

            record = records[index]
            with column:
                render_result_preview(
                    file_path=record.get("stored_path", ""),
                    file_name=record.get("original_filename") or "Uploaded image",
                    ext=record.get("ext", ""),
                )
                st.caption(
                    f"{record.get('original_filename', 'Uploaded image')} · "
                    f"{record.get('status', 'unknown')} · "
                    f"{record.get('upload_day', '')}"
                )


def render_gallery_page() -> None:
    st.subheader("Gallery")
    st.caption(
        "Browse uploaded images. Gallery items are deduplicated by content hash."
    )

    sort_col, limit_col = st.columns([2, 1])
    with sort_col:
        sort_by = st.selectbox(
            "Sort by",
            list(SORT_OPTIONS),
            index=0,
        )
    with limit_col:
        limit = st.number_input(
            "Images to show",
            min_value=4,
            max_value=200,
            value=48,
            step=4,
        )

    records = get_gallery_records(limit=int(limit), sort_by=sort_by)
    render_gallery_grid(records)
