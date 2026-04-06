"""ChromaDB inspection page."""

import os
from typing import Any

import pandas as pd
import streamlit as st

from ui.config import CHROMA_VIEWER_DEFAULT_LIMIT
from ui.data import get_chroma_client
from ui.formatting import to_jsonable


def chroma_location() -> str:
    host = os.getenv("CHROMA_HOST") or os.getenv("CHROMA_SERVER_HOST")
    if host:
        scheme = (
            "https"
            if os.getenv("CHROMA_SSL", "").lower() in {"1", "true", "yes"}
            else "http"
        )
        return f"{scheme}://{host}:{os.getenv('CHROMA_PORT') or 8000}"

    db_dir = os.getenv("CHROMA_URL")
    if not db_dir:
        return "Not configured"
    return os.path.abspath(db_dir).replace("\\", "/")


def get_item(data: dict, key: str, index: int, default=None):
    values = data.get(key)
    if values is None:
        return default
    try:
        return values[index]
    except (IndexError, KeyError, TypeError):
        return default


def embedding_shape(embedding: Any) -> str:
    if embedding is None:
        return ""
    if getattr(embedding, "shape", None) is not None:
        return str(tuple(embedding.shape))
    try:
        return f"({len(embedding)},)"
    except TypeError:
        return "(scalar,)"


def get_collection(client, collection_ref):
    return (
        client.get_collection(collection_ref)
        if isinstance(collection_ref, str)
        else collection_ref
    )


def collection_label(collection, row_limit: int) -> str:
    try:
        count = int(collection.count())
    except Exception:
        return collection.name

    if count > row_limit:
        return f"{collection.name} ({row_limit} of {count} shown)"
    return f"{collection.name} ({count} items)"


def collection_rows(data: dict) -> list[dict]:
    rows = []
    for index, entry_id in enumerate(data.get("ids") or []):
        metadata = get_item(data, "metadatas", index, {}) or {}
        metadata_keys = (
            ", ".join(sorted(str(key) for key in metadata))
            if isinstance(metadata, dict)
            else ""
        )
        rows.append(
            {
                "id": entry_id,
                "document": get_item(data, "documents", index, ""),
                "metadata_keys": metadata_keys,
                "embedding": embedding_shape(get_item(data, "embeddings", index)),
            }
        )
    return rows


def render_metadata(data: dict, index: int) -> None:
    ids = data.get("ids") or []
    if not ids:
        return

    st.markdown(f"**Metadata JSON: `{ids[index]}`**")
    document = get_item(data, "documents", index, "")
    if document:
        st.code(str(document), language="text")
    st.json(
        to_jsonable(get_item(data, "metadatas", index, {}) or {}),
        expanded=False,
    )


def render_collection(collection, row_limit: int) -> None:
    with st.expander(collection_label(collection, row_limit), expanded=False):
        try:
            data = collection.get(
                include=["documents", "metadatas", "embeddings"],
                limit=row_limit,
            )
        except Exception as exc:
            st.error(f"Could not read collection `{collection.name}`.")
            st.exception(exc)
            return

        rows = collection_rows(data)
        if not rows:
            st.info("This collection has no rows.")
            return

        event = st.dataframe(
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
            key=f"chroma_table_{collection.name}",
            on_select="rerun",
            selection_mode="single-row",
        )
        selected_rows = event.selection.rows
        render_metadata(data=data, index=selected_rows[0] if selected_rows else 0)


def render_chroma_viewer() -> None:
    st.subheader("ChromaDB Viewer")
    st.caption(f"Chroma location: {chroma_location()}")

    row_limit = int(
        st.number_input(
            "Rows per collection",
            min_value=1,
            max_value=1000,
            value=CHROMA_VIEWER_DEFAULT_LIMIT,
            step=25,
            help="Maximum rows to read from each collection.",
        )
    )

    try:
        client = get_chroma_client()
        collection_refs = client.list_collections()
    except Exception as exc:
        st.error("Could not connect to ChromaDB.")
        st.exception(exc)
        return

    if not collection_refs:
        st.info("No Chroma collections found.")
        return

    for collection_ref in collection_refs:
        try:
            render_collection(get_collection(client, collection_ref), row_limit)
        except Exception as exc:
            st.error(f"Could not load collection `{collection_ref}`.")
            st.exception(exc)
