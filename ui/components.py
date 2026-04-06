"""Reusable Streamlit UI components."""

import html
import inspect
from pathlib import Path

import streamlit as st

from ui.config import (
    APP_CSS,
    DEFAULT_TOP_N,
    GRID_COLUMNS,
    IMAGE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)
from ui.formatting import get_entry_display_fields, get_summary, to_jsonable


def get_selected_entry_id() -> str | None:
    return st.session_state.get("selected_entry_id")


def clear_selected_entry_id() -> None:
    st.session_state.pop("selected_entry_id", None)


def update_result_indexes(ids: list[str], scores: list[float]) -> None:
    st.session_state["last_result_ranks"] = {
        entry_id: index + 1 for index, entry_id in enumerate(ids)
    }
    st.session_state["last_result_score_by_id"] = {
        entry_id: scores[index]
        for index, entry_id in enumerate(ids)
        if index < len(scores)
    }


def render_app_shell() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <div class="hero">
          <h1>Media Search</h1>
          <p>Search your indexed photos and videos, then inspect the matching media without leaving the results page.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def dialog_options(**kwargs) -> dict:
    try:
        parameters = inspect.signature(st.dialog).parameters
    except (TypeError, ValueError):
        return {}
    return {key: value for key, value in kwargs.items() if key in parameters}


def render_search_settings_body() -> None:
    st.caption("Search filters live here. Date and media filters can be added later.")
    st.session_state["top_n"] = int(
        st.number_input(
            "Number of results",
            min_value=1,
            max_value=50,
            value=int(st.session_state.get("top_n", DEFAULT_TOP_N)),
            step=1,
            help="Number of ranked results to fetch from Chroma.",
        )
    )


def render_detail_body(entry_id: str, entry: dict, rank: int, score: float | None):
    metadata, file_path, file_name, ext = get_entry_display_fields(entry_id, entry)
    full_document = {"_id": entry_id, **entry}

    st.subheader(file_name)
    if score is not None:
        st.caption(f"Rank {rank} · Relevance score {score:.4f}")
    else:
        st.caption(f"Rank {rank}")

    left, right = st.columns([1.15, 0.85])
    with left:
        from ui.media import render_media

        render_media(file_path=file_path, ext=ext)

    with right:
        st.markdown("**Summary**")
        st.write(get_summary(entry))
        safe_path = html.escape(file_path or "No path stored.")
        st.markdown(
            f'<div class="detail-path">{safe_path}</div>',
            unsafe_allow_html=True,
        )

        if st.button("Back to results", type="primary", use_container_width=True):
            clear_selected_entry_id()
            st.rerun()

    with st.expander("Description", expanded=False):
        st.json(to_jsonable(entry.get("description", {})), expanded=False)

    metadata_tab, document_tab = st.tabs(["Metadata", "Full document"])
    with metadata_tab:
        st.json(to_jsonable(metadata), expanded=False)
    with document_tab:
        st.json(to_jsonable(full_document), expanded=False)


if hasattr(st, "dialog"):

    @st.dialog("Configure Search", **dialog_options(width="small"))
    def search_settings_dialog():
        render_search_settings_body()

    @st.dialog(
        "Media Details",
        **dialog_options(width="large", on_dismiss=clear_selected_entry_id),
    )
    def detail_dialog(entry_id: str, entry: dict, rank: int, score: float | None):
        render_detail_body(entry_id=entry_id, entry=entry, rank=rank, score=score)

else:

    def search_settings_dialog():
        with st.expander("Search Settings", expanded=True):
            render_search_settings_body()

    def detail_dialog(entry_id: str, entry: dict, rank: int, score: float | None):
        with st.container():
            render_detail_body(entry_id=entry_id, entry=entry, rank=rank, score=score)


def render_result_preview(file_path: str, file_name: str, ext: str) -> None:
    path = Path(file_path) if file_path else None
    if ext in IMAGE_EXTENSIONS and path and path.is_file():
        try:
            from ui.media import get_thumbnail_data_uri

            title = html.escape(file_name)
            preview = get_thumbnail_data_uri(str(path), path.stat().st_mtime_ns)
            st.markdown(
                f'<div class="result-card"><img src="{preview}" '
                f'alt="{title}" title="{title}">'
                f'<div class="result-card__title">{title}</div></div>',
                unsafe_allow_html=True,
            )
            return
        except Exception as exc:
            label = f"Preview unavailable: {html.escape(str(exc))}"
    else:
        label = html.escape("Video" if ext in VIDEO_EXTENSIONS else "File")

    st.markdown(
        f'<div class="result-placeholder">{label}</div>',
        unsafe_allow_html=True,
    )


def render_result_card(
    entry_id: str, entry: dict, rank: int, score: float | None
) -> None:
    _, file_path, file_name, ext = get_entry_display_fields(entry_id, entry)
    render_result_preview(file_path=file_path, file_name=file_name, ext=ext)
    score_text = "Score unavailable" if score is None else f"Score {score:.4f}"
    if st.button(
        f"#{rank} Details",
        key=f"details_{entry_id}",
        help=f"{file_name} · {score_text}",
        use_container_width=True,
    ):
        st.session_state["selected_entry_id"] = entry_id


def render_results_grid(
    ids: list[str],
    entries: dict,
    scores: list[float],
    columns: int = GRID_COLUMNS,
) -> None:
    update_result_indexes(ids, scores)
    score_by_id = st.session_state["last_result_score_by_id"]

    for row_start in range(0, len(ids), columns):
        row_columns = st.columns(columns, gap="medium")
        for offset, column in enumerate(row_columns):
            index = row_start + offset
            if index >= len(ids):
                continue

            entry_id = ids[index]
            entry = entries.get(entry_id)
            with column:
                if not entry:
                    st.warning(f"Result {entry_id} was not found in MongoDB.")
                    continue
                render_result_card(
                    entry_id=entry_id,
                    entry=entry,
                    rank=index + 1,
                    score=score_by_id.get(entry_id),
                )
