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
from ui.data import entry_has_description, entry_is_fully_indexed
from ui.formatting import get_entry_display_fields, get_summary, to_jsonable


DETAIL_TRIGGER_KEY_PREFIX = "result_card_detail_trigger_"


def get_selected_entry_id() -> str | None:
    value = st.session_state.get("selected_entry_id")
    if value:
        return str(value)
    return None


def set_selected_entry_id(entry_id: str) -> None:
    st.session_state["selected_entry_id"] = str(entry_id)


def clear_selected_entry_id() -> None:
    st.session_state.pop("selected_entry_id", None)


def detail_trigger_key(entry_id: str) -> str:
    return f"{DETAIL_TRIGGER_KEY_PREFIX}{entry_id}"


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
        <div class="app-header">
          <h1>Media Search</h1>
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
    st.caption(
        "Search settings are applied after retrieval. Filtered searches fetch a larger candidate pool first."
    )
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
    st.selectbox(
        "Media type",
        ["All", "Images", "Videos"],
        key="filter_media_type",
    )
    st.multiselect(
        "Extensions",
        sorted(IMAGE_EXTENSIONS | VIDEO_EXTENSIONS),
        key="filter_extensions",
        help="Leave empty to allow all extensions.",
    )
    st.number_input(
        "Minimum score",
        min_value=0.0,
        max_value=1.0,
        step=0.001,
        format="%.4f",
        key="filter_min_score",
    )
    date_from_col, date_to_col = st.columns(2)
    with date_from_col:
        st.text_input(
            "Date from",
            key="filter_date_from",
            placeholder="YYYY-MM-DD",
        )
    with date_to_col:
        st.text_input(
            "Date to",
            key="filter_date_to",
            placeholder="YYYY-MM-DD",
        )


def detail_caption(rank: int | None, score: float | None, fallback: str) -> str:
    if rank is None:
        return fallback
    if score is None:
        return f"Rank {rank}"
    return f"Rank {rank} · Relevance score {score:.4f}"


def render_detail_media(file_path: str, ext: str) -> None:
    from ui.media import render_media

    render_media(file_path=file_path, ext=ext)


def render_detail_actions(
    file_path: str,
    close_label: str,
) -> None:
    safe_path = html.escape(file_path or "No path stored.")
    st.markdown(
        f'<div class="detail-path">{safe_path}</div>',
        unsafe_allow_html=True,
    )
    if st.button(close_label, type="primary"):
        clear_selected_entry_id()
        st.rerun()


def render_indexed_detail_body(
    entry_id: str,
    entry: dict,
    rank: int | None,
    score: float | None,
    close_label: str,
) -> None:
    metadata, file_path, file_name, ext = get_entry_display_fields(entry_id, entry)
    full_document = {"_id": entry_id, **entry}
    described = entry_has_description(entry)
    fully_indexed = entry_is_fully_indexed(entry)

    st.subheader(file_name)
    st.caption(
        detail_caption(
            rank, score, "Indexed media" if fully_indexed else "Pending indexing"
        )
    )

    left, right = st.columns([1.15, 0.85])
    with left:
        render_detail_media(file_path=file_path, ext=ext)

    with right:
        if fully_indexed:
            st.markdown("**Summary**")
            st.write(get_summary(entry))
        elif described:
            st.markdown("**Indexing status**")
            st.write("Waiting for Chroma")
            st.caption(
                "Description is stored in MongoDB. Chroma indexing has not completed yet."
            )
        else:
            st.markdown("**Analysis status**")
            st.write("Waiting for description")
            st.caption(
                "Metadata is stored. Description generation and Chroma indexing have not completed yet."
            )
        render_detail_actions(file_path=file_path, close_label=close_label)

    with st.expander("Description", expanded=False):
        description = entry.get("description") or {}
        if described:
            st.json(to_jsonable(description), expanded=False)
        else:
            st.info("No generated description yet.")

    metadata_tab, document_tab = st.tabs(["Metadata", "Full document"])
    with metadata_tab:
        st.json(to_jsonable(metadata), expanded=False)
    with document_tab:
        st.json(to_jsonable(full_document), expanded=False)


def render_detail_body(
    entry_id: str | None = None,
    entry: dict | None = None,
    rank: int | None = None,
    score: float | None = None,
    close_label: str = "Close details",
) -> None:
    if entry_id and entry:
        render_indexed_detail_body(
            entry_id=entry_id,
            entry=entry,
            rank=rank,
            score=score,
            close_label=close_label,
        )
        return

    st.warning("Details could not be loaded.")


if hasattr(st, "dialog"):

    @st.dialog("Configure Search", **dialog_options(width="small"))
    def search_settings_dialog():
        render_search_settings_body()

    @st.dialog(
        "Media Details",
        **dialog_options(width="large", on_dismiss=clear_selected_entry_id),
    )
    def detail_dialog(
        entry_id: str | None = None,
        entry: dict | None = None,
        rank: int | None = None,
        score: float | None = None,
        close_label: str = "Close details",
    ):
        render_detail_body(
            entry_id=entry_id,
            entry=entry,
            rank=rank,
            score=score,
            close_label=close_label,
        )

else:

    def search_settings_dialog():
        with st.expander("Search Settings", expanded=True):
            render_search_settings_body()

    def detail_dialog(
        entry_id: str | None = None,
        entry: dict | None = None,
        rank: int | None = None,
        score: float | None = None,
        close_label: str = "Close details",
    ):
        with st.container():
            render_detail_body(
                entry_id=entry_id,
                entry=entry,
                rank=rank,
                score=score,
                close_label=close_label,
            )


def render_result_preview_card(
    file_path: str,
    file_name: str,
    ext: str,
    *,
    rank: int | None = None,
    detail_entry_id: str | None = None,
    detail_title: str | None = None,
    overlay_details_html: str | None = None,
) -> None:
    path = Path(file_path) if file_path else None
    rank_badge = (
        f'<div class="result-card__rank">#{rank}</div>' if rank is not None else ""
    )
    card_markup = ""
    if ext in IMAGE_EXTENSIONS and path and path.is_file():
        try:
            from ui.media import get_thumbnail_data_uri

            title = html.escape(file_name)
            preview = get_thumbnail_data_uri(str(path), path.stat().st_mtime_ns)
            overlay_markup = (
                f'<div class="result-card__overlay">'
                f'<div class="result-card__overlay-title">{title}</div>'
                f'<div class="result-card__overlay-divider"></div>'
                f"{overlay_details_html}</div>"
                if overlay_details_html
                else f'<div class="result-card__title">{title}</div>'
            )
            card_markup = (
                f'<div class="result-card"><img src="{preview}" '
                f'alt="{title}" title="{title}">{rank_badge}'
                f"{overlay_markup}</div>"
            )
        except Exception as exc:
            label = f"Preview unavailable: {html.escape(str(exc))}"
    else:
        label = html.escape("Video" if ext in VIDEO_EXTENSIONS else "File")

    if not card_markup:
        card_markup = (
            f'<div class="result-card result-card--placeholder">{rank_badge}'
            f'<div class="result-placeholder">{label}</div></div>'
        )

    st.markdown(card_markup, unsafe_allow_html=True)
    if detail_entry_id:
        st.button(
            "Details",
            key=detail_trigger_key(detail_entry_id),
            help=detail_title or f"Open details for {file_name}",
            on_click=set_selected_entry_id,
            args=(detail_entry_id,),
        )


def render_media_card(
    file_path: str,
    file_name: str,
    ext: str,
    *,
    caption: str | None = None,
    rank: int | None = None,
    detail_entry_id: str | None = None,
    detail_title: str | None = None,
    overlay_details_html: str | None = None,
) -> None:
    render_result_preview_card(
        file_path=file_path,
        file_name=file_name,
        ext=ext,
        rank=rank,
        detail_entry_id=detail_entry_id,
        detail_title=detail_title,
        overlay_details_html=overlay_details_html,
    )
    if caption:
        st.markdown(
            f'<div class="result-card__caption">{html.escape(caption)}</div>',
            unsafe_allow_html=True,
        )


def render_result_card(
    entry_id: str,
    entry: dict,
    rank: int,
    score: float | None,
) -> None:
    _, file_path, file_name, ext = get_entry_display_fields(entry_id, entry)
    score_text = "Score unavailable" if score is None else f"Score {score:.4f}"
    render_media_card(
        file_path=file_path,
        file_name=file_name,
        ext=ext,
        rank=rank,
        detail_entry_id=entry_id,
        detail_title=f"{file_name} · {score_text}",
    )


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
