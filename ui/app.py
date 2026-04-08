"""Streamlit application orchestration."""

from time import perf_counter

from dotenv import load_dotenv
import streamlit as st

from ui.components import (
    clear_selected_entry_id,
    detail_dialog,
    dialog_options,
    get_selected_entry_id,
    render_app_shell,
    render_results_grid,
    search_settings_dialog,
)
from ui.config import DEFAULT_TOP_N
from ui.config import FILTERED_SEARCH_MULTIPLIER, MAX_FILTERED_CANDIDATES
from ui.filters import (
    active_filters_from_state,
    apply_result_filters,
    filters_are_active,
)
from ui.history import (
    clear_history,
    coerce_scores,
    history_label,
    load_history,
    save_search,
)

EMPTY_STATE = (
    "Describe the photo, video, or text you want to find."
)


def initialize_state() -> None:
    for key, value in {
        "page": "Search",
        "search_query": "",
        "top_n": DEFAULT_TOP_N,
        "filter_media_type": "All",
        "filter_extensions": [],
        "filter_min_score": 0.0,
        "filter_date_from": "",
        "filter_date_to": "",
        "last_result_ids": [],
        "last_result_entries": {},
        "last_result_scores": [],
        "last_result_ranks": {},
        "last_result_score_by_id": {},
    }.items():
        st.session_state.setdefault(key, value)


def render_navbar() -> str:
    _, nav_col, _ = st.columns([0.9, 4.8, 0.9])
    with nav_col:
        page = st.radio(
            "Navigation",
            ["Search", "Upload", "Gallery", "ChromaDB"],
            horizontal=True,
            key="page",
            label_visibility="collapsed",
            on_change=clear_selected_entry_id,
        )
    return page


def render_history_body() -> None:
    history = load_history()
    if not history:
        st.info("No saved searches yet.")
        return

    selected_index = st.selectbox(
        "Saved searches",
        range(len(history)),
        format_func=lambda index: history_label(history[index], index),
        key="search_history_selected_index",
    )
    load_col, flush_col = st.columns(2)
    with load_col:
        if st.button("Load results", key="search_history_load", type="primary"):
            st.session_state["history_item_to_load"] = history[selected_index]
            clear_selected_entry_id()
            st.rerun()
    with flush_col:
        if st.button("Clear history", key="search_history_clear"):
            clear_history()
            st.rerun()


if hasattr(st, "dialog"):

    @st.dialog("Search History", **dialog_options(width="large"))
    def search_history_dialog():
        render_history_body()

else:

    def search_history_dialog():
        with st.expander("Search History", expanded=True):
            render_history_body()


def restore_search_state(item: dict) -> None:
    filters = item.get("filters") or {}
    st.session_state["search_query"] = item.get("query", "")
    st.session_state["top_n"] = int(item.get("top_n", DEFAULT_TOP_N) or DEFAULT_TOP_N)
    st.session_state["filter_media_type"] = filters.get("media_type", "All")
    st.session_state["filter_extensions"] = filters.get("extensions", [])
    st.session_state["filter_min_score"] = float(filters.get("min_score", 0.0) or 0)
    st.session_state["filter_date_from"] = filters.get("date_from", "")
    st.session_state["filter_date_to"] = filters.get("date_to", "")


def load_saved_result(item: dict) -> tuple[list[str], dict, list[float | None], float]:
    from ui.data import get_entries

    restore_search_state(item)
    ids = [str(entry_id) for entry_id in item.get("ids", [])]
    return ids, get_entries(ids), coerce_scores(item.get("scores")), 0.0


def render_search_controls() -> bool:
    _, search_col, _ = st.columns([0.8, 5.4, 0.8])
    with search_col:
        with st.form("search_form", clear_on_submit=False):
            st.text_input(
                "Search",
                key="search_query",
                placeholder="Try “beach sunrise”, “group photo”, or “receipt text”",
                label_visibility="collapsed",
            )

            history_col, settings_col, _, search_button_col = st.columns(
                [0.56, 0.56, 5.6, 0.72],
                gap="small",
            )
            with search_button_col:
                submitted = st.form_submit_button(
                    "↗",
                    type="primary",
                    key="search_submit",
                    help="Run search",
                )
            with history_col:
                open_history = st.form_submit_button(
                    "↺",
                    key="search_history",
                    help="Search history",
                )
            with settings_col:
                configure = st.form_submit_button(
                    "⚙",
                    key="search_configure",
                    help="Configure search",
                )

        if open_history:
            search_history_dialog()
        if configure:
            search_settings_dialog()

        filters = active_filters_from_state(st.session_state)
        filter_note = " with filters" if filters_are_active(filters) else ""
        st.markdown(
            f'<div class="search-hint muted">Retrieving top '
            f'{int(st.session_state["top_n"])} result(s){filter_note}</div>',
            unsafe_allow_html=True,
        )

    return submitted


def render_empty_state() -> None:
    st.markdown(
        f'<div class="empty-state muted">{EMPTY_STATE}</div>',
        unsafe_allow_html=True,
    )


def search(query: str, top_n: int) -> tuple[list[str], dict, list[float], float]:
    from ui.data import get_entries, get_query_results

    start = perf_counter()
    ids, result = get_query_results(query=query, top_n=top_n)
    entries = get_entries(ids)
    elapsed_ms = (perf_counter() - start) * 1000
    return ids, entries, result.get("score", []), elapsed_ms


def candidate_count(top_n: int, filters: dict) -> int:
    if not filters_are_active(filters):
        return top_n
    return min(top_n * FILTERED_SEARCH_MULTIPLIER, MAX_FILTERED_CANDIDATES)


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="Media Search", layout="wide")
    initialize_state()
    render_app_shell()
    page = render_navbar()

    if page == "ChromaDB":
        from ui.chroma_viewer import render_chroma_viewer

        render_chroma_viewer()
        return

    if page == "Upload":
        from ui.upload import render_upload_page

        render_upload_page()
        return

    if page == "Gallery":
        from ui.gallery import render_gallery_page

        render_gallery_page()
        return

    history_item = st.session_state.pop("history_item_to_load", None)
    if history_item:
        clear_selected_entry_id()
        with st.spinner("Loading saved results..."):
            ids, entries, scores, elapsed_ms = load_saved_result(history_item)
        st.session_state.update(
            last_query=st.session_state["search_query"],
            last_result_ids=ids,
            last_result_entries=entries,
            last_result_scores=scores,
            last_search_ms=elapsed_ms,
            last_candidate_count=len(ids),
            last_filters_active=filters_are_active(
                active_filters_from_state(st.session_state)
            ),
        )

    submitted = render_search_controls()

    if submitted:
        clear_selected_entry_id()
        query = st.session_state["search_query"].strip()
        top_n = int(st.session_state["top_n"])
        filters = active_filters_from_state(st.session_state)
        if not query:
            st.warning("Enter a query first.")
            render_empty_state()
            return

        try:
            with st.spinner("Searching indexed media..."):
                ids, entries, scores, elapsed_ms = search(
                    query=query,
                    top_n=candidate_count(top_n, filters),
                )
        except Exception as exc:
            st.error("Search failed. Check MongoDB, Chroma, Ollama, and .env settings.")
            st.exception(exc)
            return

        result_ids, result_scores = apply_result_filters(
            ids=ids,
            entries=entries,
            scores=scores,
            filters=filters,
            limit=top_n,
        )
        save_search(
            query=query,
            top_n=top_n,
            filters=filters,
            ids=result_ids,
            scores=result_scores,
        )
        st.session_state.update(
            last_query=query,
            last_result_ids=result_ids,
            last_result_entries=entries,
            last_result_scores=result_scores,
            last_search_ms=elapsed_ms,
            last_candidate_count=len(ids),
            last_filters_active=filters_are_active(filters),
        )

    ids = st.session_state.get("last_result_ids", [])
    entries = st.session_state.get("last_result_entries", {})
    scores = st.session_state.get("last_result_scores", [])

    if not ids:
        if submitted:
            st.info("No results found.")
        else:
            render_empty_state()
        return

    elapsed_ms = st.session_state.get("last_search_ms")
    timing = ""
    if elapsed_ms is not None:
        timing = (
            f" in {elapsed_ms / 1000:.2f}s"
            if elapsed_ms >= 1000
            else f" in {elapsed_ms:.0f}ms"
        )

    st.caption(
        f"Showing {len(ids)} result(s) for "
        f"{st.session_state.get('last_query', '')}{timing}."
    )
    if st.session_state.get("last_filters_active"):
        st.caption(
            f"Filters were applied to "
            f"{st.session_state.get('last_candidate_count', len(ids))} candidate(s)."
        )
    render_results_grid(ids=ids, entries=entries, scores=scores)

    selected_entry_id = get_selected_entry_id()
    if not selected_entry_id:
        return
    if selected_entry_id not in entries:
        clear_selected_entry_id()
        return
    detail_dialog(
        entry_id=selected_entry_id,
        entry=entries[selected_entry_id],
        rank=st.session_state["last_result_ranks"].get(selected_entry_id, 0),
        score=st.session_state["last_result_score_by_id"].get(selected_entry_id),
    )
