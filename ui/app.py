"""Streamlit application orchestration."""

from time import perf_counter

from dotenv import load_dotenv
import streamlit as st

from ui.components import (
    clear_selected_entry_id,
    detail_dialog,
    get_selected_entry_id,
    render_app_shell,
    render_results_grid,
    search_settings_dialog,
)
from ui.config import DEFAULT_TOP_N

EMPTY_STATE = (
    "Enter a search to browse the indexed media library. Results stay on this page "
    "while details open as an overlay."
)


def initialize_state() -> None:
    for key, value in {
        "page": "Search",
        "search_query": "",
        "top_n": DEFAULT_TOP_N,
        "last_result_ids": [],
        "last_result_entries": {},
        "last_result_scores": [],
        "last_result_ranks": {},
        "last_result_score_by_id": {},
    }.items():
        st.session_state.setdefault(key, value)


def render_navbar() -> str:
    _, nav_col, _ = st.columns([1.35, 2.3, 1.35])
    with nav_col:
        page = st.radio(
            "Navigation",
            ["Search", "ChromaDB"],
            horizontal=True,
            key="page",
            label_visibility="collapsed",
            on_change=clear_selected_entry_id,
        )
    return page


def render_search_controls() -> bool:
    _, search_col, _ = st.columns([1.7, 3.6, 1.7])
    with search_col:
        st.text_input(
            "Search",
            key="search_query",
            placeholder="Try “beach sunrise”, “group photo”, or “receipt text”",
            label_visibility="collapsed",
        )

        settings_col, search_button_col = st.columns([1, 1], gap="small")
        with settings_col:
            if st.button(
                "Configure",
                key="search_configure",
                use_container_width=True,
            ):
                search_settings_dialog()
        with search_button_col:
            submitted = st.button(
                "Search",
                key="search_submit",
                type="primary",
                use_container_width=True,
            )

        st.markdown(
            f'<div class="search-hint muted">Retrieving top '
            f'{int(st.session_state["top_n"])} result(s)</div>',
            unsafe_allow_html=True,
        )

    return submitted


def search(query: str, top_n: int) -> tuple[list[str], dict, list[float], float]:
    from ui.data import get_entries, get_query_results

    start = perf_counter()
    ids, result = get_query_results(query=query, top_n=top_n)
    entries = get_entries(ids)
    elapsed_ms = (perf_counter() - start) * 1000
    return ids, entries, result.get("score", []), elapsed_ms


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

    submitted = render_search_controls()

    if submitted:
        clear_selected_entry_id()
        query = st.session_state["search_query"].strip()
        if not query:
            st.warning("Enter a query first.")
            st.info(EMPTY_STATE)
            return

        try:
            with st.spinner("Searching indexed media..."):
                ids, entries, scores, elapsed_ms = search(
                    query=query,
                    top_n=int(st.session_state["top_n"]),
                )
        except Exception as exc:
            st.error("Search failed. Check MongoDB, Chroma, Ollama, and .env settings.")
            st.exception(exc)
            return

        st.session_state.update(
            last_query=query,
            last_result_ids=ids,
            last_result_entries=entries,
            last_result_scores=scores,
            last_search_ms=elapsed_ms,
        )

    ids = st.session_state.get("last_result_ids", [])
    entries = st.session_state.get("last_result_entries", {})
    scores = st.session_state.get("last_result_scores", [])

    if not ids:
        st.info("No results found." if submitted else EMPTY_STATE)
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
