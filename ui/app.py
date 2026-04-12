"""
app.py

Streamlit application entrypoint for search, upload, gallery, and Chroma views.
"""

from time import perf_counter

from dotenv import load_dotenv
import streamlit as st

from ui.chroma_viewer import render_chroma_viewer
from ui.components import (
    clear_selected_entry_id,
    detail_dialog,
    dialog_options,
    get_selected_entry_id,
    render_app_shell,
    render_results_grid,
    render_search_debug_panel,
    search_settings_dialog,
)
from ui.config import DEFAULT_TOP_N
from ui.config import FILTERED_SEARCH_MULTIPLIER, MAX_FILTERED_CANDIDATES
from ui.data import get_entries, get_query_results
from ui.filters import (
    active_filters_from_state,
    apply_result_filters,
    filters_are_active,
)
from ui.gallery import render_gallery_page
from ui.history import (
    clear_history,
    history_label,
    load_history,
    save_search,
)
from ui.upload import render_upload_page
from utils.retrieval import SearchManifest

EMPTY_STATE = "Describe the photo, video, or text you want to find."


def initialize_state() -> None:
    for key, value in {
        "page": "Search",
        "search_query": "",
        "top_n": DEFAULT_TOP_N,
        "search_preset": SearchManifest.DEFAULT_PRESET,
        "search_focus_words": SearchManifest.DEFAULT_FOCUS["words"],
        "search_focus_meaning": SearchManifest.DEFAULT_FOCUS["meaning"],
        "search_focus_text": SearchManifest.DEFAULT_FOCUS["text"],
        "search_focus_time": SearchManifest.DEFAULT_FOCUS["time"],
        "search_include_debug": False,
        "search_enabled_search_types": list(SearchManifest.SEARCH_TYPES),
        "search_enabled_sources": list(SearchManifest.SOURCES),
        "search_capabilities": [],
        "filter_media_type": "All",
        "filter_result_sources": [],
        "filter_extensions": [],
        "filter_min_score": 0.0,
        "filter_date_from": "",
        "filter_date_to": "",
        "last_result_ids": [],
        "last_result_entries": {},
        "last_result_scores": [],
        "last_result_ranks": {},
        "last_result_score_by_id": {},
        "last_result_items": [],
        "last_result_item_by_id": {},
        "last_search_response": {},
        "last_search_plan": {},
        "last_search_options": {},
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
    search_options = item.get("search_options") or {}
    preset = search_options.get("preset") or SearchManifest.DEFAULT_PRESET
    preset_focus = dict(SearchManifest.DEFAULT_FOCUS)
    preset_focus.update((SearchManifest.PRESETS.get(preset) or {}).get("focus") or {})
    focus = search_options.get("focus") or {}
    st.session_state["search_query"] = item.get("query", "")
    st.session_state["top_n"] = int(item.get("top_n", DEFAULT_TOP_N) or DEFAULT_TOP_N)
    st.session_state["search_preset"] = preset
    st.session_state["search_focus_words"] = int(
        focus.get("words", preset_focus["words"]) or 0
    )
    st.session_state["search_focus_meaning"] = int(
        focus.get("meaning", preset_focus["meaning"]) or 0
    )
    st.session_state["search_focus_text"] = int(
        focus.get("text", preset_focus["text"]) or 0
    )
    st.session_state["search_focus_time"] = int(
        focus.get("time", preset_focus["time"]) or 0
    )
    st.session_state["search_enabled_search_types"] = list(
        search_options.get("enabled_search_types") or SearchManifest.SEARCH_TYPES
    )
    st.session_state["search_enabled_sources"] = list(
        search_options.get("enabled_sources") or SearchManifest.SOURCES
    )
    st.session_state["search_capabilities"] = list(
        search_options.get("capabilities") or []
    )
    st.session_state["search_include_debug"] = bool(item.get("debug_enabled", False))
    st.session_state["filter_media_type"] = filters.get("media_type", "All")
    st.session_state["filter_result_sources"] = filters.get("result_sources", [])
    st.session_state["filter_extensions"] = filters.get("extensions", [])
    st.session_state["filter_min_score"] = float(filters.get("min_score", 0.0) or 0)
    st.session_state["filter_date_from"] = filters.get("date_from", "")
    st.session_state["filter_date_to"] = filters.get("date_to", "")


def search_options_from_state() -> dict:
    return {
        "preset": st.session_state.get("search_preset", SearchManifest.DEFAULT_PRESET),
        "focus": {
            "words": int(
                st.session_state.get(
                    "search_focus_words", SearchManifest.DEFAULT_FOCUS["words"]
                )
            ),
            "meaning": int(
                st.session_state.get(
                    "search_focus_meaning",
                    SearchManifest.DEFAULT_FOCUS["meaning"],
                )
            ),
            "text": int(
                st.session_state.get(
                    "search_focus_text", SearchManifest.DEFAULT_FOCUS["text"]
                )
            ),
            "time": int(
                st.session_state.get(
                    "search_focus_time", SearchManifest.DEFAULT_FOCUS["time"]
                )
            ),
        },
        "enabled_sources": list(
            st.session_state.get("search_enabled_sources", list(SearchManifest.SOURCES))
        ),
        "enabled_search_types": list(
            st.session_state.get(
                "search_enabled_search_types", list(SearchManifest.SEARCH_TYPES)
            )
        ),
        "capabilities": list(st.session_state.get("search_capabilities", [])),
    }


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

            _, search_button_col, history_col, settings_col = st.columns(
                [5.6, 0.72, 0.56, 0.56],
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


def search(
    query: str,
    top_n: int,
    search_options: dict,
    include_debug: bool,
) -> tuple[list[str], dict, list[float], dict, float]:
    start = perf_counter()
    ids, result = get_query_results(
        query=query,
        top_n=top_n,
        search_options=search_options,
        include_debug=include_debug,
    )
    entries = get_entries(ids)
    elapsed_ms = (perf_counter() - start) * 1000
    return ids, entries, result.get("score", []), result, elapsed_ms


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
        render_chroma_viewer()
        return

    if page == "Upload":
        render_upload_page()
        return

    if page == "Gallery":
        render_gallery_page()
        return

    history_item = st.session_state.pop("history_item_to_load", None)
    if history_item:
        clear_selected_entry_id()
        with st.spinner("Loading saved results..."):
            restore_search_state(history_item)
            replay_query = st.session_state["search_query"].strip()
            replay_filters = active_filters_from_state(st.session_state)
            search_options = search_options_from_state()
            ids, entries, scores, result, elapsed_ms = search(
                query=replay_query,
                top_n=candidate_count(int(st.session_state["top_n"]), replay_filters),
                search_options=search_options,
                include_debug=bool(st.session_state.get("search_include_debug")),
            )
            result_items = result.get("items") or []
            result_item_by_id = {item["id"]: item for item in result_items}
            filtered_ids, filtered_scores, filtered_items = apply_result_filters(
                ids=ids,
                entries=entries,
                scores=scores,
                result_items_by_id=result_item_by_id,
                filters=replay_filters,
                limit=int(st.session_state["top_n"]),
            )
        st.session_state.update(
            last_query=st.session_state["search_query"],
            last_result_ids=filtered_ids,
            last_result_entries=entries,
            last_result_scores=filtered_scores,
            last_result_items=filtered_items,
            last_result_item_by_id={item["id"]: item for item in filtered_items},
            last_search_ms=elapsed_ms,
            last_candidate_count=len(ids),
            last_filters_active=filters_are_active(
                active_filters_from_state(st.session_state)
            ),
            last_search_response=result,
            last_search_plan=result.get("search_plan") or {},
            last_search_options=search_options,
        )

    submitted = render_search_controls()

    if submitted:
        clear_selected_entry_id()
        query = st.session_state["search_query"].strip()
        top_n = int(st.session_state["top_n"])
        filters = active_filters_from_state(st.session_state)
        search_options = search_options_from_state()
        if not query:
            st.warning("Enter a query first.")
            render_empty_state()
            return

        try:
            with st.spinner("Searching indexed media..."):
                ids, entries, scores, result, elapsed_ms = search(
                    query=query,
                    top_n=candidate_count(top_n, filters),
                    search_options=search_options,
                    include_debug=bool(st.session_state.get("search_include_debug")),
                )
        except Exception as exc:
            st.error("Search failed. Check MongoDB, Chroma, Ollama, and .env settings.")
            st.exception(exc)
            return

        result_items = result.get("items") or []
        result_item_by_id = {item["id"]: item for item in result_items}
        result_ids, result_scores, filtered_items = apply_result_filters(
            ids=ids,
            entries=entries,
            scores=scores,
            result_items_by_id=result_item_by_id,
            filters=filters,
            limit=top_n,
        )
        save_search(
            query=query,
            top_n=top_n,
            filters=filters,
            search_options=search_options,
            debug_enabled=bool(st.session_state.get("search_include_debug")),
            ids=result_ids,
            scores=result_scores,
        )
        st.session_state.update(
            last_query=query,
            last_result_ids=result_ids,
            last_result_entries=entries,
            last_result_scores=result_scores,
            last_result_items=filtered_items,
            last_result_item_by_id={item["id"]: item for item in filtered_items},
            last_search_ms=elapsed_ms,
            last_candidate_count=len(ids),
            last_filters_active=filters_are_active(filters),
            last_search_response=result,
            last_search_plan=result.get("search_plan") or {},
            last_search_options=search_options,
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
    render_search_debug_panel(st.session_state.get("last_search_response"))

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
