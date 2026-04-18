"""
app.py

Streamlit application entrypoint for search, upload, gallery, and Chroma views.
"""

import html
from time import perf_counter

from dotenv import load_dotenv
import streamlit as st

from ui.chroma_viewer import render_chroma_viewer
from ui.components import (
    default_enabled_source_ids,
    sync_search_settings_state,
    search_options_from_state,
    clear_selected_entry_id,
    detail_dialog,
    dialog_options,
    get_selected_entry_id,
    render_app_shell,
    render_results_list,
    render_results_grid,
    render_search_debug_panel,
    search_settings_dialog,
)
from ui.config import DEFAULT_TOP_N
from ui.config import FILTERED_SEARCH_MULTIPLIER, MAX_FILTERED_CANDIDATES
from ui.data import get_entries, get_query_results
from ui.data import get_search_model_status
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
SEARCH_COLUMNS = [0.8, 5.4, 0.8]
RESULTS_COLUMNS = [0.3, 6.4, 0.3]


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
        "search_enabled_sources": default_enabled_source_ids(),
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
        "search_results_view": "Grid",
        "gallery_view": "Grid",
        "gallery_filter_name": "",
        "gallery_filter_status": [],
        "gallery_filter_extensions": [],
        "last_search_response": {},
        "last_search_plan": {},
        "last_search_options": {},
        "search_status_refresh_token": 0,
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
        search_options.get("enabled_sources") or default_enabled_source_ids()
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


def render_search_controls() -> tuple[bool, object]:
    if st.session_state.pop("search_clear_requested", False):
        clear_search_results(clear_query=True)

    submitted = False
    clear_requested = False
    _, search_col, _ = st.columns(SEARCH_COLUMNS)
    with search_col:
        with st.form("search_form", clear_on_submit=False):
            st.text_input(
                "Search",
                key="search_query",
                placeholder="Try “beach sunrise”, “group photo”, or “receipt text”",
                label_visibility="collapsed",
            )

            status_col, actions_col = st.columns([3.62, 1.38], gap="small")
            with status_col:
                status_container = st.empty()
            with actions_col:
                search_button_col, clear_col, history_col, settings_col = st.columns(
                    [0.92, 0.62, 0.62, 0.62],
                    gap="small",
                )
                with search_button_col:
                    submitted = st.form_submit_button(
                        "↗",
                        type="primary",
                        key="search_submit",
                        help="Run search",
                    )
                with clear_col:
                    clear_requested = st.form_submit_button(
                        "✕",
                        key="search_clear",
                        help="Clear query and results",
                    )
                with history_col:
                    open_history = st.form_submit_button(
                        "≣",
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
        if clear_requested:
            clear_selected_entry_id()
            st.session_state["search_clear_requested"] = True
            st.rerun()

    return submitted, status_container


def render_empty_state() -> None:
    return


def clear_search_results(*, clear_query: bool) -> None:
    clear_selected_entry_id()
    if clear_query:
        st.session_state["search_query"] = ""
    st.session_state["last_query"] = ""
    st.session_state["last_result_ids"] = []
    st.session_state["last_result_entries"] = {}
    st.session_state["last_result_scores"] = []
    st.session_state["last_result_ranks"] = {}
    st.session_state["last_result_score_by_id"] = {}
    st.session_state["last_result_items"] = []
    st.session_state["last_result_item_by_id"] = {}
    st.session_state["last_search_response"] = {}
    st.session_state["last_search_plan"] = {}
    st.session_state["last_search_options"] = {}
    st.session_state["last_search_ms"] = None
    st.session_state["last_candidate_count"] = 0
    st.session_state["last_filters_active"] = False


def render_search_model_status_body(
    search_options: dict,
    refresh_token: int = 0,
) -> None:
    model_status = get_search_model_status(
        search_options=search_options,
        refresh_token=refresh_token,
    )
    st.markdown(
        '<div class="search-toolbar-status">'
        f'<div class="search-status search-status--{html.escape(model_status["state"])}" '
        f'title="{html.escape(model_status["detail"])}">'
        '<span class="search-status__dot"></span>'
        f'<span>{html.escape(model_status["label"])}</span>'
        "</div>"
        "</div>",
        unsafe_allow_html=True,
    )


if hasattr(st, "fragment"):

    @st.fragment(run_every="4s")
    def render_search_model_status_widget(
        search_options: dict,
        refresh_token: int = 0,
    ) -> None:
        render_search_model_status_body(
            search_options=search_options,
            refresh_token=refresh_token,
        )

else:

    def render_search_model_status_widget(
        search_options: dict,
        refresh_token: int = 0,
    ) -> None:
        render_search_model_status_body(
            search_options=search_options,
            refresh_token=refresh_token,
        )


def render_search_model_status(
    status_container,
    search_options: dict,
    refresh_token: int = 0,
) -> None:
    with status_container.container():
        render_search_model_status_widget(
            search_options=search_options,
            refresh_token=refresh_token,
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
    st.set_page_config(page_title="AfterSight", layout="wide")
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
            search_options = sync_search_settings_state()
            replay_query = st.session_state["search_query"].strip()
            replay_filters = active_filters_from_state(st.session_state)
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
            search_status_refresh_token=(
                int(st.session_state.get("search_status_refresh_token", 0)) + 1
            ),
        )

    submitted, search_status_container = render_search_controls()

    if submitted:
        clear_selected_entry_id()
        search_options = sync_search_settings_state()
        query = st.session_state["search_query"].strip()
        top_n = int(st.session_state["top_n"])
        filters = active_filters_from_state(st.session_state)
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
            search_status_refresh_token=(
                int(st.session_state.get("search_status_refresh_token", 0)) + 1
            ),
        )

    render_search_model_status(
        status_container=search_status_container,
        search_options=search_options_from_state(),
        refresh_token=int(st.session_state.get("search_status_refresh_token", 0)),
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

    _, results_col, _ = st.columns(RESULTS_COLUMNS)
    with results_col:
        summary_suffix = ""
        if st.session_state.get("last_filters_active"):
            summary_suffix = (
                f" · filtered from "
                f"{st.session_state.get('last_candidate_count', len(ids))} candidate(s)"
            )
        summary_col, view_col = st.columns([3.45, 2.55], gap="small")
        with summary_col:
            st.markdown(
                '<div class="results-summary">'
                f"Showing {len(ids)} result(s) for "
                f"{st.session_state.get('last_query', '')}{timing}{summary_suffix}."
                "</div>",
                unsafe_allow_html=True,
            )
        with view_col:
            label_col, options_col = st.columns([0.22, 1.78], gap="small")
            with label_col:
                st.markdown(
                    '<div class="results-view-label">View</div>',
                    unsafe_allow_html=True,
                )
            with options_col:
                result_view = st.radio(
                    "View",
                    ["Grid", "Compact list", "Details list"],
                    horizontal=True,
                    key="search_results_view",
                    label_visibility="collapsed",
                )
        if result_view == "Grid":
            render_results_grid(ids=ids, entries=entries, scores=scores)
        else:
            render_results_list(
                ids=ids,
                entries=entries,
                scores=scores,
                detailed=result_view == "Details list",
            )
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
