"""
components.py

Reusable Streamlit components for search controls, results, dialogs, and debug views.
"""

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
from ui.data import (
    delete_uploaded_entry,
    entry_has_description,
    entry_is_fully_indexed,
    is_uploaded_entry,
    manifest_source_options,
    rename_uploaded_entry,
    uploaded_entry_file_hash,
)
from ui.formatting import get_entry_display_fields, get_summary, to_jsonable
from ui.history import save_feedback
from ui.media import get_thumbnail_data_uri, render_media
from utils.retrieval import SearchManifest


DETAIL_TRIGGER_KEY_PREFIX = "result_card_detail_trigger_"
PENDING_DELETE_ENTRY_ID_KEY = "detail_pending_delete_entry_id"
PENDING_DELETE_FILE_HASH_KEY = "detail_pending_delete_file_hash"
PENDING_DELETE_FILE_NAME_KEY = "detail_pending_delete_file_name"


def get_selected_entry_id() -> str | None:
    value = st.session_state.get("selected_entry_id")
    if value:
        return str(value)
    return None


def clear_detail_action_state() -> None:
    st.session_state.pop(PENDING_DELETE_ENTRY_ID_KEY, None)
    st.session_state.pop(PENDING_DELETE_FILE_HASH_KEY, None)
    st.session_state.pop(PENDING_DELETE_FILE_NAME_KEY, None)


def set_selected_entry_id(entry_id: str) -> None:
    if get_selected_entry_id() != str(entry_id):
        clear_detail_action_state()
    st.session_state["selected_entry_id"] = str(entry_id)


def clear_selected_entry_id() -> None:
    clear_detail_action_state()
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


def source_label_map() -> dict[str, str]:
    return {source_id: label for source_id, label in manifest_source_options()}


def preset_focus_values(preset: str | None = None) -> dict[str, int]:
    preset_key = str(
        preset or st.session_state.get("search_preset") or SearchManifest.DEFAULT_PRESET
    )
    preset_config = SearchManifest.PRESETS.get(preset_key) or SearchManifest.PRESETS[
        SearchManifest.DEFAULT_PRESET
    ]
    focus = dict(SearchManifest.DEFAULT_FOCUS)
    focus.update(preset_config.get("focus") or {})
    return {
        axis: int(focus.get(axis, SearchManifest.FOCUS_AXES[axis]["default"]) or 0)
        for axis in SearchManifest.FOCUS_AXES
    }


def apply_preset_focus() -> None:
    focus = preset_focus_values()
    st.session_state["search_focus_words"] = focus["words"]
    st.session_state["search_focus_meaning"] = focus["meaning"]
    st.session_state["search_focus_text"] = focus["text"]
    st.session_state["search_focus_time"] = focus["time"]


def reset_search_settings() -> None:
    st.session_state["top_n"] = int(DEFAULT_TOP_N)
    st.session_state["search_preset"] = SearchManifest.DEFAULT_PRESET
    apply_preset_focus()
    st.session_state["search_include_debug"] = False
    st.session_state["search_enabled_search_types"] = list(SearchManifest.SEARCH_TYPES)
    st.session_state["search_enabled_sources"] = [
        source_id
        for source_id, config in SearchManifest.SOURCES.items()
        if config.get("enabled_by_default", True)
    ]
    st.session_state["search_capabilities"] = []
    st.session_state["filter_media_type"] = "All"
    st.session_state["filter_result_sources"] = []
    st.session_state["filter_extensions"] = []
    st.session_state["filter_min_score"] = 0.0
    st.session_state["filter_date_from"] = ""
    st.session_state["filter_date_to"] = ""


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
    source_labels = source_label_map()
    source_ids = list(source_labels)

    title_col, reset_col = st.columns([4.2, 1.0])
    with title_col:
        st.caption("Tune retrieval first, then optionally filter the returned results.")
    with reset_col:
        if st.button("Reset", key="search_settings_reset", use_container_width=True):
            reset_search_settings()
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
    st.select_slider(
        "Search strictness",
        options=list(SearchManifest.PRESETS),
        format_func=lambda value: SearchManifest.PRESETS[value]["label"],
        key="search_preset",
        on_change=apply_preset_focus,
    )
    focus_left, focus_right = st.columns(2)
    with focus_left:
        st.slider(
            "Match words",
            min_value=0,
            max_value=100,
            key="search_focus_words",
        )
        st.slider(
            "Visible text",
            min_value=0,
            max_value=100,
            key="search_focus_text",
        )
    with focus_right:
        st.slider(
            "Match meaning",
            min_value=0,
            max_value=100,
            key="search_focus_meaning",
        )
        st.slider(
            "Dates and time",
            min_value=0,
            max_value=100,
            key="search_focus_time",
        )
    st.checkbox(
        "Include query debug data",
        key="search_include_debug",
        help="Return detailed source thresholds, candidate counts, and fusion metadata.",
    )

    with st.expander("Advanced retrieval", expanded=False):
        st.multiselect(
            "Search types",
            list(SearchManifest.SEARCH_TYPES),
            key="search_enabled_search_types",
            help="Limit retrieval to selected search types.",
        )
        st.multiselect(
            "Capabilities",
            sorted(SearchManifest.CAPABILITIES),
            format_func=lambda value: SearchManifest.CAPABILITIES[value]["label"],
            key="search_capabilities",
            help="Boost sources that are especially useful for these intents.",
        )
        st.multiselect(
            "Sources",
            source_ids,
            format_func=lambda value: source_labels.get(value, value),
            key="search_enabled_sources",
            help="Choose which Chroma sources may participate in retrieval.",
        )

    st.divider()
    st.markdown("**Result filters**")
    st.selectbox(
        "Media type",
        ["All", "Images", "Videos"],
        key="filter_media_type",
    )
    st.multiselect(
        "Result sources",
        source_ids,
        format_func=lambda value: source_labels.get(value, value),
        key="filter_result_sources",
        help="Keep only results that were supported by at least one selected source.",
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


def render_detail_path(file_path: str) -> None:
    st.markdown(
        f'<div class="detail-path">{html.escape(file_path or "No path stored.")}</div>',
        unsafe_allow_html=True,
    )


def render_close_button(entry_id: str, close_label: str) -> None:
    if st.button(
        close_label,
        key=f"detail_close_action_{entry_id}",
        type="primary",
        use_container_width=True,
    ):
        clear_selected_entry_id()
        st.rerun()


def sync_renamed_entries(entry_ids: list[str], file_name: str) -> None:
    entries = st.session_state.get("last_result_entries")
    if not isinstance(entries, dict):
        return

    for entry_id in entry_ids:
        entry = entries.get(str(entry_id))
        if not isinstance(entry, dict):
            continue
        metadata = dict(entry.get("metadata") or {})
        metadata["file_name"] = file_name
        entry["metadata"] = metadata


def sync_deleted_entries(entry_ids: list[str]) -> None:
    deleted_ids = {str(entry_id) for entry_id in entry_ids}
    if not deleted_ids:
        return

    previous_ids = list(st.session_state.get("last_result_ids", []))
    previous_scores = list(st.session_state.get("last_result_scores", []))
    kept_pairs = [
        (entry_id, previous_scores[index] if index < len(previous_scores) else None)
        for index, entry_id in enumerate(previous_ids)
        if entry_id not in deleted_ids
    ]
    st.session_state["last_result_ids"] = [entry_id for entry_id, _ in kept_pairs]
    st.session_state["last_result_scores"] = [score for _, score in kept_pairs]
    st.session_state["last_result_entries"] = {
        entry_id: entry
        for entry_id, entry in (
            st.session_state.get("last_result_entries") or {}
        ).items()
        if entry_id not in deleted_ids
    }
    update_result_indexes(
        st.session_state["last_result_ids"],
        st.session_state["last_result_scores"],
    )


def render_uploaded_management_section(
    entry_id: str,
    entry: dict,
    file_name: str,
    close_label: str,
    file_path: str,
) -> None:
    if not is_uploaded_entry(entry):
        render_close_button(entry_id, close_label)
        render_detail_path(file_path)
        return

    file_hash = uploaded_entry_file_hash(entry)
    if not file_hash:
        render_close_button(entry_id, close_label)
        render_detail_path(file_path)
        return

    st.divider()
    st.markdown("**Manage upload**")
    st.caption("Display name")

    rename_key = f"uploaded_rename_name_{entry_id}"
    if rename_key not in st.session_state:
        st.session_state[rename_key] = file_name

    rename_col, save_col = st.columns([4.6, 1.4], gap="small")
    with rename_col:
        st.text_input(
            "Display name",
            key=rename_key,
            label_visibility="collapsed",
            placeholder="Uploaded file name",
        )
    with save_col:
        rename_submitted = st.button(
            "Save name",
            key=f"uploaded_rename_submit_{entry_id}",
            use_container_width=True,
        )

    if rename_submitted:
        try:
            renamed_entry_ids, cleaned_name = rename_uploaded_entry(
                file_hash,
                st.session_state.get(rename_key, ""),
            )
        except Exception as exc:
            st.error("Renaming the uploaded file failed.")
            st.exception(exc)
        else:
            sync_renamed_entries(renamed_entry_ids, cleaned_name)
            st.session_state[rename_key] = cleaned_name
            clear_detail_action_state()
            st.rerun()

    action_col, delete_col = st.columns(2, gap="small")
    with action_col:
        render_close_button(entry_id, close_label)
    with delete_col:
        if st.button(
            "Delete upload",
            key=f"uploaded_delete_prompt_{entry_id}",
            use_container_width=True,
        ):
            st.session_state[PENDING_DELETE_ENTRY_ID_KEY] = str(entry_id)
            st.session_state[PENDING_DELETE_FILE_HASH_KEY] = file_hash
            st.session_state[PENDING_DELETE_FILE_NAME_KEY] = file_name
            st.rerun()

    render_detail_path(file_path)


def render_delete_confirm_body(entry_id: str, entry: dict) -> None:
    _, file_path, file_name, _ = get_entry_display_fields(entry_id, entry)
    file_hash = uploaded_entry_file_hash(entry)
    pending_file_name = str(
        st.session_state.get(PENDING_DELETE_FILE_NAME_KEY) or file_name
    )

    if not file_hash:
        clear_detail_action_state()
        st.warning("Uploaded file details are missing.")
        return

    st.subheader("Delete upload")
    st.write(f"Permanently delete `{pending_file_name}`?")
    st.warning(
        "This removes the uploaded file from disk and deletes its related MongoDB and Chroma records."
    )
    render_detail_path(file_path)

    action_col, delete_col = st.columns(2, gap="small")
    with action_col:
        if st.button(
            "Back to details",
            key=f"uploaded_delete_cancel_{entry_id}",
            type="primary",
            use_container_width=True,
        ):
            clear_detail_action_state()
            st.rerun()
    with delete_col:
        if st.button(
            "Delete permanently",
            key=f"uploaded_delete_confirm_action_{entry_id}",
            use_container_width=True,
        ):
            try:
                deleted_entry_ids = delete_uploaded_entry(file_hash)
            except Exception as exc:
                st.error("Deleting the uploaded file failed.")
                st.exception(exc)
            else:
                sync_deleted_entries(deleted_entry_ids)
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
    result_item = (st.session_state.get("last_result_item_by_id") or {}).get(
        entry_id
    ) or {}

    st.subheader(file_name)
    st.caption(
        detail_caption(
            rank, score, "Indexed media" if fully_indexed else "Pending indexing"
        )
    )

    left, right = st.columns([1.15, 0.85])
    with left:
        render_media(file_path=file_path, ext=ext)

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
        render_uploaded_management_section(
            entry_id=entry_id,
            entry=entry,
            file_name=file_name,
            close_label=close_label,
            file_path=file_path,
        )

        if result_item:
            render_feedback_section(
                entry_id=entry_id,
                rank=rank,
                score=score,
                result_item=result_item,
            )

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

    if result_item:
        with st.expander("Search provenance", expanded=False):
            st.json(
                to_jsonable(
                    {
                        "best_source_id": result_item.get("best_source_id"),
                        "best_search_type": result_item.get("best_search_type"),
                        "matched_fields": result_item.get("matched_fields") or [],
                        "contributions": result_item.get("contributions") or [],
                    }
                ),
                expanded=False,
            )


def render_detail_body(
    entry_id: str | None = None,
    entry: dict | None = None,
    rank: int | None = None,
    score: float | None = None,
    close_label: str = "Close details",
) -> None:
    if not entry_id or not entry:
        st.warning("Details could not be loaded.")
        return

    file_hash = uploaded_entry_file_hash(entry)
    if (
        st.session_state.get(PENDING_DELETE_ENTRY_ID_KEY) == str(entry_id)
        and st.session_state.get(PENDING_DELETE_FILE_HASH_KEY) == file_hash
    ):
        render_delete_confirm_body(entry_id=entry_id, entry=entry)
        return

    render_indexed_detail_body(
        entry_id=entry_id,
        entry=entry,
        rank=rank,
        score=score,
        close_label=close_label,
    )


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
    title = html.escape(file_name)
    overlay_markup = (
        f'<div class="result-card__overlay">'
        f'<div class="result-card__overlay-title">{title}</div>'
        f'<div class="result-card__overlay-divider"></div>'
        f"{overlay_details_html}</div>"
        if overlay_details_html
        else f'<div class="result-card__title">{title}</div>'
    )
    card_markup = ""
    if ext in IMAGE_EXTENSIONS and path and path.is_file():
        try:
            preview = get_thumbnail_data_uri(str(path), path.stat().st_mtime_ns)
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
            f'<div class="result-placeholder">{label}</div>{overlay_markup}</div>'
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


def render_result_card(
    entry_id: str,
    entry: dict,
    rank: int,
    score: float | None,
) -> None:
    _, file_path, file_name, ext = get_entry_display_fields(entry_id, entry)
    score_text = "Score unavailable" if score is None else f"Score {score:.4f}"
    result_item = (st.session_state.get("last_result_item_by_id") or {}).get(
        entry_id
    ) or {}
    best_source = source_label_map().get(result_item.get("best_source_id"), "")
    overlay_lines = []
    if best_source:
        overlay_lines.append(
            f'<div class="result-card__overlay-meta">Best source: '
            f"{html.escape(best_source)}</div>"
        )
    if score is not None:
        overlay_lines.append(
            f'<div class="result-card__overlay-meta">Score: {score:.4f}</div>'
        )
    render_media_card(
        file_path=file_path,
        file_name=file_name,
        ext=ext,
        rank=rank,
        detail_entry_id=entry_id,
        detail_title=f"{file_name} · {score_text}",
        overlay_details_html="".join(overlay_lines) or None,
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


def render_feedback_section(
    entry_id: str,
    rank: int | None,
    score: float | None,
    result_item: dict,
) -> None:
    query = str(st.session_state.get("last_query") or "").strip()
    if not query:
        return

    st.divider()
    st.markdown("**Relevance feedback**")
    positive_col, negative_col = st.columns(2, gap="small")
    if positive_col.button(
        "Relevant",
        key=f"feedback_positive_{entry_id}",
        use_container_width=True,
    ):
        save_feedback(
            query=query,
            entry_id=entry_id,
            feedback="relevant",
            rank=rank,
            score=score,
            search_options=st.session_state.get("last_search_options"),
            search_plan=st.session_state.get("last_search_plan"),
            source_ids=result_item.get("source_ids"),
            contributions=result_item.get("contributions"),
        )
        st.session_state["last_feedback_notice"] = (
            f"Saved relevant feedback for {entry_id}"
        )
    if negative_col.button(
        "Not relevant",
        key=f"feedback_negative_{entry_id}",
        use_container_width=True,
    ):
        save_feedback(
            query=query,
            entry_id=entry_id,
            feedback="not_relevant",
            rank=rank,
            score=score,
            search_options=st.session_state.get("last_search_options"),
            search_plan=st.session_state.get("last_search_plan"),
            source_ids=result_item.get("source_ids"),
            contributions=result_item.get("contributions"),
        )
        st.session_state["last_feedback_notice"] = (
            f"Saved negative feedback for {entry_id}"
        )

    notice = st.session_state.get("last_feedback_notice")
    if notice:
        st.caption(notice)


def render_search_debug_panel(result_response: dict | None) -> None:
    if not isinstance(result_response, dict) or not result_response:
        return

    items = result_response.get("items") or []
    debug = result_response.get("debug") or {}
    search_plan = result_response.get("search_plan") or {}
    contribution_rows = []
    for item in items[:10]:
        for contribution in item.get("contributions") or []:
            contribution_rows.append(
                {
                    "result_id": item.get("id"),
                    "best_source_id": item.get("best_source_id"),
                    "source_id": contribution.get("source_id"),
                    "search_type": contribution.get("search_type"),
                    "weight": contribution.get("weight"),
                    "rank": contribution.get("rank"),
                    "rrf_score": contribution.get("rrf_score"),
                    "raw_score": contribution.get("raw_score"),
                    "raw_distance": contribution.get("raw_distance"),
                    "matched_fields": ", ".join(
                        contribution.get("matched_fields") or []
                    ),
                }
            )

    with st.expander("Query debug", expanded=False):
        st.markdown("**Resolved plan**")
        st.json(to_jsonable(search_plan), expanded=False)
        if debug.get("source_stats"):
            st.markdown("**Source stats**")
            st.dataframe(debug["source_stats"], use_container_width=True)
        else:
            st.info("Debug data was not collected for this search.")
        if contribution_rows:
            st.markdown("**Top contribution rows**")
            st.dataframe(contribution_rows, use_container_width=True)
        if debug.get("trace"):
            st.markdown("**Trace**")
            st.code(
                "\n".join(str(line) for line in debug.get("trace") or []),
                language="text",
            )
