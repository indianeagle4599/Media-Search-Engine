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
from ui.data import (
    delete_uploaded_entry,
    entry_has_description,
    entry_is_fully_indexed,
    is_uploaded_entry,
    rename_uploaded_entry,
    uploaded_entry_file_hash,
)
from ui.formatting import get_entry_display_fields, get_summary, to_jsonable
from ui.media import get_thumbnail_data_uri, render_media


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
        for entry_id, entry in (st.session_state.get("last_result_entries") or {}).items()
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
    card_markup = ""
    if ext in IMAGE_EXTENSIONS and path and path.is_file():
        try:
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
