"""Search result filtering helpers."""

from datetime import date, datetime
from typing import Any

from ui.config import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS
from ui.formatting import get_entry_display_fields


DATE_KEYS = (
    "master_date",
    "estimated_date",
    "creation_date",
    "true_creation_date",
    "modification_date",
    "true_modification_date",
    "index_date",
)


def active_filters_from_state(state: dict) -> dict:
    return {
        "media_type": state.get("filter_media_type", "All"),
        "extensions": sorted(
            ext.lower().lstrip(".") for ext in state.get("filter_extensions", [])
        ),
        "min_score": float(state.get("filter_min_score", 0.0) or 0.0),
        "date_from": str(state.get("filter_date_from", "") or "").strip(),
        "date_to": str(state.get("filter_date_to", "") or "").strip(),
    }


def filters_are_active(filters: dict) -> bool:
    return any(
        [
            filters.get("media_type") != "All",
            bool(filters.get("extensions")),
            float(filters.get("min_score") or 0) > 0,
            bool(filters.get("date_from")),
            bool(filters.get("date_to")),
        ]
    )


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, dict):
        for key in ("master_date", "date", "value", "iso", "datetime"):
            parsed = parse_date(value.get(key))
            if parsed:
                return parsed
        return None

    text = str(value).strip()
    if len(text) >= 10 and text[4] == ":" and text[7] == ":":
        text = f"{text[:4]}-{text[5:7]}-{text[8:]}"
    try:
        return datetime.fromisoformat(text[:10]).date()
    except (TypeError, ValueError):
        return None


def entry_date(entry: dict) -> date | None:
    metadata = entry.get("metadata", {})
    dates = metadata.get("dates") or {}
    if isinstance(dates, dict):
        for key in DATE_KEYS:
            parsed = parse_date(dates.get(key))
            if parsed:
                return parsed

    # Fallback for older records before metadata.dates was canonicalized.
    for key in DATE_KEYS:
        parsed = parse_date(metadata.get(key))
        if parsed:
            return parsed
    return None


def entry_matches_filters(entry: dict, score: float | None, filters: dict) -> bool:
    _, _, _, ext = get_entry_display_fields("", entry)
    media_type = filters.get("media_type", "All")
    if media_type == "Images" and ext not in IMAGE_EXTENSIONS:
        return False
    if media_type == "Videos" and ext not in VIDEO_EXTENSIONS:
        return False
    if filters.get("extensions") and ext not in filters["extensions"]:
        return False
    min_score = float(filters.get("min_score") or 0)
    if min_score > 0 and (score is None or score < min_score):
        return False

    date_from = parse_date(filters.get("date_from"))
    date_to = parse_date(filters.get("date_to"))
    if date_from or date_to:
        media_date = entry_date(entry)
        if not media_date:
            return False
        if date_from and media_date < date_from:
            return False
        if date_to and media_date > date_to:
            return False
    return True


def apply_result_filters(
    ids: list[str],
    entries: dict,
    scores: list[float],
    filters: dict,
    limit: int,
) -> tuple[list[str], list[float | None]]:
    filtered_ids = []
    filtered_scores = []

    for index, entry_id in enumerate(ids):
        entry = entries.get(entry_id)
        score = scores[index] if index < len(scores) else None
        if not entry or not entry_matches_filters(entry, score, filters):
            continue
        filtered_ids.append(entry_id)
        filtered_scores.append(score)
        if len(filtered_ids) >= limit:
            break

    return filtered_ids, filtered_scores
