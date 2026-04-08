"""Lightweight display helpers for the Streamlit UI."""

from pathlib import Path
from typing import Any


def get_entry_display_fields(entry_id: str, entry: dict) -> tuple[dict, str, str, str]:
    metadata = entry.get("metadata", {})
    file_path = metadata.get("file_path", "")
    file_name = metadata.get("file_name") or Path(file_path).name or entry_id
    ext = (metadata.get("ext") or Path(file_path).suffix.lstrip(".")).lower()
    return metadata, file_path, file_name, ext


def get_summary(entry: dict) -> str:
    return (
        entry.get("description", {})
        .get("content", {})
        .get("summary", "No summary available.")
    )


def to_jsonable(value: Any):
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
