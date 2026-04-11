"""Mongo-backed search history for the Streamlit UI."""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

from ui.config import SEARCH_HISTORY_LIMIT
from utils.mongo import get_search_history_collection


def history_user() -> str:
    return os.getenv("MEDIA_SEARCH_HISTORY_USER", "local")


def search_key(item: dict) -> str:
    payload = {
        "query": item.get("query"),
        "filters": item.get("filters"),
        "ids": item.get("ids"),
    }
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def normalize_item(item: dict) -> dict:
    item = dict(item)
    if "_id" in item:
        item["_id"] = str(item["_id"])
    created_at = item.get("created_at")
    if isinstance(created_at, datetime):
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        item["created_at"] = created_at.astimezone().isoformat(timespec="seconds")
    return item


def load_history() -> list[dict]:
    try:
        cursor = (
            get_search_history_collection()
            .find({"history_user": history_user()})
            .sort("created_at", -1)
            .limit(SEARCH_HISTORY_LIMIT)
        )
        return [normalize_item(item) for item in cursor]
    except Exception:
        return []


def trim_history() -> None:
    collection = get_search_history_collection()
    old_ids = [
        item["_id"]
        for item in collection.find(
            {"history_user": history_user()},
            {"_id": 1},
        )
        .sort("created_at", -1)
        .skip(SEARCH_HISTORY_LIMIT)
    ]
    if old_ids:
        collection.delete_many({"_id": {"$in": old_ids}})


def clear_history() -> None:
    try:
        get_search_history_collection().delete_many({"history_user": history_user()})
    except Exception:
        pass


def save_search(
    query: str,
    top_n: int,
    filters: dict,
    ids: list[str],
    scores: list[float | None],
) -> None:
    if not ids:
        return

    item = {
        "history_user": history_user(),
        "created_at": datetime.now(timezone.utc),
        "query": query,
        "top_n": top_n,
        "filters": filters,
        "ids": [str(entry_id) for entry_id in ids],
        "scores": [None if score is None else float(score) for score in scores],
    }
    item["search_key"] = search_key(item)

    try:
        get_search_history_collection().update_one(
            {
                "history_user": item["history_user"],
                "search_key": item["search_key"],
            },
            {"$set": item},
            upsert=True,
        )
        trim_history()
    except Exception:
        pass


def history_label(item: dict, index: int) -> str:
    timestamp = str(item.get("created_at", ""))[:16].replace("T", " ")
    query = item.get("query") or "Untitled search"
    count = len(item.get("ids") or [])
    return f"{timestamp} · {query} · {count} result(s)"


def coerce_scores(value: Any) -> list[float | None]:
    if not isinstance(value, list):
        return []
    scores = []
    for score in value:
        if score is None:
            scores.append(None)
            continue
        try:
            scores.append(float(score))
        except (TypeError, ValueError):
            continue
    return scores
