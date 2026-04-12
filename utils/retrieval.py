"""
retrieval.py

Backend-agnostic retrieval planning, thresholding, fusion, and result shaping.
"""

import copy
import json
import re
from collections import defaultdict

from utils.date import extract_date_filter_from_query


STOPWORDS = {
    "the",
    "is",
    "in",
    "and",
    "to",
    "of",
    "a",
    "that",
    "it",
    "with",
    "as",
    "for",
    "was",
    "on",
    "by",
    "this",
    "are",
    "be",
    "or",
    "from",
}


class SearchManifest:
    SEARCH_TYPES = ("semantic", "lexical", "chrono")
    DEFAULT_PRESET = "balanced"
    DEFAULT_FOCUS = {
        "words": 50,
        "meaning": 50,
        "text": 0,
        "time": 0,
    }
    DEFAULT_ENABLED_SEARCH_TYPES = SEARCH_TYPES
    DEFAULT_RRF_SMOOTHING = 60

    EMBEDDINGS = {
        "word": {
            "family": "word",
            "key": "ollama_all_minilm_l6_v2",
            "model_name": "all-minilm:l6-v2",
            "distance_metric": "cosine",
        },
        "sentence": {
            "family": "sentence",
            "key": "ollama_mxbai_embed_large",
            "model_name": "mxbai-embed-large",
            "distance_metric": "cosine",
        },
    }

    SOURCES = {
        "content_narrative": {
            "source_id": "content_narrative",
            "collection_name": "content_narrative",
            "label": "Scene Description",
            "ui_group": "meaning",
            "fields": [
                "summary",
                "detailed_description",
                "miscellaneous",
                "background",
                "objects",
            ],
            "text_mode": "sentence",
            "embedding_family": "sentence",
            "search_types": ("semantic", "lexical"),
            "default_weights": {
                "semantic": 1.0,
                "lexical": 0.8,
            },
            "default_thresholds": {
                "semantic": {},
                "lexical": {},
            },
            "enabled_by_default": True,
            "advanced_exposure": True,
        },
        "context_narrative": {
            "source_id": "context_narrative",
            "collection_name": "context_narrative",
            "label": "Context and Event",
            "ui_group": "meaning",
            "fields": ["event", "analysis", "other_details", "vibe"],
            "text_mode": "sentence",
            "embedding_family": "sentence",
            "search_types": ("semantic", "lexical", "chrono"),
            "default_weights": {
                "semantic": 1.0,
                "lexical": 0.8,
                "chrono": 1.0,
            },
            "default_thresholds": {
                "semantic": {},
                "lexical": {},
                "chrono": {"min_specificity": 1},
            },
            "enabled_by_default": True,
            "advanced_exposure": True,
            "date_field": "master_date",
        },
        "lexical_keywords": {
            "source_id": "lexical_keywords",
            "collection_name": "lexical_keywords",
            "label": "Keywords and Attributes",
            "ui_group": "words",
            "fields": [
                "primary_category",
                "intent",
                "vibe",
                "composition",
                "background",
                "objects",
            ],
            "text_mode": "word",
            "embedding_family": "word",
            "search_types": ("semantic", "lexical"),
            "default_weights": {
                "semantic": 0.7,
                "lexical": 1.0,
            },
            "default_thresholds": {
                "semantic": {},
                "lexical": {},
            },
            "enabled_by_default": True,
            "advanced_exposure": True,
        },
        "ocr_content": {
            "source_id": "ocr_content",
            "collection_name": "ocr_content",
            "label": "Visible Text",
            "ui_group": "text",
            "fields": ["ocr_text"],
            "text_mode": "sentence",
            "embedding_family": "sentence",
            "search_types": ("semantic", "lexical"),
            "default_weights": {
                "semantic": 0.4,
                "lexical": 0.9,
            },
            "default_thresholds": {
                "semantic": {},
                "lexical": {},
            },
            "enabled_by_default": True,
            "advanced_exposure": True,
        },
        "other_data": {
            "source_id": "other_data",
            "collection_name": "other_data",
            "label": "Metadata Hints",
            "ui_group": "meaning",
            "fields": ["metadata_relevance"],
            "text_mode": "word",
            "embedding_family": "word",
            "search_types": ("semantic", "lexical"),
            "default_weights": {
                "semantic": 0.1,
                "lexical": 0.5,
            },
            "default_thresholds": {
                "semantic": {},
                "lexical": {},
            },
            "enabled_by_default": True,
            "advanced_exposure": True,
        },
    }

    PRESETS = {
        "exact": {
            "label": "Exact",
            "focus": {
                "words": 90,
                "meaning": 35,
                "text": 10,
                "time": 10,
            },
            "rrf_smoothing": 75,
            "candidate_multipliers": {
                "semantic": 8,
                "lexical": 30,
                "chrono": 30,
            },
            "semantic_thresholds": {
                "sentence": {
                    "max_distance": 0.28,
                    "tail_delta": 0.04,
                },
                "word": {
                    "max_distance": 0.24,
                    "tail_delta": 0.04,
                },
            },
            "lexical_thresholds": {
                "min_coverage": 1.0,
                "phrase_bonus": 0.3,
            },
        },
        "strict": {
            "label": "Strict",
            "focus": {
                "words": 75,
                "meaning": 45,
                "text": 15,
                "time": 15,
            },
            "rrf_smoothing": 65,
            "candidate_multipliers": {
                "semantic": 10,
                "lexical": 36,
                "chrono": 36,
            },
            "semantic_thresholds": {
                "sentence": {
                    "max_distance": 0.36,
                    "tail_delta": 0.08,
                },
                "word": {
                    "max_distance": 0.32,
                    "tail_delta": 0.08,
                },
            },
            "lexical_thresholds": {
                "min_coverage": 0.75,
                "phrase_bonus": 0.25,
            },
        },
        "balanced": {
            "label": "Balanced",
            "focus": {
                "words": 50,
                "meaning": 50,
                "text": 0,
                "time": 0,
            },
            "rrf_smoothing": 60,
            "candidate_multipliers": {
                "semantic": 10,
                "lexical": 50,
                "chrono": 50,
            },
            "semantic_thresholds": {
                "sentence": {
                    "max_distance": 0.48,
                    "tail_delta": 0.12,
                },
                "word": {
                    "max_distance": 0.42,
                    "tail_delta": 0.12,
                },
            },
            "lexical_thresholds": {
                "min_coverage": 0.5,
                "phrase_bonus": 0.2,
            },
        },
        "broad": {
            "label": "Broad",
            "focus": {
                "words": 35,
                "meaning": 70,
                "text": 10,
                "time": 10,
            },
            "rrf_smoothing": 55,
            "candidate_multipliers": {
                "semantic": 12,
                "lexical": 60,
                "chrono": 60,
            },
            "semantic_thresholds": {
                "sentence": {
                    "max_distance": 0.62,
                    "tail_delta": 0.18,
                },
                "word": {
                    "max_distance": 0.56,
                    "tail_delta": 0.18,
                },
            },
            "lexical_thresholds": {
                "min_coverage": 0.34,
                "phrase_bonus": 0.15,
            },
        },
        "explore": {
            "label": "Explore",
            "focus": {
                "words": 20,
                "meaning": 85,
                "text": 15,
                "time": 15,
            },
            "rrf_smoothing": 50,
            "candidate_multipliers": {
                "semantic": 14,
                "lexical": 75,
                "chrono": 75,
            },
            "semantic_thresholds": {
                "sentence": {
                    "max_distance": 0.78,
                    "tail_delta": 0.25,
                },
                "word": {
                    "max_distance": 0.72,
                    "tail_delta": 0.25,
                },
            },
            "lexical_thresholds": {
                "min_coverage": 0.2,
                "phrase_bonus": 0.1,
            },
        },
    }

    FOCUS_AXES = {
        "words": {
            "label": "Match words",
            "default": DEFAULT_FOCUS["words"],
        },
        "meaning": {
            "label": "Match meaning",
            "default": DEFAULT_FOCUS["meaning"],
        },
        "text": {
            "label": "Visible text",
            "default": DEFAULT_FOCUS["text"],
        },
        "time": {
            "label": "Dates and time",
            "default": DEFAULT_FOCUS["time"],
        },
    }

    CAPABILITIES = {
        "objects": {
            "label": "Find objects",
            "source_boosts": {
                "content_narrative": {
                    "semantic": 1.2,
                    "lexical": 1.1,
                },
                "lexical_keywords": {
                    "semantic": 1.1,
                    "lexical": 1.35,
                },
            },
        },
        "visible_text": {
            "label": "Visible text",
            "source_boosts": {
                "ocr_content": {
                    "semantic": 1.4,
                    "lexical": 1.6,
                }
            },
        },
        "dates": {
            "label": "Dates",
            "source_boosts": {
                "context_narrative": {
                    "semantic": 1.1,
                    "chrono": 1.6,
                }
            },
        },
        "scene_meaning": {
            "label": "Scene meaning",
            "source_boosts": {
                "content_narrative": {"semantic": 1.35},
                "context_narrative": {"semantic": 1.2},
            },
        },
        "exact_words": {
            "label": "Exact words",
            "source_boosts": {
                "content_narrative": {"lexical": 1.1},
                "context_narrative": {"lexical": 1.1},
                "lexical_keywords": {"lexical": 1.5},
                "ocr_content": {"lexical": 1.3},
            },
        },
    }


def normalize_query_text(query_text: str | list | None):
    if isinstance(query_text, list):
        return [normalize_query_text(item) for item in query_text]
    if isinstance(query_text, str):
        return query_text.strip().lower()
    return None


def tokenize_document(document: str | list | None):
    if isinstance(document, list):
        document = " ".join(str(item) for item in document if item)
    text = str(document or "").lower().replace("\n", " ")
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [token for token in text.split() if len(token) > 1 and token not in STOPWORDS]
    token_metadata = {
        "token_string": " ".join(tokens),
        "token_count": len(tokens),
    }
    if tokens:
        token_metadata["tokens"] = tokens
    return token_metadata


def get_search_manifest() -> dict:
    return {
        "presets": copy.deepcopy(SearchManifest.PRESETS),
        "focus_axes": copy.deepcopy(SearchManifest.FOCUS_AXES),
        "sources": copy.deepcopy(SearchManifest.SOURCES),
        "capabilities": copy.deepcopy(SearchManifest.CAPABILITIES),
        "search_types": list(SearchManifest.SEARCH_TYPES),
    }


def clamp_focus_value(value, default: int) -> int:
    try:
        return max(0, min(100, int(value)))
    except (TypeError, ValueError):
        return int(default)


def normalize_string_list(values) -> list[str]:
    cleaned = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            cleaned.append(text)
    return sorted(dict.fromkeys(cleaned))


def normalize_search_options(search_options: dict | None = None) -> dict:
    options = copy.deepcopy(search_options or {})
    preset = str(options.get("preset") or SearchManifest.DEFAULT_PRESET).strip().lower()
    if preset not in SearchManifest.PRESETS:
        preset = SearchManifest.DEFAULT_PRESET

    preset_focus = SearchManifest.PRESETS[preset].get("focus") or SearchManifest.DEFAULT_FOCUS
    focus_input = options.get("focus") or {}
    focus = {
        axis: clamp_focus_value(
            focus_input.get(axis, preset_focus.get(axis, SearchManifest.FOCUS_AXES[axis]["default"])),
            preset_focus.get(axis, SearchManifest.FOCUS_AXES[axis]["default"]),
        )
        for axis in SearchManifest.FOCUS_AXES
    }

    enabled_sources = normalize_string_list(options.get("enabled_sources"))
    disabled_sources = normalize_string_list(options.get("disabled_sources"))
    enabled_search_types = [
        search_type
        for search_type in normalize_string_list(
            options.get("enabled_search_types") or SearchManifest.DEFAULT_ENABLED_SEARCH_TYPES
        )
        if search_type in SearchManifest.SEARCH_TYPES
    ]
    if not enabled_search_types:
        enabled_search_types = list(SearchManifest.DEFAULT_ENABLED_SEARCH_TYPES)

    capabilities = [
        capability
        for capability in normalize_string_list(options.get("capabilities"))
        if capability in SearchManifest.CAPABILITIES
    ]
    source_overrides = options.get("source_overrides") or {}
    if not isinstance(source_overrides, dict):
        source_overrides = {}

    return {
        "preset": preset,
        "focus": focus,
        "enabled_sources": enabled_sources,
        "disabled_sources": disabled_sources,
        "enabled_search_types": enabled_search_types,
        "capabilities": capabilities,
        "source_overrides": source_overrides,
    }


def iter_query_text_values(query_texts: list) -> list[str]:
    values = []
    pending = list(reversed(query_texts))
    while pending:
        value = pending.pop()
        if isinstance(value, list):
            pending.extend(reversed(value))
            continue
        if isinstance(value, str):
            values.append(value)
    return values


def build_query_specs(query_texts: list) -> dict[str, dict]:
    query_specs = {}
    for query_text in iter_query_text_values(query_texts):
        normalized_query_text = normalize_query_text(query_text)
        if not normalized_query_text or normalized_query_text in query_specs:
            continue

        date_info = extract_date_filter_from_query(normalized_query_text)
        clean_query_text = date_info.get("clean_query_text", "")
        token_metadata = tokenize_document(clean_query_text)
        query_specs[normalized_query_text] = {
            "query_text": normalized_query_text,
            "clean_query_text": clean_query_text,
            "tokens": token_metadata.get("tokens", []),
            "token_string": token_metadata.get("token_string", ""),
            "token_count": token_metadata.get("token_count", 0),
            "date_filters": date_info.get("date_filters", []),
            "embeddings": {},
        }
    return query_specs


def make_trace_logs(include_debug: bool, trace: bool):
    return [] if include_debug or trace else None


def trace_line(trace_logs: list | None, trace: bool, message: str) -> None:
    if trace_logs is not None:
        trace_logs.append(message)
    if trace:
        print(f"[chroma] {message}")


def resolve_semantic_thresholds(preset_config: dict, source_config: dict, search_type: str) -> dict:
    if search_type != "semantic":
        return dict(source_config.get("default_thresholds", {}).get(search_type, {}))
    embedding_family = source_config.get("embedding_family")
    thresholds = copy.deepcopy(
        preset_config.get("semantic_thresholds", {}).get(embedding_family, {})
    )
    thresholds.update(source_config.get("default_thresholds", {}).get("semantic", {}))
    return thresholds


def resolve_lexical_thresholds(preset_config: dict, source_config: dict) -> dict:
    thresholds = copy.deepcopy(preset_config.get("lexical_thresholds", {}))
    thresholds.update(source_config.get("default_thresholds", {}).get("lexical", {}))
    return thresholds


def focus_weight_multiplier(source_config: dict, search_type: str, focus: dict) -> float:
    multiplier = 1.0
    if search_type == "lexical":
        multiplier *= 0.5 + (focus["words"] / 100.0)
    elif search_type == "semantic":
        multiplier *= 0.5 + (focus["meaning"] / 100.0)

    if source_config.get("ui_group") == "text":
        multiplier *= 1.0 + (focus["text"] / 100.0)
    if search_type == "chrono":
        multiplier *= 1.0 + (focus["time"] / 100.0)
    elif source_config["source_id"] == "context_narrative" and focus["time"] > 0:
        multiplier *= 1.0 + (focus["time"] / 250.0)
    return max(multiplier, 0.05)


def apply_capability_boosts(source_id: str, search_type: str, capabilities: list[str]) -> float:
    multiplier = 1.0
    for capability in capabilities:
        capability_config = SearchManifest.CAPABILITIES.get(capability) or {}
        multiplier *= float(
            ((capability_config.get("source_boosts") or {}).get(source_id, {}).get(search_type, 1.0))
            or 1.0
        )
    return max(multiplier, 0.05)


def resolve_runtime_plan(
    query_specs: dict[str, dict],
    n_results: int,
    search_options: dict | None = None,
    include_debug: bool = False,
    trace: bool = False,
    trace_logs: list | None = None,
) -> dict:
    normalized_options = normalize_search_options(search_options)
    preset_config = copy.deepcopy(SearchManifest.PRESETS[normalized_options["preset"]])
    has_date_queries = any(spec.get("date_filters") for spec in query_specs.values())
    runtime_plan = {
        "preset": normalized_options["preset"],
        "focus": normalized_options["focus"],
        "capabilities": normalized_options["capabilities"],
        "requested_search_options": normalized_options,
        "rrf_smoothing": preset_config.get("rrf_smoothing", SearchManifest.DEFAULT_RRF_SMOOTHING),
        "sources": {},
        "include_debug": bool(include_debug),
        "trace": bool(trace),
    }

    trace_line(
        trace_logs,
        trace,
        "resolved search options "
        + json.dumps(
            {
                "preset": runtime_plan["preset"],
                "focus": runtime_plan["focus"],
                "capabilities": runtime_plan["capabilities"],
                "enabled_search_types": normalized_options["enabled_search_types"],
                "enabled_sources": normalized_options["enabled_sources"],
                "disabled_sources": normalized_options["disabled_sources"],
            },
            sort_keys=True,
        ),
    )

    for source_id, source_config in SearchManifest.SOURCES.items():
        source_plan = {
            "source_id": source_id,
            "collection_name": source_config["collection_name"],
            "label": source_config["label"],
            "ui_group": source_config["ui_group"],
            "fields": list(source_config["fields"]),
            "enabled": bool(source_config.get("enabled_by_default", True)),
            "reason": "enabled_by_default",
            "search_types": {},
        }

        if normalized_options["enabled_sources"] and source_id not in normalized_options["enabled_sources"]:
            source_plan["enabled"] = False
            source_plan["reason"] = "not selected in enabled_sources"
        if source_id in normalized_options["disabled_sources"]:
            source_plan["enabled"] = False
            source_plan["reason"] = "disabled in search_options"

        source_override = normalized_options["source_overrides"].get(source_id) or {}
        if isinstance(source_override, dict) and "enabled" in source_override:
            source_plan["enabled"] = bool(source_override.get("enabled"))
            source_plan["reason"] = "overridden in source_overrides"

        requested_source_search_types = normalize_string_list(
            source_override.get("enabled_search_types") or source_override.get("search_types")
        )

        for search_type in source_config["search_types"]:
            search_plan = {
                "enabled": source_plan["enabled"],
                "reason": source_plan["reason"],
                "weight": float(source_config["default_weights"].get(search_type, 0.0)),
                "thresholds": {},
                "fetch_limit": min(
                    max(
                        n_results * int(preset_config.get("candidate_multipliers", {}).get(search_type, 20)),
                        n_results,
                    ),
                    500,
                ),
            }

            if search_type not in normalized_options["enabled_search_types"]:
                search_plan["enabled"] = False
                search_plan["reason"] = "search type disabled in search_options"
            if requested_source_search_types and search_type not in requested_source_search_types:
                search_plan["enabled"] = False
                search_plan["reason"] = "search type not selected for source override"
            if search_type == "semantic":
                search_plan["thresholds"] = resolve_semantic_thresholds(
                    preset_config, source_config, search_type
                )
                if not source_config.get("embedding_family"):
                    search_plan["enabled"] = False
                    search_plan["reason"] = "source has no embedding family"
            elif search_type == "lexical":
                search_plan["thresholds"] = resolve_lexical_thresholds(
                    preset_config, source_config
                )
            elif search_type == "chrono":
                search_plan["thresholds"] = copy.deepcopy(
                    source_config.get("default_thresholds", {}).get("chrono", {})
                )
                if not has_date_queries:
                    search_plan["enabled"] = False
                    search_plan["reason"] = "no date filters in query"

            search_plan["weight"] *= focus_weight_multiplier(source_config, search_type, runtime_plan["focus"])
            search_plan["weight"] *= apply_capability_boosts(source_id, search_type, runtime_plan["capabilities"])

            if isinstance(source_override, dict):
                source_weights = source_override.get("weights") or {}
                if search_type in source_weights:
                    try:
                        search_plan["weight"] = float(source_weights[search_type])
                    except (TypeError, ValueError):
                        pass
                source_thresholds = source_override.get("thresholds") or {}
                if isinstance(source_thresholds.get(search_type), dict):
                    search_plan["thresholds"].update(source_thresholds[search_type])
                source_fetch_limits = source_override.get("fetch_limits") or {}
                if search_type in source_fetch_limits:
                    try:
                        search_plan["fetch_limit"] = max(1, min(500, int(source_fetch_limits[search_type])))
                    except (TypeError, ValueError):
                        pass

            source_plan["search_types"][search_type] = search_plan

        runtime_plan["sources"][source_id] = source_plan
        enabled_types = [
            search_type
            for search_type, search_plan in source_plan["search_types"].items()
            if search_plan["enabled"] and search_plan["weight"] > 0
        ]
        trace_line(
            trace_logs,
            trace,
            f"source {source_id}: enabled={source_plan['enabled']} active_types={enabled_types or []} reason={source_plan['reason']}",
        )

    return runtime_plan


def parse_field_fragments(metadata: dict) -> dict:
    raw_value = metadata.get("field_fragments_json")
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def match_fields_from_fragments(query_spec: dict, metadata: dict) -> list[str]:
    field_fragments = parse_field_fragments(metadata)
    if not field_fragments:
        return []

    query_tokens = set(query_spec.get("tokens") or [])
    query_token_string = query_spec.get("token_string") or ""
    matched_fields = []
    for field_name, fragment in field_fragments.items():
        fragment_tokens = set(tokenize_document(fragment).get("tokens", []))
        fragment_string = tokenize_document(fragment).get("token_string", "")
        if query_tokens and query_tokens & fragment_tokens:
            matched_fields.append(field_name)
            continue
        if query_token_string and query_token_string in fragment_string:
            matched_fields.append(field_name)
    return sorted(dict.fromkeys(matched_fields))


def build_candidate_row(
    *,
    query_text: str,
    query_spec: dict,
    source_plan: dict,
    search_type: str,
    id_: str,
    document: str,
    metadata: dict,
    raw_score: float | None,
    raw_distance: float | None,
    normalized_score: float,
    threshold_passed: bool,
    threshold_reason: str,
    variant: str | None = None,
) -> dict:
    return {
        "id": str(id_),
        "document": str(document or ""),
        "metadata": metadata or {},
        "query_text": query_text,
        "source_id": source_plan["source_id"],
        "collection_name": source_plan["collection_name"],
        "search_type": search_type,
        "variant": variant,
        "raw_score": raw_score,
        "raw_distance": raw_distance,
        "normalized_score": float(normalized_score),
        "threshold_passed": bool(threshold_passed),
        "threshold_reason": str(threshold_reason or ""),
        "matched_fields": match_fields_from_fragments(query_spec, metadata),
    }


def summarize_source_debug(
    *,
    source_plan: dict,
    search_type: str,
    query_text: str,
    thresholds: dict,
    before_count: int,
    after_count: int,
    kept_rows: list[dict],
    search_plan: dict,
) -> dict:
    return {
        "query_text": query_text,
        "source_id": source_plan["source_id"],
        "collection_name": source_plan["collection_name"],
        "label": source_plan["label"],
        "search_type": search_type,
        "weight": search_plan["weight"],
        "thresholds": thresholds,
        "candidate_count_before": before_count,
        "candidate_count_after": after_count,
        "fetch_limit": search_plan["fetch_limit"],
        "top_ids": [row["id"] for row in kept_rows[:5]],
    }


def build_query_response(
    query_text: str | list,
    query_rows: list[dict],
    runtime_plan: dict,
    debug_rows: list[dict],
    n_results: int,
    trace_logs: list | None = None,
    trace: bool = False,
) -> dict:
    response = {
        "ids": [],
        "score": [],
        "rank": [],
        "items": [],
        "search_plan": copy.deepcopy(runtime_plan),
    }

    if isinstance(query_text, list):
        query_values = {
            value for value in (normalize_query_text(query_text) or []) if isinstance(value, str)
        }
    else:
        normalized_value = normalize_query_text(query_text)
        query_values = {normalized_value} if isinstance(normalized_value, str) else set()

    grouped_rows = defaultdict(list)
    for row in query_rows:
        if row.get("query_text") not in query_values:
            continue
        grouped_rows[row["id"]].append(row)

    rrf_smoothing = int(runtime_plan.get("rrf_smoothing", SearchManifest.DEFAULT_RRF_SMOOTHING))
    item_scores = []
    for item_id, rows in grouped_rows.items():
        merged_contributions = {}
        for row in rows:
            source_key = (row["source_id"], row["search_type"])
            source_search_plan = (
                runtime_plan["sources"]
                .get(row["source_id"], {})
                .get("search_types", {})
                .get(row["search_type"], {})
            )
            weight = float(source_search_plan.get("weight", 0.0) or 0.0)
            rrf_score = weight / (int(row["rank"]) + rrf_smoothing)
            contribution = merged_contributions.get(source_key)
            if contribution is None:
                contribution = {
                    "source_id": row["source_id"],
                    "collection_name": row["collection_name"],
                    "search_type": row["search_type"],
                    "raw_score": row["raw_score"],
                    "raw_distance": row["raw_distance"],
                    "normalized_score": row["normalized_score"],
                    "rank": row["rank"],
                    "weight": weight,
                    "rrf_score": rrf_score,
                    "threshold_passed": row["threshold_passed"],
                    "threshold_reason": row["threshold_reason"],
                    "matched_fields": list(row.get("matched_fields") or []),
                }
            else:
                contribution["rrf_score"] += rrf_score
                if row["raw_score"] is not None:
                    previous_raw_score = contribution.get("raw_score")
                    contribution["raw_score"] = max(
                        row["raw_score"],
                        previous_raw_score if previous_raw_score is not None else row["raw_score"],
                    )
                if row["raw_distance"] is not None:
                    previous_raw_distance = contribution.get("raw_distance")
                    contribution["raw_distance"] = (
                        row["raw_distance"]
                        if previous_raw_distance is None
                        else min(previous_raw_distance, row["raw_distance"])
                    )
                contribution["normalized_score"] = max(
                    contribution["normalized_score"], row["normalized_score"]
                )
                contribution["rank"] = min(contribution["rank"], row["rank"])
                contribution["matched_fields"] = sorted(
                    dict.fromkeys(
                        list(contribution.get("matched_fields") or [])
                        + list(row.get("matched_fields") or [])
                    )
                )
            merged_contributions[source_key] = contribution

        contributions = sorted(
            merged_contributions.values(),
            key=lambda contribution: (
                contribution["rrf_score"],
                contribution["weight"],
                -(contribution["raw_distance"] or float("inf"))
                if contribution.get("raw_distance") is not None
                else contribution["normalized_score"],
            ),
            reverse=True,
        )
        total_score = sum(contribution["rrf_score"] for contribution in contributions)
        source_ids = [contribution["source_id"] for contribution in contributions]
        matched_fields = sorted(
            dict.fromkeys(
                field_name
                for contribution in contributions
                for field_name in contribution.get("matched_fields") or []
            )
        )
        item_scores.append(
            {
                "id": item_id,
                "score": total_score,
                "source_ids": source_ids,
                "best_source_id": source_ids[0] if source_ids else None,
                "best_search_type": contributions[0]["search_type"] if contributions else None,
                "contributions": contributions,
                "matched_fields": matched_fields,
            }
        )

    item_scores.sort(key=lambda item: (item["score"], item["id"]), reverse=True)
    for index, item in enumerate(item_scores[:n_results], start=1):
        response["ids"].append(item["id"])
        response["score"].append(item["score"])
        response["rank"].append(index - 1)
        response["items"].append(
            {
                "id": item["id"],
                "rank": index,
                "score": item["score"],
                "source_ids": item["source_ids"],
                "best_source_id": item["best_source_id"],
                "best_search_type": item["best_search_type"],
                "contributions": item["contributions"],
                "matched_fields": item["matched_fields"],
            }
        )

    trace_line(
        trace_logs,
        trace,
        f"{query_text} fused results: {[item['id'] for item in response['items'][:5]]}",
    )

    if runtime_plan.get("include_debug"):
        response["debug"] = {
            "query_text": query_text,
            "source_stats": [row for row in debug_rows if row["query_text"] in query_values],
            "trace": list(trace_logs or []),
            "candidate_count_before_filters": len(grouped_rows),
            "result_count": len(response["items"]),
        }

    return response
