"""
chroma.py

Contains utilities to create, update and use a chromadb for storing and querying from the descriptions of all images.
"""

import json, re, calendar
from datetime import datetime, timezone

import chromadb
from chromadb.utils.batch_utils import create_batches
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
)

import pandas as pd

STOPWORDS = set(
    [
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
    ]
)

DATE_MASK_PATTERN = re.compile(
    r"([0-9Xx]{4}-[0-9Xx]{2}-[0-9Xx]{2}[Tt][0-9Xx]{2}:[0-9Xx]{2}:[0-9Xx]{2})"
)
SEMANTIC_DATE_MASKS = {
    # Existing Times of Day
    "midnight": ("XXXX-XX-XXT00:00:00", "XXXX-XX-XXT00:59:59"),
    "dawn": ("XXXX-XX-XXT05:00:00", "XXXX-XX-XXT06:59:59"),
    "sunrise": ("XXXX-XX-XXT06:00:00", "XXXX-XX-XXT07:59:59"),
    "morning": ("XXXX-XX-XXT06:00:00", "XXXX-XX-XXT11:59:59"),
    "noon": ("XXXX-XX-XXT12:00:00", "XXXX-XX-XXT12:59:59"),
    "afternoon": ("XXXX-XX-XXT13:00:00", "XXXX-XX-XXT16:59:59"),
    "golden hour": ("XXXX-XX-XXT17:00:00", "XXXX-XX-XXT18:59:59"),
    "sunset": ("XXXX-XX-XXT18:00:00", "XXXX-XX-XXT18:59:59"),
    "twilight": ("XXXX-XX-XXT18:00:00", "XXXX-XX-XXT19:59:59"),
    "evening": ("XXXX-XX-XXT17:00:00", "XXXX-XX-XXT20:59:59"),
    "night": ("XXXX-XX-XXT20:00:00", "XXXX-XX-XXT23:59:59"),
    # Business / Extended Times
    "business hours": ("XXXX-XX-XXT09:00:00", "XXXX-XX-XXT17:59:59"),
    "working hours": ("XXXX-XX-XXT09:00:00", "XXXX-XX-XXT17:59:59"),
    "lunch": ("XXXX-XX-XXT12:00:00", "XXXX-XX-XXT13:59:59"),
    "lunchtime": ("XXXX-XX-XXT12:00:00", "XXXX-XX-XXT13:59:59"),
    # Decades & Centuries
    "70s": ("197X-XX-XXTXX:XX:XX", "197X-XX-XXTXX:XX:XX"),
    "1970s": ("197X-XX-XXTXX:XX:XX", "197X-XX-XXTXX:XX:XX"),
    "80s": ("198X-XX-XXTXX:XX:XX", "198X-XX-XXTXX:XX:XX"),
    "1980s": ("198X-XX-XXTXX:XX:XX", "198X-XX-XXTXX:XX:XX"),
    "90s": ("199X-XX-XXTXX:XX:XX", "199X-XX-XXTXX:XX:XX"),
    "1990s": ("199X-XX-XXTXX:XX:XX", "199X-XX-XXTXX:XX:XX"),
    "2000s": ("200X-XX-XXTXX:XX:XX", "200X-XX-XXTXX:XX:XX"),
    "2010s": ("201X-XX-XXTXX:XX:XX", "201X-XX-XXTXX:XX:XX"),
    "2020s": ("202X-XX-XXTXX:XX:XX", "202X-XX-XXTXX:XX:XX"),
    "20th century": ("19XX-XX-XXTXX:XX:XX", "19XX-XX-XXTXX:XX:XX"),
    "21st century": ("20XX-XX-XXTXX:XX:XX", "20XX-XX-XXTXX:XX:XX"),
    # Day-Level Month Segments
    "start of month": ("XXXX-XX-01TXX:XX:XX", "XXXX-XX-10TXX:XX:XX"),
    "early month": ("XXXX-XX-01TXX:XX:XX", "XXXX-XX-10TXX:XX:XX"),
    "mid month": ("XXXX-XX-11TXX:XX:XX", "XXXX-XX-20TXX:XX:XX"),
    "middle of month": ("XXXX-XX-11TXX:XX:XX", "XXXX-XX-20TXX:XX:XX"),
    "end of month": ("XXXX-XX-21TXX:XX:XX", "XXXX-XX-31TXX:XX:XX"),
    "late month": ("XXXX-XX-21TXX:XX:XX", "XXXX-XX-31TXX:XX:XX"),
    # Seasons (Non-Wrapping)
    "spring": ("XXXX-03-XXTXX:XX:XX", "XXXX-05-XXTXX:XX:XX"),
    "summer": ("XXXX-06-XXTXX:XX:XX", "XXXX-08-XXTXX:XX:XX"),
    "autumn": ("XXXX-09-XXTXX:XX:XX", "XXXX-11-XXTXX:XX:XX"),
    "fall": ("XXXX-09-XXTXX:XX:XX", "XXXX-11-XXTXX:XX:XX"),
    # Holidays & Notable Periods
    "holiday season": ("XXXX-11-20TXX:XX:XX", "XXXX-12-31TXX:XX:XX"),
    "christmas": ("XXXX-12-24TXX:XX:XX", "XXXX-12-26TXX:XX:XX"),
    "new year's eve": ("XXXX-12-31TXX:XX:XX", "XXXX-12-31TXX:XX:XX"),
    "new year's day": ("XXXX-01-01TXX:XX:XX", "XXXX-01-01TXX:XX:XX"),
}

field_type_map = {
    # Sentence-like
    "sentence": [
        "summary",
        "detailed_description",
        "ocr_text",
        "miscellaneous",
        "event",
        "analysis",
        "metadata_relevance",
        "other_details",
    ],
    # List-like
    "list": ["objects", "vibe"],
    # Word-like
    "word": ["background", "primary_category", "intent", "composition"],
    # Absolute (non-semantic)
    "absolute": ["estimated_date"],
}
field_type_rev_map = {v_i: k for k, v in field_type_map.items() for v_i in v}

collection_dict = {
    "content_narrative": [
        "summary",
        "detailed_description",
        "miscellaneous",
        "background",
        "objects",
    ],
    "context_narrative": ["event", "analysis", "other_details", "vibe"],
    "lexical_keywords": [
        "primary_category",
        "intent",
        "vibe",
        "composition",
        "background",
        "objects",
    ],
    "ocr_content": ["ocr_text"],
    "other_data": ["metadata_relevance"],
}
field_weight_dict = {
    # semantic search weights
    "content_narrative": 1.0,
    "context_narrative": 1.0,
    "lexical_keywords": 0.7,
    "ocr_content": 0.4,
    "other_data": 0.1,
    # lexical search weights
    "content_narrative_lexical": 0.8,
    "context_narrative_lexical": 0.8,
    "lexical_keywords_lexical": 1.0,
    "ocr_content_lexical": 0.9,
    "other_data_lexical": 0.5,
    # chronological search weights
    "context_narrative_chrono": 1.0,
}

collection_type_map = {
    # Bigger, more narrative, contextual fields that may benefit from a more powerful embedding model
    "sentence": ["content_narrative", "context_narrative", "ocr_content"],
    # Smaller, more discrete fields that may be well-handled by a lighter embedding model
    # and a hybrid search strategy that combines lexical and semantic search
    "word": ["lexical_keywords", "other_data"],
    # Absolute (non-semantic)
    "absolute": [
        "estimated_date",
        "master_date",
        "creation_date",
        "modification_date",
        "date_reliability",
        "index_date",
        "estimated_ts",
        "master_ts",
        "creation_ts",
        "modification_ts",
        "index_ts",
    ],
}
collection_type_rev_map = {v_i: k for k, v in collection_type_map.items() for v_i in v}

minilm_ef = DefaultEmbeddingFunction()  # Currently "ONNXMiniLM_L6_V2" as of 23/03/2026
ollama_ef = OllamaEmbeddingFunction(
    model_name="mxbai-embed-large",
)

collection_ef_map = {
    "sentence": ollama_ef,
    "list": minilm_ef,
    "word": minilm_ef,
}


def normalize_query_text(query_text: str):
    if isinstance(query_text, list):
        return [normalize_query_text(q) for q in query_text]
    elif isinstance(query_text, str):
        return query_text.strip().lower()


def tokenize_document(document: str):
    text = document.lower().replace("\n", " ")
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [t for t in text.split() if len(t) > 1 and t not in STOPWORDS]
    token_string = " ".join(tokens)
    token_count = len(tokens)

    return {
        "tokens": tokens,
        "token_string": token_string,
        "token_count": token_count,
    }


def prep_dict_for_upsert(field_dict: dict):
    ids = []
    documents = []
    for key, val in field_dict.items():
        if not val:
            continue

        if isinstance(val, list):
            for i, v in enumerate(val):
                if v:
                    ids.append(f"{key}_item_{i+1}")
                    documents.append(str(v))

        elif isinstance(val, dict):
            for k, v in val.items():
                if v:
                    ids.append(f"{key}_{k}")
                    documents.append(str(v))

        else:
            ids.append(str(key))
            documents.append(str(val))
    return ids, documents


def combine_fields(extracted_fields: dict, field_list: list[str]):
    final_field_value = []
    for field_name in field_list:
        field_item = extracted_fields.get(field_name)
        if not field_item:
            continue
        elif isinstance(field_item, str):
            if field_type_rev_map.get(field_name) == "sentence":
                final_field_value.append(f"{field_item}\n")
            elif field_type_rev_map.get(field_name) == "word":
                final_field_value.append(f"{field_item}, ")
        elif isinstance(field_item, list):
            final_field_value.append(", ".join([str(i) for i in field_item]) + "\n")

    return "".join(final_field_value).strip()


def combine_extracted_fields(extracted_fields: dict, combination_dict: dict):
    combined_fields = {}
    for collection_name, field_list in combination_dict.items():
        combined_fields[collection_name] = combine_fields(extracted_fields, field_list)
    return combined_fields


def split_date(date_str: str):
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "second": dt.second,
            "microsecond": dt.microsecond,
            "ts": float(dt.timestamp()),
            "tz_offset_minutes": (
                int(dt.utcoffset().total_seconds() // 60) if dt.utcoffset() else 0
            ),
        }
    except Exception as e:
        print(f"Error splitting date string '{date_str}' to smaller parts:", e)
        return None


def date_dict_to_ts(date_dict: dict):
    ts_dict = {}
    for key, date_str in date_dict.items():
        if not date_str:
            continue

        date_parts = split_date(date_str)
        if not date_parts:
            continue

        for date_part in date_parts:
            date_item_key = key.replace("date", date_part)
            ts_dict[date_item_key] = date_parts[date_part]
    return ts_dict


def extract_metadata_fields(metadata_object: dict):
    date_object = metadata_object.get("dates") or {}
    clean_date_object = {
        "master_date": date_object.get("master_date"),
        "creation_date": date_object.get("true_creation_date"),
        "modification_date": date_object.get("true_modification_date"),
        "index_date": date_object.get("index_date"),
    }
    ts_object = date_dict_to_ts(clean_date_object)
    clean_date_object["date_reliability"] = (
        date_object.get("date_reliability") or "unknown"
    )
    return {
        "absolute": {
            # From metadata object
            **clean_date_object,
            **ts_object,
            # From extracted_metadata field
            ## None yet. Need to either add Exif tags or clean up io.py for cleaner metadata extraction
        },
    }


def extract_description_fields(description_object: dict):
    content_object = description_object.get("content") or {}
    context_object = description_object.get("context") or {}

    extracted_fields = {
        # Will still need further refinement to handle OCR in "contains"
        # and separate out shorter, sentence-like texts from longer, paragraph-like texts
        # From content
        "summary": content_object.get("summary"),
        "detailed_description": content_object.get("detailed_description"),
        "ocr_text": content_object.get("text"),
        "miscellaneous": content_object.get("miscellaneous"),
        # From context
        "event": context_object.get("event"),
        "analysis": context_object.get("analysis"),
        "metadata_relevance": context_object.get("metadata_relevance"),
        "other_details": context_object.get("other_details"),
        # From content
        "objects": content_object.get("objects"),
        "vibe": content_object.get("vibe"),
        # From content
        "background": content_object.get("background"),
        # From context
        "primary_category": context_object.get("primary_category"),
        "intent": context_object.get("intent"),  # May need to handle "/"
        "composition": context_object.get("composition"),
        "estimated_date": context_object.get("estimated_date"),
        **date_dict_to_ts({"estimated_date": context_object.get("estimated_date")}),
    }

    combined_fields = combine_extracted_fields(
        extracted_fields=extracted_fields, combination_dict=collection_dict
    )
    final_extracted_fields = {}
    for field_name, field_value in combined_fields.items():
        field_type = collection_type_rev_map.get(field_name)
        if not field_type:
            continue
        if final_extracted_fields.get(field_type):
            final_extracted_fields[field_type][field_name] = field_value
        else:
            final_extracted_fields[field_type] = {field_name: field_value}
    return final_extracted_fields


def classify_by_field_types(entries: dict, verbose: bool = False):
    if verbose:
        print("\n" * 4, "==" * 40, "\n" * 2)

    class_wise_db_dict = {
        "sentence": {},
        "list": {},
        "word": {},
        "absolute": {},
    }
    metadata_dict = {}
    for entry_hash, entry_object in entries.items():
        metadata = entry_object.get("metadata")
        if metadata:
            extracted_fields = extract_metadata_fields(metadata_object=metadata)
            description = entry_object.get("description")
            if description:
                extracted_fields = merge_dicts(
                    extracted_fields,
                    extract_description_fields(description_object=description),
                )
                if verbose:
                    print(json.dumps(extracted_fields, indent=2))
            for field_type, field_object in extracted_fields.items():
                if field_type == "absolute":
                    metadata_dict[entry_hash] = field_object
                    continue
                for field_name, field_value in field_object.items():
                    if field_name in class_wise_db_dict[field_type]:
                        class_wise_db_dict[field_type][field_name][
                            entry_hash
                        ] = field_value
                    else:
                        class_wise_db_dict[field_type][field_name] = {
                            entry_hash: field_value
                        }
    class_wise_db_dict["absolute"] = metadata_dict
    if verbose:
        print(json.dumps(class_wise_db_dict, indent=2))

    return class_wise_db_dict


def merge_dicts(dict1: dict, dict2: dict):
    # Need to resolve other iterables as well
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
            dict1[key] = merge_dicts(dict1[key], value)
        elif key in dict1 and isinstance(dict1[key], list) and isinstance(value, list):
            dict1[key].extend(value)
        else:
            dict1[key] = value
    return dict1


def upsert_batch_to_collection(collection, batches):
    for batch_ids, _, batch_metadatas, batch_documents in batches:
        if batch_metadatas is None:
            batch_metadatas = [None] * len(batch_ids)
        for i in range(len(batch_ids)):
            tokens_dict = tokenize_document(batch_documents[i])
            if batch_metadatas and isinstance(batch_metadatas[i], dict):
                batch_metadatas[i].update(tokens_dict)
            else:
                batch_metadatas[i] = tokens_dict
        collection.upsert(
            ids=batch_ids,
            metadatas=batch_metadatas,
            documents=batch_documents,
        )


def populate_db(
    entries: dict,
    chroma_client: chromadb.PersistentClient,
    overwrite: bool = False,
    verbose: bool = False,
):
    class_wise_db_dict = classify_by_field_types(entries, verbose)

    for field_type, field_object in class_wise_db_dict.items():
        for field_name, field_dict in field_object.items():
            if field_type in ["sentence", "list", "word"]:
                collection_kwargs = {
                    "name": field_name,
                    "configuration": {"hnsw": {"space": "cosine"}},
                    "get_or_create": True,
                    "embedding_function": collection_ef_map.get(field_type),
                }
                collection = chroma_client.create_collection(**collection_kwargs)
                ids, documents = prep_dict_for_upsert(field_dict)

                if not ids or not documents:
                    continue

                if not overwrite:
                    existing_ids = set(
                        collection.get(ids=ids, include=[]).get("ids", [])
                    )  # Check if documents already exist
                    missing_ids = set(ids) - existing_ids
                    if missing_ids:
                        new_ids, new_documents = [], []
                        for id, document in zip(ids, documents):
                            if id in missing_ids:
                                new_ids.append(id)
                                new_documents.append(document)
                        ids, documents = new_ids, new_documents
                    else:
                        continue

                if field_type == "sentence" and field_name == "context_narrative":
                    # Upsert absolute fields to a one collection to allow simpler searches
                    absolute_fields = class_wise_db_dict.get("absolute", {})

                    metadatas_list = [absolute_fields.get(id) for id in ids]
                    batches = create_batches(
                        chroma_client,
                        ids=ids,
                        metadatas=metadatas_list,
                        documents=documents,
                    )
                else:
                    batches = create_batches(
                        chroma_client, ids=ids, documents=documents
                    )
                upsert_batch_to_collection(collection, batches)


def count_mask_specificity(start_mask: str, end_mask: str):
    if not start_mask or not end_mask:
        return 0

    s_parts = re.fullmatch(
        r"([0-9X]{4})-([0-9X]{2})-([0-9X]{2})T([0-9X]{2}):([0-9X]{2}):([0-9X]{2})",
        start_mask.upper(),
    )
    e_parts = re.fullmatch(
        r"([0-9X]{4})-([0-9X]{2})-([0-9X]{2})T([0-9X]{2}):([0-9X]{2}):([0-9X]{2})",
        end_mask.upper(),
    )
    if not s_parts or not e_parts:
        return 0

    score = 0
    for s, e in zip(s_parts.groups(), e_parts.groups()):
        s_all_x = set(s) == {"X"}
        e_all_x = set(e) == {"X"}

        if s_all_x and e_all_x:
            continue
        if "X" not in s and "X" not in e and s == e:
            score += 2
        else:
            score += 1
    return score


def _parse_mask_groups(date_mask: str):
    m = re.fullmatch(
        r"([0-9X]{4})-([0-9X]{2})-([0-9X]{2})T([0-9X]{2}):([0-9X]{2}):([0-9X]{2})",
        date_mask.upper(),
    )
    if not m:
        raise ValueError(
            "date_mask must match YYYY-MM-DDTHH:MM:SS using digits and/or X"
        )
    return m.groups()


def _masked_int_bounds(mask: str, lo_default: int, hi_default: int):
    mask = mask.upper()
    if set(mask) == {"X"}:
        return lo_default, hi_default
    lo = int(mask.replace("X", "0"))
    hi = int(mask.replace("X", "9"))
    return lo, hi


def mask_to_datetime_bounds(date_mask: str):
    year_s, month_s, day_s, hour_s, minute_s, second_s = _parse_mask_groups(date_mask)

    year_lo, year_hi = _masked_int_bounds(year_s, 1, 9999)
    month_lo, month_hi = _masked_int_bounds(month_s, 1, 12)
    hour_lo, hour_hi = _masked_int_bounds(hour_s, 0, 23)
    minute_lo, minute_hi = _masked_int_bounds(minute_s, 0, 59)
    second_lo, second_hi = _masked_int_bounds(second_s, 0, 59)

    year_lo = max(1, min(year_lo, 9999))
    year_hi = max(1, min(year_hi, 9999))
    month_lo = max(1, min(month_lo, 12))
    month_hi = max(1, min(month_hi, 12))

    max_day_lo = calendar.monthrange(year_lo, month_lo)[1]
    max_day_hi = calendar.monthrange(year_hi, month_hi)[1]

    if set(day_s.upper()) == {"X"}:
        day_lo, day_hi = 1, max_day_hi
    else:
        day_lo = int(day_s.upper().replace("X", "0"))
        day_hi = int(day_s.upper().replace("X", "9"))
        day_lo = max(1, min(day_lo, max_day_lo))
        day_hi = max(1, min(day_hi, max_day_hi))

    start_dt = datetime(
        year_lo,
        month_lo,
        day_lo,
        hour_lo,
        minute_lo,
        second_lo,
        tzinfo=timezone.utc,
    )
    end_dt = datetime(
        year_hi,
        month_hi,
        day_hi,
        hour_hi,
        minute_hi,
        second_hi,
        tzinfo=timezone.utc,
    )

    if start_dt > end_dt:
        raise ValueError(f"Invalid masked datetime range derived from {date_mask}")

    return start_dt, end_dt


def mask_to_ts_bounds(date_mask: str):
    start_dt, end_dt = mask_to_datetime_bounds(date_mask)
    return float(start_dt.timestamp()), float(end_dt.timestamp())


def build_timestamp_where_clause(date_field: str, start_mask: str, end_mask: str):
    valid_fields = {
        "estimated_date": "estimated_ts",
        "creation_date": "creation_ts",
        "modification_date": "modification_ts",
        "master_date": "master_ts",
    }
    if date_field not in valid_fields:
        raise ValueError(f"Invalid date_field: {date_field}")

    ts_field = valid_fields[date_field]
    start_ts, _ = mask_to_ts_bounds(start_mask)
    _, end_ts = mask_to_ts_bounds(end_mask)

    if start_ts > end_ts:
        raise ValueError(
            f"Invalid timestamp range: start_mask={start_mask}, end_mask={end_mask}"
        )

    return {
        "$and": [
            {ts_field: {"$gte": start_ts}},
            {ts_field: {"$lte": end_ts}},
        ]
    }


def combine_where_clauses(*clauses):
    flat = []
    for clause in clauses:
        if not clause:
            continue
        if isinstance(clause, dict) and "$and" in clause and len(clause) == 1:
            flat.extend(clause["$and"])
        else:
            flat.append(clause)

    if not flat:
        return None
    if len(flat) == 1:
        return flat[0]
    return {"$and": flat}


def extract_date_filter_from_query(query_text: str):
    q = query_text.strip()
    q_lower = q.lower()

    # 1) semantic phrases first
    for key in sorted(SEMANTIC_DATE_MASKS, key=len, reverse=True):
        if key in q_lower:
            clean = re.sub(re.escape(key), "", q, flags=re.IGNORECASE).strip()
            clean = re.sub(r"\s+", " ", clean).strip()
            return {
                "start_mask": SEMANTIC_DATE_MASKS[key][0].upper(),
                "end_mask": SEMANTIC_DATE_MASKS[key][1].upper(),
                "clean_query_text": clean.lower(),
                "matched_semantic_key": key,
                "is_pure_date_query": len(tokenize_document(clean).get("tokens", []))
                == 0,
            }

    # 2) extract all explicit masks, independent of separator details
    masks = [m.upper() for m in DATE_MASK_PATTERN.findall(q)]
    if len(masks) >= 2:
        start_mask, end_mask = masks[0], masks[1]
        clean = DATE_MASK_PATTERN.sub("", q)
        clean = re.sub(r"\bto\b", " ", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\s+", " ", clean).strip()
        return {
            "start_mask": start_mask,
            "end_mask": end_mask,
            "clean_query_text": clean.lower(),
            "matched_semantic_key": None,
            "is_pure_date_query": len(tokenize_document(clean).get("tokens", [])) == 0,
        }

    # 3) single explicit mask
    if len(masks) == 1:
        mask = masks[0]
        clean = DATE_MASK_PATTERN.sub("", q)
        clean = re.sub(r"\s+", " ", clean).strip()
        return {
            "start_mask": mask,
            "end_mask": mask,
            "clean_query_text": clean.lower(),
            "matched_semantic_key": None,
            "is_pure_date_query": len(tokenize_document(clean).get("tokens", [])) == 0,
        }

    # 4) no date found
    return {
        "start_mask": None,
        "end_mask": None,
        "clean_query_text": q.lower(),
        "matched_semantic_key": None,
        "is_pure_date_query": False,
    }


def chronological_search_collection(
    collection: chromadb.Collection,
    query_texts: list[str],
    date_field: str = "master_date",
    n_results: int = 50,
):
    query_results_dict = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }

    ts_field = date_field.replace("_date", "_ts")

    for query_text in query_texts:
        date_info = extract_date_filter_from_query(query_text)
        start_mask = date_info.get("start_mask")
        end_mask = date_info.get("end_mask")

        if not start_mask or not end_mask:
            continue

        where_clause = build_timestamp_where_clause(
            date_field=date_field,
            start_mask=start_mask,
            end_mask=end_mask,
        )

        query_result = collection.get(
            where=where_clause,
            include=["documents", "metadatas"],
        )

        ids = query_result.get("ids", [])
        documents = query_result.get("documents", [])
        metadatas = query_result.get("metadatas", [])

        specificity_score = count_mask_specificity(start_mask, end_mask)

        scored = []
        for id_, doc_, meta_ in zip(ids, documents, metadatas):
            meta_ = meta_ or {}
            reliability_bonus = 1 if meta_.get("date_reliability") == "high" else 0
            ts_value = meta_.get(ts_field)
            recency_tiebreak = (
                ts_value if isinstance(ts_value, (int, float)) else float("-inf")
            )
            score = specificity_score + reliability_bonus
            scored.append((id_, doc_, score, recency_tiebreak))

        scored.sort(key=lambda x: (x[2], x[3]), reverse=True)
        scored = scored[:n_results]

        for rank, (id_, doc_, score_, _) in enumerate(scored, start=1):
            query_results_dict["ids"].append(id_)
            query_results_dict["documents"].append(doc_)
            query_results_dict["distances"].append(-float(score_))
            query_results_dict["rank"].append(rank)
            query_results_dict["query_text"].append(query_text)
            query_results_dict["collection"].append(f"{collection.name}_chrono")

    return query_results_dict


def lexical_search_collection(
    collection: chromadb.Collection, query_dict: dict, n_results: int = 50
):
    query_results_dict = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }

    for query_text in query_dict.keys():
        date_info = extract_date_filter_from_query(query_text)

        clean_query_text = date_info["clean_query_text"]
        start_mask = date_info["start_mask"]
        end_mask = date_info["end_mask"]

        tokens_dict = tokenize_document(clean_query_text)
        query_tokens = set(tokens_dict.get("tokens", []))
        query_token_string = tokens_dict.get("token_string", "")

        token_where = None
        if query_tokens:
            if len(query_tokens) == 1:
                token_where = {"tokens": {"$contains": list(query_tokens)[0]}}
            else:
                token_where = {
                    "$or": [{"tokens": {"$contains": token}} for token in query_tokens]
                }

        date_where = None
        if start_mask and end_mask:
            date_where = build_timestamp_where_clause(
                date_field="master_date",
                start_mask=start_mask,
                end_mask=end_mask,
            )

        where_clause = combine_where_clauses(token_where, date_where)
        if not where_clause:
            continue

        query_result = collection.get(
            where=where_clause,
            include=["documents", "metadatas"],
        )

        ids = query_result.get("ids", [])
        documents = query_result.get("documents", [])
        metadatas = query_result.get("metadatas", [])

        scored = []
        for id_, doc_, meta_ in zip(ids, documents, metadatas):
            meta_ = meta_ or {}

            if not query_tokens:
                score = 1
            else:
                doc_tokens = set(meta_.get("tokens", []))
                doc_token_string = meta_.get("token_string", "")
                token_overlap = len(query_tokens & doc_tokens)
                substring_bonus = (
                    2
                    if query_token_string and query_token_string in doc_token_string
                    else 0
                )
                score = token_overlap + substring_bonus

            if score > 0:
                scored.append((id_, doc_, score))

        scored.sort(key=lambda x: x[2], reverse=True)
        scored = scored[:n_results]

        for rank, (id_, doc_, score_) in enumerate(scored, start=1):
            query_results_dict["ids"].append(id_)
            query_results_dict["documents"].append(doc_)
            query_results_dict["distances"].append(-float(score_))
            query_results_dict["rank"].append(rank)
            query_results_dict["query_text"].append(query_text)
            query_results_dict["collection"].append(f"{collection.name}_lexical")

    return query_results_dict


def semantic_search_collection(
    collection: chromadb.Collection, query_dict: dict, n_results: int = 50
):
    for query_text in query_dict:
        if query_dict[query_text] is None:
            raise ValueError(f"Embedding for query text '{query_text}' is missing.")

    final_query_texts = list(query_dict.keys())
    query_results = collection.query(
        query_embeddings=list(query_dict.values()),
        n_results=n_results,
        include=["documents", "distances"],
    )

    query_results_dict = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }
    for i, query_text in enumerate(final_query_texts):
        if (
            i >= len(query_results.get("ids", []))
            or not query_results.get("ids", [])[i]
        ):
            continue

        query_results_dict["ids"].extend(query_results.get("ids", [])[i])
        query_results_dict["documents"].extend(query_results.get("documents", [])[i])
        query_results_dict["distances"].extend(query_results.get("distances", [])[i])
        res_len = len(query_results.get("ids", [])[i])
        query_results_dict["rank"].extend(list(range(1, 1 + res_len)))
        query_results_dict["query_text"].extend([query_text] * res_len)
        query_results_dict["collection"].extend([collection.name] * res_len)
    return query_results_dict


def get_final_results(
    query_text: str | list,
    query_results_df: pd.DataFrame,
    rrf_smoothing: int = 60,
    n_results: int = 5,
):
    if query_results_df.empty:
        return pd.DataFrame(columns=["ids", "score", "rank"])

    relevant_df = query_results_df.copy()
    relevant_df["query_text"] = (
        relevant_df["query_text"].astype(str).str.strip().str.lower()
    )

    if isinstance(query_text, str):
        mask = relevant_df["query_text"] == normalize_query_text(query_text)
    elif isinstance(query_text, list):
        mask = relevant_df["query_text"].isin(
            [normalize_query_text(q) for q in query_text]
        )
    else:
        return pd.DataFrame(columns=["ids", "score", "rank"])

    relevant_df = relevant_df[mask].copy()

    if relevant_df.empty:
        return pd.DataFrame(columns=["ids", "score", "rank"])

    relevant_df["ids"] = relevant_df["ids"].astype(str).str[:105]
    weights = relevant_df["collection"].map(field_weight_dict).fillna(0.2)
    relevant_df["rrf_score"] = weights / (relevant_df["rank"] + rrf_smoothing)

    rrf_vals = relevant_df.groupby("ids")["rrf_score"].sum().nlargest(n_results)
    return pd.DataFrame(
        {
            "ids": rrf_vals.index,
            "score": rrf_vals.values,
            "rank": list(range(len(rrf_vals))),
        }
    )


def clean_query_texts(query_texts: list):
    cleaned_query_texts = set()
    for query_text in query_texts:
        if isinstance(query_text, str):
            cleaned_query_texts.add(normalize_query_text(query_text))
        elif isinstance(query_text, list):
            cleaned_query_texts.update(clean_query_texts(query_text))
    return list(cleaned_query_texts)


def populate_query_embedding_cache(
    query_texts: list,
    embedding_functions: list[chromadb.utils.embedding_functions.EmbeddingFunction],
):
    query_embedding_cache = {}
    for embedding_function in embedding_functions:
        ef_name = id(embedding_function)
        qes = embedding_function(query_texts)
        for i, qt in enumerate(query_texts):
            if not query_embedding_cache.get(qt, {}):
                query_embedding_cache[qt] = {ef_name: qes[i].tolist()}
            else:
                query_embedding_cache[qt][ef_name] = qes[i].tolist()
    return query_embedding_cache


def query_all_collections(
    chroma_client: chromadb.PersistentClient, query_texts: list, n_results: int = 5
):
    import time

    start = time.time()

    cleaned_query_texts = clean_query_texts(query_texts)
    unique_embedding_functions = list(
        {id(ef): ef for ef in collection_ef_map.values()}.values()
    )
    query_embedding_cache = populate_query_embedding_cache(
        cleaned_query_texts, unique_embedding_functions
    )
    stop = time.time()
    print(f"Time taken to run embedding functions: {stop - start:.2f} seconds")

    query_info_map = {
        qt: extract_date_filter_from_query(qt) for qt in cleaned_query_texts
    }

    combined_query_results = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }

    collection_names = collection_dict.keys()
    for col_name in collection_names:
        col_type = collection_type_rev_map.get(col_name) or "sentence"

        collection_ef = collection_ef_map.get(col_type)
        collection = chroma_client.get_collection(
            col_name, embedding_function=collection_ef
        )

        if collection.count() == 0:
            continue

        all_query_dict = {
            qt: query_embedding_cache.get(qt, {}).get(id(collection_ef)) or []
            for qt in query_embedding_cache
        }

        lexical_query_dict = {
            qt: emb
            for qt, emb in all_query_dict.items()
            if not query_info_map.get(qt, {}).get("is_pure_date_query", False)
        }
        semantic_query_dict = lexical_query_dict

        if lexical_query_dict:
            lexical_query_results_dict = lexical_search_collection(
                collection=collection,
                query_dict=lexical_query_dict,
                n_results=min(n_results * 50, 500),
            )
            if lexical_query_results_dict:
                for key in combined_query_results:
                    combined_query_results[key].extend(
                        lexical_query_results_dict.get(key, [])
                    )

        if col_name == "context_narrative":
            chrono_query_results_dict = chronological_search_collection(
                collection=collection,
                query_texts=list(all_query_dict.keys()),
                date_field="master_date",
                n_results=min(n_results * 50, 500),
            )
            if chrono_query_results_dict:
                for key in combined_query_results:
                    combined_query_results[key].extend(
                        chrono_query_results_dict.get(key, [])
                    )

        if semantic_query_dict:
            semantic_query_results_dict = semantic_search_collection(
                collection=collection,
                query_dict=semantic_query_dict,
                n_results=min(n_results * 10, 500),
            )
            if semantic_query_results_dict:
                for key in combined_query_results:
                    combined_query_results[key].extend(
                        semantic_query_results_dict.get(key, [])
                    )

    combined_query_results = pd.DataFrame(combined_query_results)

    final_results = {}
    for query_text in query_texts:
        result = get_final_results(
            normalize_query_text(query_text),
            combined_query_results,
            n_results=n_results,
        )
        final_results[str(query_text)] = {k: list(result[k]) for k in result}

    return final_results
