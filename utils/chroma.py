"""
chroma.py

ChromaDB storage, embedding, indexing, and backend query helpers for AfterSight.
"""

import json, os, multiprocessing as mp
from urllib import error as urllib_error
from urllib import request as urllib_request

import chromadb
from chromadb.utils.batch_utils import create_batches

from utils.date import (
    build_date_where_clause,
    count_mask_specificity,
    date_dict_to_ts,
)
from utils.retrieval import (
    SearchManifest,
    build_candidate_row,
    build_query_response,
    build_query_specs,
    make_trace_logs,
    normalize_query_text,
    normalize_search_options,
    summarize_source_debug,
    tokenize_document,
    trace_line,
    resolve_runtime_plan,
)


process_embedding_function_cache = {}

DEFAULT_EMBEDDING_PROCESS_COUNT = min(4, os.cpu_count() or 1)
DEFAULT_EMBEDDING_PARALLEL_MIN_DOCS = 24
DEFAULT_EMBEDDING_CHUNK_COUNT_PER_PROCESS = 2
DEFAULT_OLLAMA_KEEP_ALIVE = "2m"


def get_embedding_config_by_key(embedding_key: str) -> dict | None:
    for config in SearchManifest.EMBEDDINGS.values():
        if config["key"] == embedding_key:
            return config
    return None


def get_embedding_key_for_family(embedding_family: str | None) -> str | None:
    if not embedding_family:
        return None
    config = SearchManifest.EMBEDDINGS.get(embedding_family)
    if not config:
        return None
    return config["key"]


def get_embedding_function(embedding_key: str):
    embedding_config = get_embedding_config_by_key(embedding_key)
    if embedding_config is None:
        raise KeyError(f"Unknown embedding key: {embedding_key}")

    embedding_function = process_embedding_function_cache.get(embedding_key)
    if embedding_function is not None:
        return embedding_function

    embedding_function = OllamaKeepAliveEmbeddingFunction(
        model_name=embedding_config["model_name"]
    )
    process_embedding_function_cache[embedding_key] = embedding_function
    return embedding_function


def get_ollama_base_url() -> str:
    base_url = (os.getenv("OLLAMA_HOST") or "http://127.0.0.1:11434").strip()
    if "://" not in base_url:
        base_url = f"http://{base_url}"
    base_url = base_url.rstrip("/")
    if base_url.endswith("/api"):
        base_url = base_url[: -len("/api")]
    return base_url


def get_ollama_keep_alive() -> str:
    value = os.getenv("CHROMA_OLLAMA_KEEP_ALIVE")
    if value is None or not value.strip():
        return DEFAULT_OLLAMA_KEEP_ALIVE
    return value.strip()


def normalize_ollama_model_name(model_name: str) -> str:
    value = str(model_name or "").strip().lower()
    if not value:
        return ""
    if ":" not in value:
        return value
    base_name, tag = value.rsplit(":", 1)
    return base_name if tag == "latest" else value


def get_loaded_ollama_model_names() -> set[str]:
    request = urllib_request.Request(
        f"{get_ollama_base_url()}/api/ps",
        method="GET",
    )
    try:
        with urllib_request.urlopen(request) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"Ollama request to {get_ollama_base_url()}/api/ps failed: "
            f"HTTP {exc.code} {exc.reason}. {message}"
        ) from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Failed to reach Ollama at {get_ollama_base_url()}: {exc}") from exc

    models = payload.get("models")
    if not isinstance(models, list):
        return set()

    loaded = set()
    for model in models:
        if not isinstance(model, dict):
            continue
        for field in ("name", "model"):
            value = str(model.get(field) or "").strip()
            if value:
                loaded.add(normalize_ollama_model_name(value))
    return loaded


def active_search_embedding_model_names(search_options: dict | None = None) -> list[str]:
    normalized_options = normalize_search_options(search_options)
    enabled_search_types = set(normalized_options["enabled_search_types"])
    if "semantic" not in enabled_search_types:
        return []

    enabled_sources = normalized_options["enabled_sources"] or [
        source_id
        for source_id, source_config in SearchManifest.SOURCES.items()
        if source_config.get("enabled_by_default", True)
    ]
    disabled_sources = set(normalized_options["disabled_sources"])
    model_names = []
    for source_id in enabled_sources:
        if source_id in disabled_sources:
            continue
        source_config = SearchManifest.SOURCES.get(source_id) or {}
        if "semantic" not in source_config.get("search_types", ()):
            continue
        embedding_family = source_config.get("embedding_family")
        embedding_key = get_embedding_key_for_family(embedding_family)
        embedding_config = (
            get_embedding_config_by_key(embedding_key) if embedding_key else None
        )
        model_name = normalize_ollama_model_name(
            str((embedding_config or {}).get("model_name") or "").strip()
        )
        if model_name:
            model_names.append(model_name)
    return sorted(dict.fromkeys(model_names))


class OllamaKeepAliveEmbeddingFunction:
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.base_url = get_ollama_base_url()

    def __call__(self, documents: list[str]) -> list[list[float]]:
        if not documents:
            return []

        embeddings = self._post_json(
            "/api/embed",
            {
                "model": self.model_name,
                "input": documents,
                "keep_alive": get_ollama_keep_alive(),
            },
        ).get("embeddings")
        if not isinstance(embeddings, list):
            raise RuntimeError("Ollama embeddings response did not include embeddings.")
        return embeddings

    def _post_json(self, path: str, payload: dict) -> dict:
        request = urllib_request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib_request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace").strip()
            if exc.code == 404:
                if "model" in message.lower() and "not found" in message.lower():
                    raise RuntimeError(
                        f"Ollama model '{self.model_name}' was not found. "
                        f"Pull it first with `ollama pull {self.model_name}`."
                    ) from exc
            raise RuntimeError(
                f"Ollama request to {self.base_url}{path} failed: "
                f"HTTP {exc.code} {exc.reason}. {message}"
            ) from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(
                f"Failed to reach Ollama at {self.base_url}: {exc}"
            ) from exc


def resolve_embedding_process_count(process_count: int | None = None) -> int:
    if process_count is not None:
        return max(1, int(process_count))

    raw_value = os.getenv("CHROMA_EMBEDDING_PROCESSES")
    if raw_value is None or not raw_value.strip():
        return DEFAULT_EMBEDDING_PROCESS_COUNT

    try:
        return max(1, int(raw_value))
    except ValueError:
        return DEFAULT_EMBEDDING_PROCESS_COUNT


def resolve_embedding_parallel_min_docs() -> int:
    raw_value = os.getenv("CHROMA_EMBEDDING_MIN_DOCS")
    if raw_value is None or not raw_value.strip():
        return DEFAULT_EMBEDDING_PARALLEL_MIN_DOCS

    try:
        return max(1, int(raw_value))
    except ValueError:
        return DEFAULT_EMBEDDING_PARALLEL_MIN_DOCS


def resolve_embedding_batch_size(document_count: int, process_count: int) -> int:
    raw_value = os.getenv("CHROMA_EMBEDDING_BATCH_SIZE")
    if raw_value is not None and raw_value.strip():
        try:
            return max(1, int(raw_value))
        except ValueError:
            pass

    divisor = max(1, process_count * DEFAULT_EMBEDDING_CHUNK_COUNT_PER_PROCESS)
    return max(1, (document_count + divisor - 1) // divisor)


def normalize_embedding_batch(embeddings):
    if hasattr(embeddings, "tolist"):
        embeddings = embeddings.tolist()
    return [
        embedding.tolist() if hasattr(embedding, "tolist") else list(embedding)
        for embedding in embeddings
    ]


def embed_documents_with_function(
    documents: list[str], embedding_key: str
) -> list[list[float]]:
    embedding_function = get_embedding_function(embedding_key)
    return normalize_embedding_batch(embedding_function(documents))


def generate_embeddings_for_key(
    documents: list[str],
    embedding_key: str | None,
    process_count: int | None = None,
) -> list[list[float]]:
    if not documents or not embedding_key:
        return []

    process_count = resolve_embedding_process_count(process_count)
    if process_count <= 1 or len(documents) < resolve_embedding_parallel_min_docs():
        return embed_documents_with_function(documents, embedding_key)

    batch_size = resolve_embedding_batch_size(
        document_count=len(documents),
        process_count=process_count,
    )
    document_batches = [
        documents[i : i + batch_size] for i in range(0, len(documents), batch_size)
    ]
    if len(document_batches) <= 1:
        return embed_documents_with_function(documents, embedding_key)

    pool_size = min(process_count, len(document_batches))
    if pool_size <= 1:
        return embed_documents_with_function(documents, embedding_key)

    with mp.get_context("spawn").Pool(pool_size) as pool:
        try:
            embedded_batches = pool.starmap(
                embed_documents_with_function,
                [(batch, embedding_key) for batch in document_batches],
            )
        except Exception:
            return embed_documents_with_function(documents, embedding_key)

    embeddings = []
    for embedded_batch in embedded_batches:
        embeddings.extend(normalize_embedding_batch(embedded_batch))
    return embeddings


def get_chroma_client(
    path: str | None = None,
    host: str | None = None,
    port: int | str | None = None,
    ssl: bool | None = None,
):
    host = host or os.getenv("CHROMA_HOST") or os.getenv("CHROMA_SERVER_HOST")
    if host:
        port = int(port or os.getenv("CHROMA_PORT") or 8000)
        if ssl is None:
            ssl = os.getenv("CHROMA_SSL", "").strip().lower() in {"1", "true", "yes"}
        return chromadb.HttpClient(host=host, port=port, ssl=ssl)

    path = path or os.getenv("CHROMA_URL")
    if not path:
        raise RuntimeError("Set CHROMA_URL or CHROMA_HOST/CHROMA_SERVER_HOST.")

    return chromadb.PersistentClient(path=path)


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
            **clean_date_object,
            **ts_object,
        },
    }


def extract_description_fragments(description_object: dict):
    content_object = description_object.get("content") or {}
    context_object = description_object.get("context") or {}
    return {
        "summary": content_object.get("summary"),
        "detailed_description": content_object.get("detailed_description"),
        "ocr_text": content_object.get("text"),
        "miscellaneous": content_object.get("miscellaneous"),
        "event": context_object.get("event"),
        "analysis": context_object.get("analysis"),
        "metadata_relevance": context_object.get("metadata_relevance"),
        "other_details": context_object.get("other_details"),
        "objects": content_object.get("objects"),
        "vibe": content_object.get("vibe"),
        "background": content_object.get("background"),
        "primary_category": context_object.get("primary_category"),
        "intent": context_object.get("intent"),
        "composition": context_object.get("composition"),
        "estimated_date": context_object.get("estimated_date"),
    }


def normalize_field_fragment(value):
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items() if item}
    if value:
        return str(value)
    return None


def combine_field_fragments(field_fragments: dict, text_mode: str) -> str:
    parts = []
    for value in field_fragments.values():
        normalized_value = normalize_field_fragment(value)
        if not normalized_value:
            continue
        if isinstance(normalized_value, list):
            parts.append(", ".join(normalized_value))
        elif isinstance(normalized_value, dict):
            parts.append(", ".join(str(item) for item in normalized_value.values()))
        elif text_mode == "word":
            parts.append(str(normalized_value))
        else:
            parts.append(str(normalized_value))
    separator = ", " if text_mode == "word" else "\n"
    return separator.join(part for part in parts if part).strip(" ,\n")


def build_entry_source_records(entry_object: dict) -> dict[str, dict]:
    metadata_object = entry_object.get("metadata") or {}
    if not metadata_object:
        return {}

    absolute_fields = extract_metadata_fields(metadata_object).get("absolute", {})
    description_object = entry_object.get("description") or {}
    description_fragments = (
        extract_description_fragments(description_object) if description_object else {}
    )
    source_records = {}

    for source_id, source_config in SearchManifest.SOURCES.items():
        field_fragments = {}
        for field_name in source_config["fields"]:
            field_value = description_fragments.get(field_name)
            if field_value:
                field_fragments[field_name] = field_value

        document = combine_field_fragments(field_fragments, source_config["text_mode"])
        if not document:
            continue

        metadata = {
            "source_id": source_id,
            "source_fields": list(field_fragments.keys()),
            "field_fragments_json": json.dumps(
                {
                    key: normalize_field_fragment(value)
                    for key, value in field_fragments.items()
                    if normalize_field_fragment(value)
                },
                separators=(",", ":"),
            ),
        }
        if source_config.get("date_field"):
            metadata.update(absolute_fields)

        source_records[source_id] = {
            "document": document,
            "metadata": metadata,
        }
    return source_records


def build_source_entry_map(entries: dict, verbose: bool = False) -> dict[str, dict]:
    source_entry_map = {source_id: {} for source_id in SearchManifest.SOURCES}
    if verbose:
        print("\n" * 4, "==" * 40, "\n" * 2)

    for entry_id, entry_object in entries.items():
        source_records = build_entry_source_records(entry_object)
        if verbose and source_records:
            print(json.dumps(source_records, indent=2))
        for source_id, record in source_records.items():
            source_entry_map[source_id][entry_id] = record

    if verbose:
        print(json.dumps(source_entry_map, indent=2))
    return source_entry_map


def prep_source_records_for_upsert(
    source_records: dict,
) -> tuple[list[str], list[str], list[dict]]:
    ids = []
    documents = []
    metadatas = []
    for entry_id, record in source_records.items():
        document = str(record.get("document") or "").strip()
        if not document:
            continue
        ids.append(str(entry_id))
        documents.append(document)
        metadatas.append(dict(record.get("metadata") or {}))
    return ids, documents, metadatas


def upsert_batch_to_collection(collection, batches, embedding_key: str | None = None):
    prepared_batches = []
    documents_to_embed = []
    for batch_ids, _, batch_metadatas, batch_documents in batches:
        if batch_metadatas is None:
            batch_metadatas = [None] * len(batch_ids)
        else:
            batch_metadatas = list(batch_metadatas)
        for index in range(len(batch_ids)):
            tokens_dict = tokenize_document(batch_documents[index])
            if batch_metadatas and isinstance(batch_metadatas[index], dict):
                batch_metadatas[index].update(tokens_dict)
            else:
                batch_metadatas[index] = tokens_dict
        prepared_batches.append((batch_ids, batch_metadatas, batch_documents))
        if embedding_key:
            documents_to_embed.extend(batch_documents)

    embeddings = (
        generate_embeddings_for_key(documents_to_embed, embedding_key)
        if embedding_key
        else []
    )
    embedding_offset = 0
    for batch_ids, batch_metadatas, batch_documents in prepared_batches:
        upsert_kwargs = {
            "ids": batch_ids,
            "metadatas": batch_metadatas,
            "documents": batch_documents,
        }
        if embedding_key:
            next_offset = embedding_offset + len(batch_documents)
            upsert_kwargs["embeddings"] = embeddings[embedding_offset:next_offset]
            embedding_offset = next_offset
        collection.upsert(**upsert_kwargs)


def populate_db(
    entries: dict,
    chroma_client: chromadb.PersistentClient,
    overwrite: bool = False,
    verbose: bool = False,
):
    source_entry_map = build_source_entry_map(entries, verbose=verbose)

    for source_id, source_config in SearchManifest.SOURCES.items():
        source_records = source_entry_map.get(source_id) or {}
        ids, documents, metadatas = prep_source_records_for_upsert(source_records)
        if not ids or not documents:
            continue

        collection = chroma_client.create_collection(
            name=source_config["collection_name"],
            configuration={"hnsw": {"space": "cosine"}},
            get_or_create=True,
        )
        if not overwrite:
            existing_ids = set(collection.get(ids=ids, include=[]).get("ids", []))
            if existing_ids:
                filtered_ids = []
                filtered_documents = []
                filtered_metadatas = []
                for entry_id, document, metadata in zip(ids, documents, metadatas):
                    if entry_id in existing_ids:
                        continue
                    filtered_ids.append(entry_id)
                    filtered_documents.append(document)
                    filtered_metadatas.append(metadata)
                ids, documents, metadatas = (
                    filtered_ids,
                    filtered_documents,
                    filtered_metadatas,
                )
            if not ids or not documents:
                continue

        batches = create_batches(
            chroma_client,
            ids=ids,
            metadatas=metadatas,
            documents=documents,
        )
        upsert_batch_to_collection(
            collection,
            batches,
            embedding_key=get_embedding_key_for_family(
                source_config.get("embedding_family")
            ),
        )


def delete_entry_ids(chroma_client, entry_ids: list[str]):
    if not chroma_client or not entry_ids:
        return

    try:
        collections = chroma_client.list_collections()
    except Exception:
        return

    prefixes = tuple(f"{entry_id}_" for entry_id in entry_ids)
    direct_ids = set(entry_ids)

    for collection_item in collections:
        collection_name = (
            collection_item.name
            if hasattr(collection_item, "name")
            else collection_item
        )
        if not collection_name:
            continue

        try:
            collection = chroma_client.get_collection(collection_name)
            existing_ids = collection.get(include=[]).get("ids", [])
        except Exception:
            continue

        ids_to_delete = [
            item_id
            for item_id in existing_ids
            if item_id in direct_ids or item_id.startswith(prefixes)
        ]
        if ids_to_delete:
            collection.delete(ids=ids_to_delete)


def lexical_search_collection(
    collection: chromadb.Collection,
    query_specs: dict[str, dict],
    source_plan: dict,
    search_plan: dict,
    trace_logs: list | None = None,
    trace: bool = False,
):
    rows = []
    debug_rows = []

    for query_text, query_spec in query_specs.items():
        query_tokens = query_spec.get("tokens", [])
        if not query_tokens:
            continue

        query_token_string = query_spec.get("token_string", "")
        token_where = (
            {"tokens": {"$contains": query_tokens[0]}}
            if len(query_tokens) == 1
            else {"$or": [{"tokens": {"$contains": token}} for token in query_tokens]}
        )

        query_result = collection.get(
            where=token_where, include=["documents", "metadatas"]
        )
        ids = query_result.get("ids", [])
        documents = query_result.get("documents", [])
        metadatas = query_result.get("metadatas", [])

        scored = []
        min_coverage = float(search_plan["thresholds"].get("min_coverage", 0.0) or 0.0)
        phrase_bonus = float(search_plan["thresholds"].get("phrase_bonus", 0.0) or 0.0)

        for id_, document, metadata in zip(ids, documents, metadatas):
            metadata = dict(metadata or {})
            doc_tokens = set(metadata.get("tokens", []))
            if not doc_tokens:
                doc_tokens = set(tokenize_document(document).get("tokens", []))
            doc_token_string = metadata.get("token_string", "") or tokenize_document(
                document
            ).get("token_string", "")
            token_overlap = len(set(query_tokens) & doc_tokens)
            coverage = token_overlap / max(1, len(query_tokens))
            phrase_hit = bool(
                query_token_string and query_token_string in doc_token_string
            )
            normalized_score = min(
                1.0, coverage + (phrase_bonus if phrase_hit else 0.0)
            )

            if len(query_tokens) == 1:
                threshold_passed = token_overlap > 0
                threshold_reason = "single-token overlap required"
            else:
                threshold_passed = coverage >= min_coverage or phrase_hit
                threshold_reason = (
                    f"coverage>={min_coverage:.2f}"
                    if threshold_passed
                    else f"coverage {coverage:.2f} below {min_coverage:.2f}"
                )

            candidate = build_candidate_row(
                query_text=query_text,
                query_spec=query_spec,
                source_plan=source_plan,
                search_type="lexical",
                id_=id_,
                document=document,
                metadata=metadata,
                raw_score=coverage + (phrase_bonus if phrase_hit else 0.0),
                raw_distance=None,
                normalized_score=normalized_score,
                threshold_passed=threshold_passed,
                threshold_reason=threshold_reason,
            )
            candidate["coverage"] = coverage
            candidate["phrase_hit"] = phrase_hit
            scored.append(candidate)

        scored.sort(
            key=lambda row: (
                row["threshold_passed"],
                row.get("phrase_hit", False),
                row.get("raw_score") or 0.0,
                row["id"],
            ),
            reverse=True,
        )
        kept = [row for row in scored if row["threshold_passed"]][
            : search_plan["fetch_limit"]
        ]
        for rank, row in enumerate(kept, start=1):
            row["rank"] = rank
        rows.extend(kept)

        debug_entry = summarize_source_debug(
            source_plan=source_plan,
            search_type="lexical",
            query_text=query_text,
            thresholds=search_plan["thresholds"],
            before_count=len(scored),
            after_count=len(kept),
            kept_rows=kept,
            search_plan=search_plan,
        )
        debug_rows.append(debug_entry)
        trace_line(
            trace_logs,
            trace,
            f"{query_text} lexical {source_plan['source_id']}: kept {len(kept)} of {len(scored)}",
        )

    return rows, debug_rows


def chronological_search_collection(
    collection: chromadb.Collection,
    query_specs: dict[str, dict],
    source_plan: dict,
    search_plan: dict,
    trace_logs: list | None = None,
    trace: bool = False,
):
    rows = []
    debug_rows = []
    date_field = SearchManifest.SOURCES[source_plan["source_id"]].get(
        "date_field", "master_date"
    )
    ts_field = date_field.replace("_date", "_ts")

    for query_text, query_spec in query_specs.items():
        date_filters = query_spec.get("date_filters", [])
        if not date_filters:
            continue

        scored = []
        min_specificity = int(search_plan["thresholds"].get("min_specificity", 1) or 1)
        for filter_index, date_filter in enumerate(date_filters):
            start_mask = date_filter.get("start_mask")
            end_mask = date_filter.get("end_mask")
            if not start_mask or not end_mask:
                continue

            where_clause = build_date_where_clause(date_field, date_filter)
            if not where_clause:
                continue

            query_result = collection.get(
                where=where_clause, include=["documents", "metadatas"]
            )
            ids = query_result.get("ids", [])
            documents = query_result.get("documents", [])
            metadatas = query_result.get("metadatas", [])
            specificity_score = count_mask_specificity(start_mask, end_mask)

            for id_, document, metadata in zip(ids, documents, metadatas):
                metadata = dict(metadata or {})
                reliability_bonus = (
                    1 if metadata.get("date_reliability") == "high" else 0
                )
                ts_value = metadata.get(ts_field)
                recency_tiebreak = (
                    ts_value if isinstance(ts_value, (int, float)) else float("-inf")
                )
                raw_score = float(specificity_score + reliability_bonus)
                threshold_passed = raw_score >= min_specificity
                threshold_reason = (
                    f"specificity>={min_specificity}"
                    if threshold_passed
                    else f"specificity {raw_score:.2f} below {min_specificity}"
                )
                candidate = build_candidate_row(
                    query_text=query_text,
                    query_spec=query_spec,
                    source_plan=source_plan,
                    search_type="chrono",
                    id_=id_,
                    document=document,
                    metadata=metadata,
                    raw_score=raw_score,
                    raw_distance=None,
                    normalized_score=max(raw_score, 0.0),
                    threshold_passed=threshold_passed,
                    threshold_reason=threshold_reason,
                    variant=str(filter_index),
                )
                candidate["recency_tiebreak"] = recency_tiebreak
                scored.append(candidate)

        scored.sort(
            key=lambda row: (
                row["threshold_passed"],
                row.get("raw_score") or 0.0,
                row.get("recency_tiebreak", float("-inf")),
                row["id"],
            ),
            reverse=True,
        )
        kept = [row for row in scored if row["threshold_passed"]][
            : search_plan["fetch_limit"]
        ]
        max_score = max((row.get("raw_score") or 0.0) for row in kept) if kept else 0.0
        for rank, row in enumerate(kept, start=1):
            row["rank"] = rank
            row["normalized_score"] = (
                (row.get("raw_score") or 0.0) / max_score if max_score > 0 else 0.0
            )
        rows.extend(kept)

        debug_entry = summarize_source_debug(
            source_plan=source_plan,
            search_type="chrono",
            query_text=query_text,
            thresholds=search_plan["thresholds"],
            before_count=len(scored),
            after_count=len(kept),
            kept_rows=kept,
            search_plan=search_plan,
        )
        debug_rows.append(debug_entry)
        trace_line(
            trace_logs,
            trace,
            f"{query_text} chrono {source_plan['source_id']}: kept {len(kept)} of {len(scored)}",
        )

    return rows, debug_rows


def semantic_search_collection(
    collection: chromadb.Collection,
    query_specs: dict[str, dict],
    source_plan: dict,
    search_plan: dict,
    trace_logs: list | None = None,
    trace: bool = False,
):
    rows = []
    debug_rows = []
    embedding_key = get_embedding_key_for_family(
        SearchManifest.SOURCES[source_plan["source_id"]].get("embedding_family")
    )
    if not embedding_key:
        return rows, debug_rows

    final_query_texts = []
    query_embeddings = []
    for query_text, query_spec in query_specs.items():
        if not query_spec.get("tokens"):
            continue
        query_embedding = (query_spec.get("embeddings") or {}).get(embedding_key)
        if query_embedding is None:
            raise ValueError(f"Embedding for query text '{query_text}' is missing.")
        final_query_texts.append(query_text)
        query_embeddings.append(query_embedding)

    if not query_embeddings:
        return rows, debug_rows

    query_results = collection.query(
        query_embeddings=query_embeddings,
        n_results=search_plan["fetch_limit"],
        include=["documents", "distances", "metadatas"],
    )

    absolute_limit = float(search_plan["thresholds"].get("max_distance", 1.0) or 1.0)
    tail_delta = float(search_plan["thresholds"].get("tail_delta", 0.0) or 0.0)

    for index, query_text in enumerate(final_query_texts):
        ids = (
            (query_results.get("ids") or [[]])[index]
            if index < len(query_results.get("ids", []))
            else []
        )
        if not ids:
            debug_rows.append(
                summarize_source_debug(
                    source_plan=source_plan,
                    search_type="semantic",
                    query_text=query_text,
                    thresholds=search_plan["thresholds"],
                    before_count=0,
                    after_count=0,
                    kept_rows=[],
                    search_plan=search_plan,
                )
            )
            continue

        documents = (query_results.get("documents") or [[]])[index]
        distances = (query_results.get("distances") or [[]])[index]
        metadatas = (query_results.get("metadatas") or [[]])[index]
        best_distance = min(distances) if distances else float("inf")
        dynamic_limit = best_distance + tail_delta

        scored = []
        for id_, document, distance, metadata in zip(
            ids, documents, distances, metadatas
        ):
            metadata = dict(metadata or {})
            threshold_passed = distance <= absolute_limit and distance <= dynamic_limit
            threshold_reason = (
                f"distance<={absolute_limit:.3f} and <={dynamic_limit:.3f}"
                if threshold_passed
                else f"distance {distance:.3f} above {min(absolute_limit, dynamic_limit):.3f}"
            )
            admission_limit = max(min(absolute_limit, dynamic_limit), 1e-6)
            normalized_score = max(0.0, 1.0 - (float(distance) / admission_limit))
            candidate = build_candidate_row(
                query_text=query_text,
                query_spec=query_specs[query_text],
                source_plan=source_plan,
                search_type="semantic",
                id_=id_,
                document=document,
                metadata=metadata,
                raw_score=None,
                raw_distance=float(distance),
                normalized_score=normalized_score,
                threshold_passed=threshold_passed,
                threshold_reason=threshold_reason,
            )
            scored.append(candidate)

        scored.sort(
            key=lambda row: (
                row["threshold_passed"],
                -(row.get("raw_distance") or float("inf")),
                row["id"],
            ),
            reverse=True,
        )
        kept = [row for row in scored if row["threshold_passed"]]
        kept.sort(
            key=lambda row: ((row.get("raw_distance") or float("inf")), row["id"])
        )
        kept = kept[: search_plan["fetch_limit"]]
        for rank, row in enumerate(kept, start=1):
            row["rank"] = rank
        rows.extend(kept)

        debug_entry = summarize_source_debug(
            source_plan=source_plan,
            search_type="semantic",
            query_text=query_text,
            thresholds={
                **search_plan["thresholds"],
                "best_distance": best_distance,
                "dynamic_limit": dynamic_limit,
            },
            before_count=len(scored),
            after_count=len(kept),
            kept_rows=kept,
            search_plan=search_plan,
        )
        debug_rows.append(debug_entry)
        trace_line(
            trace_logs,
            trace,
            f"{query_text} semantic {source_plan['source_id']}: "
            f"kept {len(kept)} of {len(scored)} "
            f"(best={best_distance:.3f}, limit={min(absolute_limit, dynamic_limit):.3f})",
        )

    return rows, debug_rows


def query_collections(
    chroma_client: chromadb.PersistentClient,
    query_texts: list,
    n_results: int = 5,
    search_options: dict | None = None,
    include_debug: bool = False,
    trace: bool = False,
):
    query_specs = build_query_specs(query_texts)
    trace_logs = make_trace_logs(include_debug=include_debug, trace=trace)
    runtime_plan = resolve_runtime_plan(
        query_specs,
        n_results=n_results,
        search_options=search_options,
        include_debug=include_debug,
        trace=trace,
        trace_logs=trace_logs,
    )

    semantic_query_specs = {
        query_text: query_spec
        for query_text, query_spec in query_specs.items()
        if query_spec.get("tokens")
    }

    required_embedding_families = sorted(
        {
            SearchManifest.SOURCES[source_id].get("embedding_family")
            for source_id, source_plan in runtime_plan["sources"].items()
            for search_type, search_plan in source_plan["search_types"].items()
            if search_type == "semantic"
            and search_plan["enabled"]
            and search_plan["weight"] > 0
        }
        - {None}
    )
    if semantic_query_specs and required_embedding_families:
        semantic_query_texts = list(semantic_query_specs.keys())
        for embedding_family in required_embedding_families:
            embedding_key = get_embedding_key_for_family(embedding_family)
            embeddings = generate_embeddings_for_key(
                semantic_query_texts, embedding_key
            )
            for index, query_text in enumerate(semantic_query_texts):
                semantic_query_specs[query_text]["embeddings"][embedding_key] = (
                    embeddings[index]
                )

    existing_collection_refs = {}
    for collection_ref in chroma_client.list_collections():
        collection_name = (
            collection_ref.name if hasattr(collection_ref, "name") else collection_ref
        )
        if collection_name:
            existing_collection_refs[collection_name] = collection_ref

    combined_rows = []
    debug_rows = []
    for source_id, source_plan in runtime_plan["sources"].items():
        if not source_plan["enabled"]:
            continue

        collection_ref = existing_collection_refs.get(source_plan["collection_name"])
        if collection_ref is None:
            trace_line(
                trace_logs,
                trace,
                f"source {source_id}: skipped missing collection {source_plan['collection_name']}",
            )
            continue

        collection = (
            collection_ref
            if hasattr(collection_ref, "query")
            else chroma_client.get_collection(source_plan["collection_name"])
        )

        try:
            lexical_plan = source_plan["search_types"].get("lexical")
            if lexical_plan and lexical_plan["enabled"] and lexical_plan["weight"] > 0:
                lexical_rows, lexical_debug_rows = lexical_search_collection(
                    collection=collection,
                    query_specs=semantic_query_specs,
                    source_plan=source_plan,
                    search_plan=lexical_plan,
                    trace_logs=trace_logs,
                    trace=trace,
                )
                combined_rows.extend(lexical_rows)
                debug_rows.extend(lexical_debug_rows)

            chrono_plan = source_plan["search_types"].get("chrono")
            if chrono_plan and chrono_plan["enabled"] and chrono_plan["weight"] > 0:
                chrono_rows, chrono_debug_rows = chronological_search_collection(
                    collection=collection,
                    query_specs=query_specs,
                    source_plan=source_plan,
                    search_plan=chrono_plan,
                    trace_logs=trace_logs,
                    trace=trace,
                )
                combined_rows.extend(chrono_rows)
                debug_rows.extend(chrono_debug_rows)

            semantic_plan = source_plan["search_types"].get("semantic")
            if (
                semantic_plan
                and semantic_plan["enabled"]
                and semantic_plan["weight"] > 0
            ):
                semantic_rows, semantic_debug_rows = semantic_search_collection(
                    collection=collection,
                    query_specs=semantic_query_specs,
                    source_plan=source_plan,
                    search_plan=semantic_plan,
                    trace_logs=trace_logs,
                    trace=trace,
                )
                combined_rows.extend(semantic_rows)
                debug_rows.extend(semantic_debug_rows)
        except Exception as exc:
            trace_line(
                trace_logs,
                trace,
                f"source {source_id}: skipped after query error {exc}",
            )

    final_results = {}
    for original_query_text in query_texts:
        normalized_query = normalize_query_text(original_query_text)
        response = build_query_response(
            query_text=(
                normalized_query
                if isinstance(normalized_query, str)
                else str(normalized_query)
            ),
            query_rows=combined_rows,
            runtime_plan=runtime_plan,
            debug_rows=debug_rows,
            n_results=n_results,
            trace_logs=trace_logs,
            trace=trace,
        )
        final_results[str(original_query_text)] = response
    return final_results


def query_all_collections(
    chroma_client: chromadb.PersistentClient,
    query_texts: list,
    n_results: int = 5,
    search_options: dict | None = None,
    include_debug: bool = False,
    trace: bool = False,
):
    return query_collections(
        chroma_client=chroma_client,
        query_texts=query_texts,
        n_results=n_results,
        search_options=search_options,
        include_debug=include_debug,
        trace=trace,
    )


def load_env_if_available() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv()


LOCAL_QUERY_RUN = {
    "query_texts": [],
    "n_results": 5,
    "search_options": {
        "preset": SearchManifest.DEFAULT_PRESET,
        "focus": dict(SearchManifest.DEFAULT_FOCUS),
        "enabled_sources": [],
        "disabled_sources": [],
        "enabled_search_types": list(SearchManifest.SEARCH_TYPES),
        "capabilities": [],
    },
    "include_debug": True,
    "trace": False,
    "chroma_path": None,
}


def main(run_config: dict | None = None) -> None:
    load_env_if_available()
    config = run_config or LOCAL_QUERY_RUN
    query_texts = [
        str(query_text).strip()
        for query_text in (config.get("query_texts") or [])
        if str(query_text).strip()
    ]
    if not query_texts:
        raise ValueError(
            "Set LOCAL_QUERY_RUN['query_texts'] in utils/chroma.py before running this module."
        )

    client = get_chroma_client(
        path=config.get("chroma_path") or os.getenv("CHROMA_URL")
    )
    result = query_collections(
        chroma_client=client,
        query_texts=query_texts,
        n_results=max(1, int(config.get("n_results", 5) or 5)),
        search_options=config.get("search_options"),
        include_debug=bool(config.get("include_debug")),
        trace=bool(config.get("trace")),
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
