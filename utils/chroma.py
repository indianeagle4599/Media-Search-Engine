"""
chroma.py

Contains utilities to create, update and use a chromadb for storing and querying from the descriptions of all images.
"""

import chromadb, json, os, re, multiprocessing as mp
from datetime import datetime
from chromadb.utils.batch_utils import create_batches
from urllib import error as urllib_error
from urllib import request as urllib_request

import pandas as pd

from utils.date import (
    build_date_where_clause,
    count_mask_specificity,
    date_dict_to_ts,
    extract_date_filter_from_query,
)

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

default_embedding_key = "ollama_all_minilm_l6_v2"
sentence_embedding_key = "ollama_mxbai_embed_large"
embedding_model_map = {
    default_embedding_key: "all-minilm:l6-v2",
    sentence_embedding_key: "mxbai-embed-large",
}
collection_embedding_key_map = {
    "sentence": sentence_embedding_key,
    "list": default_embedding_key,
    "word": default_embedding_key,
}
process_embedding_function_cache = {}

DEFAULT_EMBEDDING_PROCESS_COUNT = min(4, os.cpu_count() or 1)
DEFAULT_EMBEDDING_PARALLEL_MIN_DOCS = 24
DEFAULT_EMBEDDING_CHUNK_COUNT_PER_PROCESS = 2
DEFAULT_OLLAMA_KEEP_ALIVE = "2m"


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
    token_metadata = {
        "token_string": " ".join(tokens),
        "token_count": len(tokens),
    }
    if tokens:
        token_metadata["tokens"] = tokens
    return token_metadata


def get_embedding_function(embedding_key: str):
    model_name = embedding_model_map.get(embedding_key)
    if model_name is None:
        raise KeyError(f"Unknown embedding key: {embedding_key}")

    embedding_function = process_embedding_function_cache.get(embedding_key)
    if embedding_function is not None:
        return embedding_function

    embedding_function = OllamaKeepAliveEmbeddingFunction(model_name=model_name)
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
    try:
        with mp.get_context("spawn").Pool(processes=pool_size) as pool:
            embedding_batches = pool.starmap(
                embed_documents_with_function,
                [(batch, embedding_key) for batch in document_batches],
            )
    except (
        AssertionError,
        AttributeError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ):
        return embed_documents_with_function(documents, embedding_key)

    embeddings = []
    for embedding_batch in embedding_batches:
        embeddings.extend(embedding_batch)
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
            ssl = os.getenv("CHROMA_SSL", "").strip().lower() in {
                "1",
                "true",
                "yes",
            }
        return chromadb.HttpClient(host=host, port=port, ssl=ssl)

    path = path or os.getenv("CHROMA_URL")
    if not path:
        raise RuntimeError("Set CHROMA_URL or CHROMA_HOST/CHROMA_SERVER_HOST.")

    return chromadb.PersistentClient(path=path)


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


def upsert_batch_to_collection(collection, batches, embedding_key: str | None = None):
    prepared_batches = []
    documents_to_embed = []
    for batch_ids, _, batch_metadatas, batch_documents in batches:
        if batch_metadatas is None:
            batch_metadatas = [None] * len(batch_ids)
        else:
            batch_metadatas = list(batch_metadatas)
        for i in range(len(batch_ids)):
            tokens_dict = tokenize_document(batch_documents[i])
            if batch_metadatas and isinstance(batch_metadatas[i], dict):
                batch_metadatas[i].update(tokens_dict)
            else:
                batch_metadatas[i] = tokens_dict
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
        collection.upsert(
            **upsert_kwargs,
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
                ids, documents = prep_dict_for_upsert(field_dict)

                if not ids or not documents:
                    continue

                collection = chroma_client.create_collection(
                    name=field_name,
                    configuration={"hnsw": {"space": "cosine"}},
                    get_or_create=True,
                )
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
                upsert_batch_to_collection(
                    collection,
                    batches,
                    embedding_key=collection_embedding_key_map.get(field_type),
                )


def chronological_search_collection(
    collection: chromadb.Collection,
    query_specs: dict[str, dict],
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

    for query_text, query_spec in query_specs.items():
        date_filters = query_spec.get("date_filters", [])
        if not date_filters:
            continue

        for filter_i, date_filter in enumerate(date_filters):
            start_mask = date_filter.get("start_mask")
            end_mask = date_filter.get("end_mask")
            if not start_mask or not end_mask:
                continue

            where_clause = build_date_where_clause(date_field, date_filter)
            if not where_clause:
                continue

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
                query_results_dict["collection"].append(
                    f"{collection.name}_chrono_{filter_i}"
                )

    return query_results_dict


def lexical_search_collection(
    collection: chromadb.Collection, query_specs: dict[str, dict], n_results: int = 50
):
    query_results_dict = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }

    for query_text, query_spec in query_specs.items():
        query_tokens = query_spec.get("tokens", [])
        if not query_tokens:
            continue

        query_token_string = query_spec.get("token_string", "")
        if len(query_tokens) == 1:
            token_where = {"tokens": {"$contains": query_tokens[0]}}
        else:
            token_where = {
                "$or": [{"tokens": {"$contains": token}} for token in query_tokens]
            }

        query_tokens = set(query_tokens)

        query_result = collection.get(
            where=token_where,
            include=["documents", "metadatas"],
        )

        ids = query_result.get("ids", [])
        documents = query_result.get("documents", [])
        metadatas = query_result.get("metadatas", [])

        scored = []
        for id_, doc_, meta_ in zip(ids, documents, metadatas):
            meta_ = meta_ or {}

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


def semantic_search_collection(
    collection: chromadb.Collection,
    query_specs: dict[str, dict],
    embedding_key: str,
    n_results: int = 50,
):
    final_query_texts = []
    query_embeddings = []
    for query_text, query_spec in query_specs.items():
        query_embedding = (query_spec.get("embeddings") or {}).get(embedding_key)
        if query_embedding is None:
            raise ValueError(f"Embedding for query text '{query_text}' is missing.")
        final_query_texts.append(query_text)
        query_embeddings.append(query_embedding)

    query_results = collection.query(
        query_embeddings=query_embeddings,
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
    chrono_mask = (
        relevant_df["collection"]
        .astype(str)
        .str.startswith("context_narrative_chrono_")
    )
    if chrono_mask.any():
        chrono_df = relevant_df[chrono_mask].copy()
        chrono_df["chrono_rrf_score"] = 1 / (chrono_df["rank"] + rrf_smoothing)
        chrono_vals = (
            chrono_df.groupby("ids")["chrono_rrf_score"]
            .sum()
            .sort_values(ascending=False)
        )
        chrono_agg_df = pd.DataFrame(
            {
                "ids": chrono_vals.index,
                "documents": [""] * len(chrono_vals),
                "distances": -chrono_vals.values,
                "rank": list(range(1, len(chrono_vals) + 1)),
                "query_text": [relevant_df["query_text"].iloc[0]] * len(chrono_vals),
                "collection": ["context_narrative_chrono"] * len(chrono_vals),
            }
        )
        relevant_df = pd.concat(
            [relevant_df[~chrono_mask], chrono_agg_df], ignore_index=True
        )

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

def query_all_collections(
    chroma_client: chromadb.PersistentClient, query_texts: list, n_results: int = 5
):
    query_specs = {}
    pending_query_texts = list(reversed(query_texts))
    while pending_query_texts:
        query_text = pending_query_texts.pop()
        if isinstance(query_text, list):
            pending_query_texts.extend(reversed(query_text))
            continue
        if not isinstance(query_text, str):
            continue

        normalized_query_text = normalize_query_text(query_text)
        if normalized_query_text in query_specs:
            continue

        date_info = extract_date_filter_from_query(normalized_query_text)
        clean_query_text = date_info.get("clean_query_text", "")
        token_metadata = tokenize_document(clean_query_text)
        query_tokens = token_metadata.get("tokens", [])
        query_specs[normalized_query_text] = {
            "query_text": normalized_query_text,
            "clean_query_text": clean_query_text,
            "tokens": query_tokens,
            "token_string": token_metadata.get("token_string", ""),
            "date_filters": date_info.get("date_filters", []),
            "is_pure_date_query": bool(date_info.get("date_filters")) and not query_tokens,
            "embeddings": {},
        }

    semantic_query_specs = {
        query_text: query_spec
        for query_text, query_spec in query_specs.items()
        if query_spec.get("tokens")
    }
    if semantic_query_specs:
        semantic_query_texts = list(semantic_query_specs.keys())
        for embedding_key in dict.fromkeys(collection_embedding_key_map.values()):
            embeddings = generate_embeddings_for_key(
                semantic_query_texts,
                embedding_key,
            )
            for i, query_text in enumerate(semantic_query_texts):
                semantic_query_specs[query_text]["embeddings"][embedding_key] = (
                    embeddings[i]
                )

    chronological_query_specs = {
        query_text: query_spec
        for query_text, query_spec in query_specs.items()
        if query_spec.get("date_filters")
    }

    combined_query_results = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }

    existing_collection_refs = {}
    for collection_ref in chroma_client.list_collections():
        collection_name = (
            collection_ref.name
            if hasattr(collection_ref, "name")
            else collection_ref
        )
        if collection_name:
            existing_collection_refs[collection_name] = collection_ref

    for col_name in collection_dict:
        collection_ref = existing_collection_refs.get(col_name)
        if collection_ref is None:
            continue

        col_type = collection_type_rev_map.get(col_name) or "sentence"
        collection_embedding_key = collection_embedding_key_map.get(col_type)
        collection = (
            collection_ref
            if hasattr(collection_ref, "query")
            else chroma_client.get_collection(col_name)
        )

        if semantic_query_specs:
            lexical_query_results_dict = lexical_search_collection(
                collection=collection,
                query_specs=semantic_query_specs,
                n_results=min(n_results * 50, 500),
            )
            if lexical_query_results_dict:
                for key in combined_query_results:
                    combined_query_results[key].extend(
                        lexical_query_results_dict.get(key, [])
                    )

        if col_name == "context_narrative" and chronological_query_specs:
            chrono_query_results_dict = chronological_search_collection(
                collection=collection,
                query_specs=chronological_query_specs,
                date_field="master_date",
                n_results=min(n_results * 50, 500),
            )
            if chrono_query_results_dict:
                for key in combined_query_results:
                    combined_query_results[key].extend(
                        chrono_query_results_dict.get(key, [])
                    )

        if semantic_query_specs and collection_embedding_key:
            semantic_query_results_dict = semantic_search_collection(
                collection=collection,
                query_specs=semantic_query_specs,
                embedding_key=collection_embedding_key,
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
