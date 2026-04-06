"""
chroma.py

Contains utilities to create, update and use a chromadb for storing and querying from the descriptions of all images.
"""

import json, re

import chromadb
from chromadb.utils.batch_utils import create_batches
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
)

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
        date_filters = date_info.get("date_filters", [])

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

        if not token_where:
            continue

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
    chrono_mask = relevant_df["collection"].astype(str).str.startswith(
        "context_narrative_chrono_"
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
                "query_text": [relevant_df["query_text"].iloc[0]]
                * len(chrono_vals),
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


def clean_query_texts(query_texts: list):
    cleaned = set()
    for query_text in query_texts:
        if isinstance(query_text, str):
            cleaned.add(normalize_query_text(query_text))
        elif isinstance(query_text, list):
            cleaned.update(clean_query_texts(query_text))
    return list(cleaned)


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

    query_info_map = {}
    for qt in cleaned_query_texts:
        query_info_map[qt] = extract_date_filter_from_query(qt)
        query_info_map[qt]["is_pure_date_query"] = (
            len(tokenize_document(query_info_map[qt]["clean_query_text"])["tokens"])
            == 0
        )

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
