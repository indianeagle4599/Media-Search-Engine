"""
chroma.py

Contains utilities to create, update and use a chromadb for storing and querying from the descriptions of all images.
"""

import chromadb, json
from chromadb.utils.batch_utils import create_batches
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
)

import pandas as pd

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
    "content_narrative": 1.0,
    "context_narrative": 1.0,
    "lexical_keywords": 0.7,
    "ocr_content": 0.4,
    "other_data": 0.1,
}

collection_type_map = {
    # Bigger, more narrative, contextual fields that may benefit from a more powerful embedding model
    "sentence": ["content_narrative", "context_narrative", "ocr_content"],
    # Smaller, more discrete fields that may be well-handled by a lighter embedding model
    # and a hybrid search strategy that combines lexical and semantic search
    "word": ["lexical_keywords", "other_data"],
    # Absolute (non-semantic)
    "absolute": ["estimated_date"],
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
    return {
        "absolute": {
            # Will need to fix canonicalization and resolution of dates
            # From metadata object
            "creation_date": metadata_object.get("creation_date"),
            "modification_date": metadata_object.get("modification_date"),
            "index_date": metadata_object.get("index_date"),
            # From extracted_metadata field
            ## None yet. Need to add Exif tags.
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
    for entry_hash, entry_object in entries.items():
        metadata = entry_object.get("metadata")
        if metadata:
            extracted_fields = extract_metadata_fields(metadata_object=metadata)
            if entry_object["description"]:
                description = entry_object.get("description")
                extracted_fields = merge_dicts(
                    extracted_fields,
                    extract_description_fields(description_object=description),
                )
                if verbose:
                    print(json.dumps(extracted_fields, indent=2))
            for field_type, field_object in extracted_fields.items():
                for field_name, field_value in field_object.items():
                    if field_name in class_wise_db_dict[field_type]:
                        class_wise_db_dict[field_type][field_name][
                            entry_hash
                        ] = field_value
                    else:
                        class_wise_db_dict[field_type][field_name] = {
                            entry_hash: field_value
                        }

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
                        batches = create_batches(
                            chroma_client, ids=new_ids, documents=new_documents
                        )
                    else:
                        continue
                else:
                    batches = create_batches(
                        chroma_client, ids=ids, documents=documents
                    )

                for batch_ids, _, _, batch_documents in batches:
                    collection.upsert(ids=batch_ids, documents=batch_documents)

            elif field_type == "absolute":
                # NO EMBEDDINGS, create a collection with all flat, non-semantic fields
                pass


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
    if isinstance(query_text, str):
        mask = query_results_df["query_text"] == query_text
    elif isinstance(query_text, list):
        mask = query_results_df["query_text"].isin(query_text)
    relevant_df = query_results_df[mask].copy()

    if relevant_df.empty:
        return pd.DataFrame(columns=["ids", "score", "rank"])

    relevant_df["ids"] = relevant_df["ids"].str[
        :105
    ]  # sha256 (64 chars) + "_" + sha1 (40 chars) (64+1+40 = 105)
    weights = relevant_df["collection"].map(field_weight_dict).fillna(0.2)
    relevant_df["rrf_score"] = weights / (relevant_df["rank"] + rrf_smoothing)

    rrf_vals = relevant_df.groupby("ids")["rrf_score"].sum().nlargest(n_results)
    ranks = pd.DataFrame(
        {
            "ids": rrf_vals.index,
            "score": rrf_vals.values,
            "rank": list(range(len(rrf_vals))),
        }
    )
    return ranks


def clean_query_texts(query_texts: list):
    cleaned_query_texts = set()
    for query_text in query_texts:
        if isinstance(query_text, str):
            cleaned_query_texts.add(query_text.strip().lower())
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

    combined_query_results = {
        "ids": [],
        "documents": [],
        "distances": [],
        "rank": [],
        "query_text": [],
        "collection": [],
    }
    collections = chroma_client.list_collections()
    for col in collections:
        col_name = col.name if hasattr(col, "name") else col
        col_type = collection_type_rev_map.get(col_name) or "sentence"

        collection_ef = collection_ef_map.get(col_type)
        collection = chroma_client.get_collection(
            col_name, embedding_function=collection_ef
        )

        if collection.count() == 0:
            continue

        query_dict = {
            qt: query_embedding_cache.get(qt, {}).get(id(collection_ef)) or []
            for qt in query_embedding_cache
        }
        query_results_dict = semantic_search_collection(
            collection=collection, query_dict=query_dict, n_results=n_results
        )
        if query_results_dict:
            for key in combined_query_results:
                combined_query_results[key].extend(query_results_dict.get(key, []))

    if combined_query_results:
        combined_query_results = pd.DataFrame(combined_query_results)

    final_results = {}
    for query_text in query_texts:
        result = get_final_results(
            query_text, combined_query_results, n_results=n_results
        )
        final_results[str(query_text)] = {k: list(result[k]) for k in result}

    return final_results
