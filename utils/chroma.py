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

collection_type_map = {
    ## Sentence-like
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
collection_type_rev_map = {v_i: k for k, v in collection_type_map.items() for v_i in v}

minilm_ef = DefaultEmbeddingFunction()  # Currently "ONNXMiniLM_L6_V2" as of 23/03/2026
ollama_ef = OllamaEmbeddingFunction(
    url="http://localhost:11434",
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

    return {
        ## Sentence-like
        "sentence": {
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
        },
        # List-like
        "list": {
            # From content
            "objects": content_object.get("objects"),
            "vibe": content_object.get("vibe"),
        },
        # Word-like
        "word": {
            # From content
            "background": content_object.get("background"),
            # From context
            "primary_category": context_object.get("primary_category"),
            "intent": context_object.get("intent"),  # May need to handle "/"
            "composition": context_object.get("composition"),
        },
        # Absolute (non-semantic)
        "absolute": {
            "estimated_date": context_object.get("estimated_date"),
        },
    }


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
    entries: dict, chroma_client: chromadb.PersistentClient, verbose: bool = False
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

                batches = create_batches(chroma_client, ids=ids, documents=documents)
                for batch_ids, _, _, batch_documents in batches:
                    collection.upsert(ids=batch_ids, documents=batch_documents)

            elif field_type == "absolute":
                # NO EMBEDDINGS, create a collection with all flat, non-semantic fields
                pass


def semantic_search_collection(
    collection: chromadb.Collection, query_texts: list[str], n_results: int = 5
):
    final_query_texts = set()
    if len(query_texts):
        for query_i in query_texts:
            if isinstance(query_i, list):
                [final_query_texts.add(query_i_j) for query_i_j in query_i]
            elif isinstance(query_i, str):
                final_query_texts.add(query_i)

    final_query_texts = list(final_query_texts)
    if not len(final_query_texts):
        return pd.DataFrame()

    query_results = collection.query(
        query_texts=final_query_texts,
        n_results=n_results,
        include=["documents", "distances"],
    )

    query_results_df = []
    for i, query_text in enumerate(final_query_texts):
        query_result = pd.DataFrame(
            {
                q: v[i]
                for q, v in query_results.items()
                if v is not None and q in ["ids", "documents", "distances"]
            }
        )
        query_result["rank"] = list(range(1, 1 + len(query_result.iloc[:, 0])))
        query_result["query_text"] = query_text
        query_result["collection"] = collection.name
        query_results_df.append(query_result)

    query_results_df = pd.concat(query_results_df).reset_index(drop=True)
    return query_results_df


def get_final_results(
    query_text: str, query_results_df: pd.DataFrame, rrf_smoothing: int = 60
):
    if isinstance(query_text, str):
        relevant_df = query_results_df[
            query_results_df["query_text"] == query_text
        ].copy()
    elif isinstance(query_text, list):
        relevant_df = query_results_df[
            query_results_df["query_text"].isin(query_text)
        ].copy()

    def clean_doc_id(doc_id):
        # sha256 (64 chars) + "_" + sha1 (40 chars) (64+1+40 = 105)
        return doc_id[:105]

    relevant_df["ids"] = relevant_df["ids"].apply(lambda x: clean_doc_id(x))
    relevant_df["rrf_score"] = 1 / (relevant_df["rank"] + rrf_smoothing)

    rrf_vals = (
        relevant_df.groupby("ids")["rrf_score"].sum().sort_values(ascending=False)
    )
    ranks = pd.DataFrame(
        {
            "ids": rrf_vals.index,
            "score": rrf_vals.values,
            "rank": list(range(len(rrf_vals))),
        }
    )
    return ranks


def query_all_collections(
    chroma_client: chromadb.PersistentClient, query_texts: list, n_results: int = 5
):
    combined_query_results = []
    collections = chroma_client.list_collections()
    for col in collections:
        col_name = col.name if hasattr(col, "name") else col
        col_type = collection_type_rev_map.get(col_name) or "sentence"

        collection = chroma_client.get_collection(
            col_name, embedding_function=collection_ef_map.get(col_type)
        )
        query_results = semantic_search_collection(
            collection=collection, query_texts=query_texts, n_results=n_results
        )
        combined_query_results.append(query_results)
    combined_query_results = pd.concat(combined_query_results).reset_index(drop=True)

    final_results = {}
    for query_text in query_texts:
        result = get_final_results(query_text, combined_query_results)
        final_results[str(query_text)] = {k: list(result[k]) for k in result}

    return final_results
