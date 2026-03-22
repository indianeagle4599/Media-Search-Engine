"""
chroma.py

Contains utilities to create, update and use a chromadb for storing and querying from the descriptions of all images.
"""

import chromadb, json
from chromadb.utils.batch_utils import create_batches
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)

ollama_ef = OllamaEmbeddingFunction(
    url="http://localhost:11434",
    model_name="mxbai-embed-large",
)

collection_ef_map = {"sentence": ollama_ef}


def prep_sentence_dict_for_upsert(field_dict: dict):
    ids = []
    documents = []
    for key, val in field_dict.items():
        if val:
            ids.append(str(key))
            documents.append(str(val))
    return ids, documents


def prep_list_dict_for_upsert(field_dict: dict):
    ids, documents = [], []
    for key, val_list in field_dict.items():
        if len(val_list):
            for i, val in enumerate(val_list):
                if val:
                    ids.append(str(key) + f"_object_{i+1}")
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
            collection_kwargs = {
                "name": field_name,
                "configuration": {"hnsw": {"space": "cosine"}},
                "get_or_create": True,
            }
            if field_name in collection_ef_map:
                collection_kwargs["embedding_function"] = collection_ef_map[field_name]

            if field_type == "sentence" or field_type == "word":
                ids, documents = prep_sentence_dict_for_upsert(field_dict)
            elif field_type == "list":
                ids, documents = prep_list_dict_for_upsert(field_dict)
            elif field_type == "absolute":
                # NO EMBEDDINGS, check simplest way to use DB
                pass

            if not ids or not documents or len(ids) != len(documents):
                continue

            collection = chroma_client.create_collection(**collection_kwargs)
            batches = create_batches(chroma_client, ids=ids, documents=documents)
            for batch_ids, _, batch_metadatas, batch_documents in batches:
                collection.upsert(ids=batch_ids, documents=batch_documents)
