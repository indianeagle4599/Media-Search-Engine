"""
chroma.py

Contains utilities to create, update and use a chromadb for storing and querying from the descriptions of all images.
"""

import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from collections.abc import MutableMapping

chroma_client = chromadb.PersistentClient(path="./.chromadb")
ollama_ef = OllamaEmbeddingFunction(
    url="http://localhost:11434",
    model_name="mxbai-embed-large",
)


def flatten(dictionary, parent_key="", separator="."):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def prep_dict_for_upsert(db_dict: dict):
    ids, documents, metadatas = [], [], []
    for key, val in db_dict.items():
        if val:
            description = val["content"]["summary"]
            ids.append(str(key))
            documents.append(str(description))
            metadatas.append(flatten(val))
    return ids, documents, metadatas


def add_to_db(
    db_dict: dict,
    chroma_client: chromadb.Client = chroma_client,
    collection_name: str = "test",
):
    collection = chroma_client.create_collection(
        name=collection_name,
        configuration={"hnsw": {"space": "cosine"}},
        embedding_function=ollama_ef,
        get_or_create=True,
    )
    if db_dict:
        ids, documents, metadatas = prep_dict_for_upsert(db_dict=db_dict)
        collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
    else:
        print("Empty dict, added nothing to collection.")
    return collection


if __name__ == "__main__":
    import json

    descriptions: dict = {}
    with open("json_outs\\test_outs.json", "r") as f:
        descriptions = json.load(f)

    my_collection = add_to_db(descriptions, collection_name="image_descriptions")
    test_queries = ["people", "documents", "wallpapers", "party", "nature"]
    results = {}
    for test_query in test_queries:
        result = my_collection.query(query_texts=[test_query], n_results=5)
        # results[test_query] = result
        results[test_query] = {key: result[key] for key in ["documents", "distances"]}
    print(json.dumps(results, indent=2))
