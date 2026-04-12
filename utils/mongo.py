"""
mongo.py

Contains utilities and adapters to connect to, search in and update mongoDB collections.
"""

import os, pymongo
from functools import lru_cache

DEFAULT_SEARCH_HISTORY_COLLECTION = "media_search_history"
DEFAULT_SEARCH_FEEDBACK_COLLECTION = "media_search_feedback"


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@lru_cache(maxsize=1)
def get_mongo_database() -> pymongo.database.Database:
    client = pymongo.MongoClient(get_required_env("MONGO_URL"))
    return client[get_required_env("MONGO_DB_NAME")]


@lru_cache(maxsize=None)
def get_mongo_collection(
    collection_name: str | None = None,
) -> pymongo.collection.Collection:
    return get_mongo_database()[
        collection_name or get_required_env("MONGO_COLLECTION_NAME")
    ]


@lru_cache(maxsize=1)
def get_search_history_collection(
    default_name: str = DEFAULT_SEARCH_HISTORY_COLLECTION,
) -> pymongo.collection.Collection:
    collection = get_mongo_collection(
        os.getenv("MEDIA_SEARCH_HISTORY_COLLECTION", default_name)
    )
    collection.create_index([("history_user", 1), ("created_at", -1)])
    collection.create_index([("history_user", 1), ("search_key", 1)], unique=True)
    return collection


@lru_cache(maxsize=1)
def get_search_feedback_collection(
    default_name: str = DEFAULT_SEARCH_FEEDBACK_COLLECTION,
) -> pymongo.collection.Collection:
    collection = get_mongo_collection(
        os.getenv("MEDIA_SEARCH_FEEDBACK_COLLECTION", default_name)
    )
    collection.create_index([("history_user", 1), ("created_at", -1)])
    collection.create_index([("history_user", 1), ("query", 1), ("entry_id", 1)])
    return collection


def find_dict_objects(
    objects: list | dict | str,
    collection: pymongo.collection.Collection,
    batch_size: int = 2048,
) -> dict:
    if isinstance(objects, dict):
        object_keys = list(objects.keys())
    elif isinstance(objects, list):
        object_keys = objects
    elif isinstance(objects, str):
        object_keys = [objects]
    else:
        raise TypeError("objects must be a dict, list, or string")

    n = 0
    result_dict = {}
    while n < len(object_keys):
        find_from = object_keys[n : n + batch_size]
        n += batch_size
        result = {
            doc.pop("_id"): doc for doc in collection.find({"_id": {"$in": find_from}})
        }
        result_dict.update(result)
    return result_dict


def upsert_dict_objects(
    objects: dict,
    collection: pymongo.collection.Collection,
    batch_size: int = 2048,
) -> None:
    updates = []
    for key, value in objects.items():
        updates.append(
            pymongo.UpdateOne(
                filter={"_id": key},
                update={"$set": value},
                upsert=True,
            )
        )
        if len(updates) >= batch_size:
            collection.bulk_write(updates)
            updates = []

    if updates:
        collection.bulk_write(updates)


def check_if_exists(
    keys_dict: dict,
    collection: pymongo.collection.Collection,
    required_fields: list[str] | None = None,
):
    ids = list(keys_dict)
    base_filter = {"_id": {"$in": ids}}
    missing = set()
    if required_fields:
        for field in required_fields:
            cursor = collection.find(
                {**base_filter, field: {"$exists": True, "$ne": None}}
            )
            present = {doc["_id"] for doc in cursor}
            missing.update(set(ids) - present)
    found = find_dict_objects(ids, collection)
    if not required_fields:
        missing = set(ids) - set(found)
    return found, list(missing)


def get_random_objects(
    collection: pymongo.collection.Collection,
    n: int = 1,
    batch_size: int = 2048,
):
    final_result = {}
    while n > batch_size:
        pipeline = [{"$sample": {"size": batch_size}}]
        result = {doc.pop("_id"): doc for doc in collection.aggregate(pipeline)}
        final_result.update(result)
        n -= batch_size
    pipeline = [{"$sample": {"size": n}}]
    result = {doc.pop("_id"): doc for doc in collection.aggregate(pipeline)}
    final_result.update(result)
    return final_result


def find_uploaded_documents_by_hash(
    file_hash: str,
    projection: dict | None = None,
) -> list[dict]:
    cleaned_hash = str(file_hash or "").strip()
    if not cleaned_hash:
        return []
    return list(
        get_mongo_collection().find({"metadata.file_hash": cleaned_hash}, projection)
    )


def rename_uploaded_documents_by_hash(file_hash: str, file_name: str) -> list[str]:
    documents = find_uploaded_documents_by_hash(file_hash, {"_id": 1})
    if not documents:
        raise ValueError("Uploaded file was not found in MongoDB.")

    mongo_ids = [document["_id"] for document in documents]
    get_mongo_collection().update_many(
        {"_id": {"$in": mongo_ids}},
        {"$set": {"metadata.file_name": file_name}},
    )
    return [str(entry_id) for entry_id in mongo_ids]


def delete_uploaded_documents_by_hash(file_hash: str) -> tuple[list[str], list[str]]:
    documents = find_uploaded_documents_by_hash(
        file_hash,
        {"_id": 1, "metadata.file_path": 1},
    )
    if not documents:
        return [], []

    mongo_ids = [document["_id"] for document in documents]
    file_paths = sorted(
        {
            str((document.get("metadata") or {}).get("file_path") or "")
            for document in documents
            if str((document.get("metadata") or {}).get("file_path") or "")
        }
    )
    get_mongo_collection().delete_many({"_id": {"$in": mongo_ids}})
    return [str(entry_id) for entry_id in mongo_ids], file_paths
