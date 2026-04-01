"""
mongo.py

Contains utilities and adapters to connect to, search in and update mongoDB collections.
"""

import os, pymongo


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
        object_keys = [
            objects,
        ]

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
        filter_dict = {"_id": key}
        update_dict = {"$set": value}
        updates.append(
            pymongo.UpdateOne(filter=filter_dict, update=update_dict, upsert=True)
        )
        if len(updates) >= batch_size:
            collection.bulk_write(updates)
            updates = []

    if updates:
        collection.bulk_write(updates)


def check_if_exists(
    keys_dict: dict,
    collection: pymongo.collection.Collection,
    required_fields: (
        list[str] | None
    ) = None,  # Pass nested fields as dot paths, e.g. "description.content"
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


if __name__ == "__main__":
    import random, json

    url = "mongodb://localhost:27017"
    collection = pymongo.MongoClient(url)["test_db"]["test"]

    print("\nUpserting objects:")
    dicts = {
        "key_one": {
            "name": "object_one",
            "description": "The first object.",
        },
        "key_two": {
            "name": "object_two",
            "description": "The second object.",
        },
        "key_three": {
            "name": "object_three",
            "description": "The third object.",
        },
        "key_four": {
            "name": "object_four",
            "description": "The fourth object.",
        },
        "key_five": {
            "name": "object_five",
            "description": "The fifth object.",
        },
    }
    upsert_dict_objects(objects=dicts, collection=collection)

    k = 3
    keys = random.choices(list(dicts.keys()), k=k)
    print(f"Retrieving {k} objects by key:")
    print(json.dumps(find_dict_objects(objects=keys, collection=collection), indent=2))

    print(f"Retrieving {k} objects randomly:")
    print(json.dumps(get_random_objects(collection=collection, n=k), indent=2))
