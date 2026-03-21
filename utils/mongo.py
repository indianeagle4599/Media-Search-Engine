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
):
    search_keys = keys_dict.keys()

    found_objects = find_dict_objects(list(search_keys), collection)
    found_keys = found_objects.keys()

    missing_keys = set(search_keys).difference(set(found_keys))

    return found_objects, list(missing_keys)


if __name__ == "__main__":
    url = "mongodb://localhost:27017"
    collection = pymongo.MongoClient(url)["test_db"]["test"]

    one_key = "key_one"
    many_keys = [
        "key_two",
        "key_three",
        "key_four",
        "key_five",
        "key_six",
    ]
    print(find_dict_objects(objects=one_key, collection=collection))
    print(find_dict_objects(objects=many_keys, collection=collection))

    one_dict = {
        "key_one": {
            "name": "object_one",
            "description": "The first object.",
        }
    }
    many_dicts = {
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
        "key_six": {
            "name": "object_six",
            "description": "The sixth object.",
        },
    }
    upsert_dict_objects(objects=one_dict, collection=collection)
    upsert_dict_objects(objects=many_dicts, collection=collection)
