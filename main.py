"""
main.py

The main orchestrator. Which:
1. Finds and extracts metadata from all images in given folder.
2. Fetches existing descriptions from the local DB.
3. Tries to populate the db with descriptions of images not yet described.
4. Populate new entries in ChromaDB and test retrieval using queries.
"""

import os, json, warnings, hashlib
import pymongo
from dotenv import load_dotenv

from utils.io import index_folder
from utils.mongo import check_if_exists, upsert_dict_objects
from utils.prompt import describe_image
from utils.chroma import get_chroma_client, populate_db, query_all_collections

from google import genai

load_dotenv()
GEM_API_KEY = os.getenv("GEM_API_KEY")
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")
CHROMA_URL = os.getenv("CHROMA_URL")

API_NAME = "gemini"
MODEL_NAME = "gemini-2.5-flash-lite"


# Adapter to convert folder_dict to standard schema
# and to fetch existing entries from MongoDB
def fetch_existing(folder_dict: dict, collection: pymongo.collection.Collection):
    descriptions = {}
    for file_hash in folder_dict:
        metadata = folder_dict[file_hash].copy()
        model_hash = hashlib.sha1((API_NAME + MODEL_NAME).encode("utf-8")).hexdigest()

        entry_hash = file_hash + "_" + model_hash
        metadata.update(
            {
                "file_hash": file_hash,
                "model_hash": model_hash,
                "api_name": API_NAME,
                "model_name": MODEL_NAME,
            }
        )
        descriptions[entry_hash] = {"description": {}, "metadata": metadata}
    found_objects, missing_keys = check_if_exists(
        descriptions,
        collection,
        required_fields=[
            "description",
        ],
    )
    descriptions.update(
        found_objects
    )  # Might need error handling for mismatches in old and current metadata
    return descriptions, missing_keys


def update_metadata(
    descriptions: dict,
    folder_dict: dict,
    collection: pymongo.collection.Collection,
):
    updated_metadata_dict = {}

    for entry_hash, data in descriptions.items():
        fh, mh = entry_hash.split("_", 1)

        meta = data.get("metadata") or {}
        file_hash = meta.get("file_hash") or fh
        model_hash = meta.get("model_hash") or mh

        base = folder_dict.get(file_hash)
        if base is None:
            continue

        metadata = base.copy()
        metadata["file_hash"] = file_hash
        metadata["model_hash"] = model_hash

        model_name = meta.get("model_name")
        api_name = meta.get("api_name")
        if model_name and api_name:
            metadata["api_name"] = api_name
            metadata["model_name"] = model_name

        updated_metadata_dict[f"{file_hash}_{model_hash}"] = {"metadata": metadata}
        descriptions[entry_hash]["metadata"] = metadata

    upsert_dict_objects(objects=updated_metadata_dict, collection=collection)
    return descriptions


def populate_missing(
    descriptions: dict,
    missing_keys: list,
    collection: pymongo.collection.Collection,
    client: genai.Client,
    batch_size: int = 128,
    verbose: bool = False,
):
    new_descriptions = {}
    for missing_key in missing_keys:
        metadata = descriptions[missing_key]["metadata"]
        try:
            description = describe_image(client, metadata)
            if description:
                new_descriptions[missing_key] = {
                    "description": description,
                    "metadata": metadata,
                }
        except genai.errors.APIError as e:
            if str(e.code) == "429":
                warnings.warn(
                    "Received Gemini 'APIError' while running 'describe_image': "
                    "Quota reached! Stopping image analysis.",
                )
                break
            print("Received Gemini 'APIError' while running 'describe_image':", e)
        except Exception as e:
            print("Reached an Exception while running 'describe_image':", e)

        if len(new_descriptions) >= batch_size:
            upsert_dict_objects(new_descriptions, collection)
            descriptions.update(new_descriptions)
            new_descriptions = {}
    if new_descriptions:
        upsert_dict_objects(new_descriptions, collection)
        descriptions.update(new_descriptions)

    if verbose:
        print(json.dumps(descriptions, indent=2))

    return descriptions


def main():
    import time

    # Connect to Gemini
    client = genai.Client(api_key=GEM_API_KEY)
    # Connect to MongoDB
    collection = pymongo.MongoClient(MONGO_URL)[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
    # Connect to ChromaDB
    chroma_client = get_chroma_client(path=CHROMA_URL)

    verbose = True
    images_root = "images_root"

    # Read root folder to find all images and their metadata
    start = time.time()
    folder_dict = index_folder(images_root)
    stop = time.time()
    print(f"Time taken to index folder: {stop - start:.2f} seconds")

    # Read DB to find existing image descriptions
    start = time.time()
    descriptions, missing_keys = fetch_existing(folder_dict, collection)
    stop = time.time()
    print(
        f"Time taken to fetch existing entries from MongoDB: {stop - start:.2f} seconds"
    )

    # Update metadata in DB if needed (e.g. if new images were added to the folder or if the schema was updated)
    start = time.time()
    descriptions = update_metadata(descriptions, folder_dict, collection)
    stop = time.time()
    print(f"Time taken to update metadata in MongoDB: {stop - start:.2f} seconds")

    # Try to populate missing descriptions
    start = time.time()
    descriptions = populate_missing(
        descriptions=descriptions,
        missing_keys=missing_keys,
        collection=collection,
        client=client,
    )
    stop = time.time()
    print(
        f"Time taken to populate missing entries to MongoDB: {stop - start:.2f} seconds"
    )

    # Populate DB results and new descriptions in ChromaDB
    start = time.time()
    populate_db(entries=descriptions, chroma_client=chroma_client)
    stop = time.time()
    print(f"Time taken to populate DB: {stop - start:.2f} seconds")

    # Retrieve from ChromaDB
    start = time.time()
    query_texts = [
        "nature",
        "people",
        "party",
        "daytime",
        "day time",
        "night time",
        ["boy", "girl", "camera"],
        "antifungal",
    ]
    ranked_queries = query_all_collections(
        chroma_client=chroma_client,
        query_texts=query_texts,
    )

    k = 5
    for query_text, result in ranked_queries.items():
        top_k_image_ids = result["ids"][:k]
        top_k_data = [
            descriptions[i]["description"]["content"]["summary"]
            for i in top_k_image_ids
        ]
        print(f"{query_text}:", json.dumps(top_k_data, indent=2))
    stop = time.time()
    print(f"Time taken to query ChromaDB: {stop - start:.2f} seconds")


if __name__ == "__main__":
    main()
