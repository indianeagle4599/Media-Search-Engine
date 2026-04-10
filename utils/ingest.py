"""Reusable media ingestion orchestration."""

import hashlib
import json
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import pymongo
from google import genai

from utils.chroma import populate_db
from utils.io import index_folder, index_paths
from utils.mongo import check_if_exists, upsert_dict_objects
from utils.prompt import describe_image


@dataclass
class IngestConfig:
    api_name: str
    model_name: str
    mongo_collection: pymongo.collection.Collection
    chroma_client: Any
    genai_client: genai.Client | None = None
    update_existing_metadata: bool = True
    batch_size: int = 128
    verbose: bool = False


@dataclass
class IngestResult:
    folder_dict: dict
    descriptions: dict
    missing_keys: list[str] = field(default_factory=list)
    duplicate_existing_keys: list[str] = field(default_factory=list)
    metadata_updated_keys: list[str] = field(default_factory=list)
    populated_keys: list[str] = field(default_factory=list)
    chroma_indexed_keys: list[str] = field(default_factory=list)
    failed_keys: list[str] = field(default_factory=list)
    rate_limited_keys: list[str] = field(default_factory=list)
    error_details: dict[str, dict[str, str]] = field(default_factory=dict)
    timings: dict[str, float] = field(default_factory=dict)


def model_hash(api_name: str, model_name: str) -> str:
    return hashlib.sha1((api_name + model_name).encode("utf-8")).hexdigest()


def entry_id_for_file(file_hash: str, config: IngestConfig) -> str:
    return f"{file_hash}_{model_hash(config.api_name, config.model_name)}"


def prepare_descriptions(folder_dict: dict, config: IngestConfig) -> dict:
    descriptions = {}
    mh = model_hash(config.api_name, config.model_name)
    for file_hash, file_metadata in folder_dict.items():
        metadata = file_metadata.copy()
        entry_hash = f"{file_hash}_{mh}"
        metadata.update(
            {
                "file_hash": file_hash,
                "model_hash": mh,
                "api_name": config.api_name,
                "model_name": config.model_name,
            }
        )
        descriptions[entry_hash] = {"description": {}, "metadata": metadata}
    return descriptions


def fetch_existing(folder_dict: dict, config: IngestConfig):
    descriptions = prepare_descriptions(folder_dict, config)
    found_objects, missing_keys = check_if_exists(
        descriptions,
        config.mongo_collection,
        required_fields=["description.content"],
    )
    descriptions.update(found_objects)
    return descriptions, missing_keys, found_objects


def update_metadata(
    descriptions: dict,
    folder_dict: dict,
    config: IngestConfig,
    keys_to_update: set[str] | None = None,
) -> tuple[dict, list[str]]:
    updated_metadata_dict = {}

    for entry_hash, data in descriptions.items():
        if keys_to_update is not None and entry_hash not in keys_to_update:
            continue

        fh, mh = entry_hash.split("_", 1)
        meta = data.get("metadata") or {}
        file_hash = meta.get("file_hash") or fh
        model_hash_value = meta.get("model_hash") or mh
        base = folder_dict.get(file_hash)
        if base is None:
            continue

        metadata = base.copy()
        metadata["file_hash"] = file_hash
        metadata["model_hash"] = model_hash_value
        metadata["api_name"] = meta.get("api_name") or config.api_name
        metadata["model_name"] = meta.get("model_name") or config.model_name

        updated_metadata_dict[f"{file_hash}_{model_hash_value}"] = {
            "metadata": metadata
        }
        descriptions[entry_hash]["metadata"] = metadata

    if updated_metadata_dict:
        upsert_dict_objects(
            objects=updated_metadata_dict,
            collection=config.mongo_collection,
        )
    return descriptions, list(updated_metadata_dict)


def populate_missing(
    descriptions: dict,
    missing_keys: list[str],
    config: IngestConfig,
) -> tuple[dict, list[str], list[str], list[str], dict[str, dict[str, str]]]:
    if not missing_keys or not config.genai_client:
        return descriptions, [], [], [], {}

    new_descriptions = {}
    populated_keys = []
    failed_keys = []
    rate_limited_keys = []
    error_details: dict[str, dict[str, str]] = {}

    for index, missing_key in enumerate(missing_keys):
        metadata = descriptions[missing_key]["metadata"]
        try:
            description = describe_image(config.genai_client, metadata)
            if description:
                new_descriptions[missing_key] = {
                    "description": description,
                    "metadata": metadata,
                }
                populated_keys.append(missing_key)
            else:
                failed_keys.append(missing_key)
                error_details[missing_key] = {
                    "stage": "description",
                    "reason": "No description was generated for this media item.",
                }
        except genai.errors.APIError as exc:
            if str(exc.code) == "429":
                rate_limited_keys = missing_keys[index:]
                for key in rate_limited_keys:
                    error_details[key] = {
                        "stage": "description",
                        "reason": "Gemini quota reached while generating descriptions.",
                    }
                warnings.warn(
                    "Received Gemini 'APIError' while running 'describe_image': "
                    "Quota reached. Stopping image analysis.",
                )
                break
            print("Received Gemini 'APIError' while running 'describe_image':", exc)
            failed_keys.append(missing_key)
            error_details[missing_key] = {
                "stage": "description",
                "reason": str(exc),
            }
        except Exception as exc:
            print("Reached an Exception while running 'describe_image':", exc)
            failed_keys.append(missing_key)
            error_details[missing_key] = {
                "stage": "description",
                "reason": str(exc),
            }

        if len(new_descriptions) >= config.batch_size:
            upsert_dict_objects(new_descriptions, config.mongo_collection)
            descriptions.update(new_descriptions)
            new_descriptions = {}

    if new_descriptions:
        upsert_dict_objects(new_descriptions, config.mongo_collection)
        descriptions.update(new_descriptions)

    if config.verbose:
        print(json.dumps(descriptions, indent=2))

    return (
        descriptions,
        populated_keys,
        failed_keys,
        rate_limited_keys,
        error_details,
    )


def has_description(entry: dict) -> bool:
    description = entry.get("description")
    return bool(description and description.get("content"))


def has_chroma_index(entry: dict) -> bool:
    indexing = entry.get("indexing") or {}
    return bool(indexing.get("chroma_indexed_at"))


def mark_chroma_indexed(entry_ids: list[str], config: IngestConfig) -> str | None:
    if not entry_ids:
        return None

    indexed_at = datetime.now(timezone.utc).isoformat()
    upsert_dict_objects(
        objects={
            entry_id: {"indexing.chroma_indexed_at": indexed_at}
            for entry_id in entry_ids
        },
        collection=config.mongo_collection,
    )
    return indexed_at


def ingest_index(
    folder_dict: dict,
    config: IngestConfig,
) -> IngestResult:
    timings = {}

    start = perf_counter()
    descriptions, missing_keys, found_objects = fetch_existing(folder_dict, config)
    timings["fetch_existing"] = perf_counter() - start
    missing_keys = sorted(set(missing_keys))

    duplicate_existing_keys = sorted(
        key
        for key, entry in found_objects.items()
        if has_description(entry) and has_chroma_index(entry)
    )
    keys_to_update = None
    if not config.update_existing_metadata:
        keys_to_update = set(missing_keys)

    start = perf_counter()
    descriptions, metadata_updated_keys = update_metadata(
        descriptions=descriptions,
        folder_dict=folder_dict,
        config=config,
        keys_to_update=keys_to_update,
    )
    timings["update_metadata"] = perf_counter() - start

    start = perf_counter()
    (
        descriptions,
        populated_keys,
        failed_keys,
        rate_limited_keys,
        error_details,
    ) = populate_missing(
        descriptions=descriptions,
        missing_keys=missing_keys,
        config=config,
    )
    timings["populate_missing"] = perf_counter() - start

    if config.update_existing_metadata:
        chroma_entries = {
            key: entry
            for key, entry in descriptions.items()
            if has_description(entry)
        }
    else:
        chroma_entries = {
            key: entry
            for key, entry in descriptions.items()
            if has_description(entry) and not has_chroma_index(entry)
        }

    start = perf_counter()
    chroma_indexed_keys: list[str] = []
    if chroma_entries and config.chroma_client:
        populate_db(
            entries=chroma_entries,
            chroma_client=config.chroma_client,
            overwrite=config.update_existing_metadata,
        )
        indexed_at = mark_chroma_indexed(list(chroma_entries), config)
        if indexed_at:
            for key in chroma_entries:
                descriptions.setdefault(key, {}).setdefault("indexing", {})[
                    "chroma_indexed_at"
                ] = indexed_at
        chroma_indexed_keys = list(chroma_entries)
    timings["populate_chroma"] = perf_counter() - start

    return IngestResult(
        folder_dict=folder_dict,
        descriptions=descriptions,
        missing_keys=missing_keys,
        duplicate_existing_keys=duplicate_existing_keys,
        metadata_updated_keys=metadata_updated_keys,
        populated_keys=populated_keys,
        chroma_indexed_keys=chroma_indexed_keys,
        failed_keys=failed_keys,
        rate_limited_keys=rate_limited_keys,
        error_details=error_details,
        timings=timings,
    )


def ingest_folder(root_path: str, config: IngestConfig) -> IngestResult:
    start = perf_counter()
    folder_dict = index_folder(root_path)
    index_seconds = perf_counter() - start
    result = ingest_index(folder_dict=folder_dict, config=config)
    result.timings = {"index_files": index_seconds, **result.timings}
    return result


def ingest_files(
    file_paths: list[str],
    config: IngestConfig,
    metadata_overrides: dict[str, dict] | None = None,
) -> IngestResult:
    start = perf_counter()
    folder_dict = index_paths(
        file_paths=file_paths,
        metadata_overrides=metadata_overrides,
    )
    index_seconds = perf_counter() - start
    result = ingest_index(
        folder_dict=folder_dict,
        config=config,
    )
    result.timings = {"index_files": index_seconds, **result.timings}
    return result
