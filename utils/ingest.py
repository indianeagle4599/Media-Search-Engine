"""
ingest.py

Reusable media ingestion orchestration for Mongo-backed metadata, description generation, and Chroma indexing.
"""

import hashlib, json, os, warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import pymongo
from google import genai

from utils.chroma import populate_db
from utils.io import index_folder, index_paths
from utils.mongo import check_if_exists, upsert_dict_objects
from utils.prompt import (
    build_batch_request,
    describe_prepared_batch,
    prepare_batch_entry,
)


DEFAULT_DESCRIPTION_RIGOR = "very low"
DEFAULT_DESCRIPTION_MAX_INLINE_BYTES = 18 * 1024 * 1024
DEFAULT_ANALYSIS_IMAGE_MAX_WIDTH = 1600
DEFAULT_ANALYSIS_IMAGE_MAX_HEIGHT = 1600
DEFAULT_FLAGGED_ANALYSIS_PATH = os.path.join(
    "json_outs",
    "flagged_analysis_files.json",
)
STATUS_STORED = "stored"
STATUS_DESCRIBED = "described"
STATUS_INDEXED = "indexed"
DESCRIPTION_BATCH_SIZE_BY_RIGOR = {
    "very low": 50,
    "low": 20,
    "medium": 10,
    "high": 5,
    "very high": 2,
    "extreme": 1,
}


@dataclass
class IngestConfig:
    api_name: str
    model_name: str
    mongo_collection: pymongo.collection.Collection
    chroma_client: Any
    genai_client: genai.Client | None = None
    update_existing_metadata: bool = True
    batch_size: int = 128
    description_rigor: str = DEFAULT_DESCRIPTION_RIGOR
    description_max_inline_bytes: int = DEFAULT_DESCRIPTION_MAX_INLINE_BYTES
    analysis_image_max_width: int = DEFAULT_ANALYSIS_IMAGE_MAX_WIDTH
    analysis_image_max_height: int = DEFAULT_ANALYSIS_IMAGE_MAX_HEIGHT
    flagged_analysis_path: str = DEFAULT_FLAGGED_ANALYSIS_PATH
    include_flagged_files: bool = False
    use_dummy_descriptions: bool = False
    verbose: bool = False
    progress_callback: Any | None = None


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


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def build_ingest_config_from_env(
    *,
    mongo_collection: pymongo.collection.Collection,
    chroma_client: Any,
    update_existing_metadata: bool,
    run_analysis: bool = True,
    default_description_rigor: str = DEFAULT_DESCRIPTION_RIGOR,
    verbose: bool = False,
    require_api_key: bool = False,
) -> IngestConfig:
    use_dummy_descriptions = run_analysis and env_flag(
        "MEDIA_USE_DUMMY_DESCRIPTIONS",
        default=False,
    )
    genai_client = None
    if run_analysis and not use_dummy_descriptions:
        api_key = os.getenv("GEM_API_KEY")
        if require_api_key and not api_key:
            raise RuntimeError("Missing required environment variable: GEM_API_KEY")
        genai_client = genai.Client(api_key=api_key)

    return IngestConfig(
        api_name=os.getenv("MEDIA_API_NAME", "gemini"),
        model_name=os.getenv("MEDIA_MODEL_NAME", "gemini-2.5-flash-lite"),
        mongo_collection=mongo_collection,
        chroma_client=chroma_client,
        genai_client=genai_client,
        update_existing_metadata=update_existing_metadata,
        description_rigor=os.getenv(
            "MEDIA_DESCRIPTION_RIGOR",
            default_description_rigor,
        ),
        description_max_inline_bytes=int(
            os.getenv(
                "MEDIA_DESCRIPTION_MAX_INLINE_BYTES",
                DEFAULT_DESCRIPTION_MAX_INLINE_BYTES,
            )
        ),
        analysis_image_max_width=int(
            os.getenv(
                "MEDIA_ANALYSIS_IMAGE_MAX_WIDTH",
                DEFAULT_ANALYSIS_IMAGE_MAX_WIDTH,
            )
        ),
        analysis_image_max_height=int(
            os.getenv(
                "MEDIA_ANALYSIS_IMAGE_MAX_HEIGHT",
                DEFAULT_ANALYSIS_IMAGE_MAX_HEIGHT,
            )
        ),
        flagged_analysis_path=os.getenv(
            "MEDIA_FLAGGED_ANALYSIS_PATH",
            DEFAULT_FLAGGED_ANALYSIS_PATH,
        ),
        use_dummy_descriptions=use_dummy_descriptions,
        verbose=verbose,
    )


def model_hash(api_name: str, model_name: str) -> str:
    return hashlib.sha1((api_name + model_name).encode("utf-8")).hexdigest()


def entry_id_for_file(file_hash: str, config: IngestConfig) -> str:
    return f"{file_hash}_{model_hash(config.api_name, config.model_name)}"


def load_flagged_analysis_files(path: str) -> dict[str, dict]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        warnings.warn(f"Could not read flagged analysis file list: {exc}")
        return {}
    return payload if isinstance(payload, dict) else {}


def save_flagged_analysis_files(path: str, flagged_files: dict[str, dict]) -> None:
    if not path:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(flagged_files, file, indent=2)


def entry_file_hash(entry_id: str, metadata: dict) -> str:
    return str((metadata or {}).get("file_hash") or str(entry_id).split("_", 1)[0])


def flag_analysis_file(
    flagged_files: dict[str, dict],
    entry_id: str,
    metadata: dict,
    reason: str,
) -> str:
    file_hash = entry_file_hash(entry_id, metadata)
    flagged_entry = dict(flagged_files.get(file_hash) or {})
    flagged_entry.update(
        {
            "file_hash": file_hash,
            "entry_id": entry_id,
            "file_name": metadata.get("file_name") or "",
            "file_path": metadata.get("file_path") or "",
            "last_reason": reason,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    flagged_entry["failure_count"] = int(flagged_entry.get("failure_count") or 0) + 1
    flagged_files[file_hash] = flagged_entry
    return file_hash


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


def can_describe_missing(config: IngestConfig) -> bool:
    return bool(config.genai_client or config.use_dummy_descriptions)


def normalize_description_rigor(rigor: str) -> str:
    normalized = str(rigor or DEFAULT_DESCRIPTION_RIGOR).strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ")
    normalized = " ".join(normalized.split())
    if normalized in DESCRIPTION_BATCH_SIZE_BY_RIGOR:
        return normalized
    return DEFAULT_DESCRIPTION_RIGOR


def description_batch_size(config: IngestConfig) -> int:
    rigor = normalize_description_rigor(config.description_rigor)
    return DESCRIPTION_BATCH_SIZE_BY_RIGOR[rigor]


def iter_missing_batches(
    descriptions: dict,
    missing_keys: list[str],
    config: IngestConfig,
):
    max_batch_size = description_batch_size(config)
    max_inline_bytes = max(1, int(config.description_max_inline_bytes or 1))
    batch_keys = []
    batch_request = None

    def flush_batch():
        nonlocal batch_keys, batch_request
        if not batch_keys:
            return None
        flushed_batch = {
            "keys": batch_keys,
            "batch_request": batch_request,
        }
        batch_keys = []
        batch_request = None
        return flushed_batch

    for missing_key in missing_keys:
        prepared_entry = prepare_batch_entry(
            missing_key,
            descriptions[missing_key]["metadata"],
            config.use_dummy_descriptions,
            analysis_image_max_width=config.analysis_image_max_width,
            analysis_image_max_height=config.analysis_image_max_height,
        )

        if prepared_entry is None:
            flushed_batch = flush_batch()
            if flushed_batch:
                yield flushed_batch
            yield {
                "keys": [missing_key],
                "batch_request": None,
                "error_reason": "Media item could not be prepared for Gemini analysis.",
            }
            continue

        candidate_entries = (batch_request["entries"] if batch_request else []) + [
            prepared_entry
        ]
        candidate_request = build_batch_request(
            candidate_entries,
            use_dummy_descriptions=config.use_dummy_descriptions,
        )

        if (
            not config.use_dummy_descriptions
            and not batch_keys
            and candidate_request["request_bytes"] > max_inline_bytes
        ):
            yield {
                "keys": [missing_key],
                "batch_request": None,
                "error_reason": (
                    "Prepared Gemini inline request exceeds the configured byte limit."
                ),
            }
            continue

        if batch_keys and candidate_request["request_bytes"] > max_inline_bytes:
            yield flush_batch()
            batch_request = build_batch_request(
                [prepared_entry],
                use_dummy_descriptions=config.use_dummy_descriptions,
            )
            if (
                not config.use_dummy_descriptions
                and batch_request["request_bytes"] > max_inline_bytes
            ):
                batch_request = None
                yield {
                    "keys": [missing_key],
                    "batch_request": None,
                    "error_reason": (
                        "Prepared Gemini inline request exceeds the configured byte limit."
                    ),
                }
                continue
            batch_keys = [missing_key]
        else:
            batch_keys.append(missing_key)
            batch_request = candidate_request

        if len(batch_keys) >= max_batch_size:
            yield flush_batch()

    flushed_batch = flush_batch()
    if flushed_batch:
        yield flushed_batch


def record_description_failures(
    failed_keys: list[str],
    error_details: dict[str, dict[str, str]],
    entry_keys: list[str],
    reason: str,
) -> None:
    for key in entry_keys:
        failed_keys.append(key)
        error_details[key] = {
            "stage": "description",
            "reason": reason,
        }


def annotate_description(description: dict, config: IngestConfig) -> dict:
    annotated = dict(description or {})
    generation = dict(annotated.get("generation") or {})
    generation["rigor"] = normalize_description_rigor(config.description_rigor)
    annotated["generation"] = generation
    return annotated


def get_processing_status(entry: dict) -> str:
    indexing = entry.get("indexing") or {}
    status = str(indexing.get("status") or "").strip().lower()
    if status in {STATUS_STORED, STATUS_DESCRIBED, STATUS_INDEXED}:
        return status
    if has_chroma_index(entry):
        return STATUS_INDEXED
    if has_description(entry):
        return STATUS_DESCRIBED
    return STATUS_STORED


def sync_processing_statuses(
    entry_ids: list[str],
    descriptions: dict,
    config: IngestConfig,
) -> None:
    if not entry_ids:
        return

    updates = {}
    for entry_id in entry_ids:
        entry = descriptions.get(entry_id) or {}
        status = get_processing_status(entry)
        indexing = dict(entry.get("indexing") or {})
        indexing["status"] = status
        entry["indexing"] = indexing
        updates[entry_id] = {"indexing.status": status}

    upsert_dict_objects(updates, config.mongo_collection)


def apply_batch_output(
    descriptions: dict,
    batch_keys: list[str],
    batch_output: dict[str, dict],
    new_descriptions: dict,
    populated_keys: list[str],
    config: IngestConfig,
) -> tuple[int, list[str]]:
    success_count = 0
    missing_output_keys = []
    for missing_key in batch_keys:
        description = batch_output.get(missing_key)
        if not description:
            missing_output_keys.append(missing_key)
            continue

        metadata = descriptions[missing_key]["metadata"]
        new_descriptions[missing_key] = {
            "description": annotate_description(description, config),
            "metadata": metadata,
        }
        populated_keys.append(missing_key)
        success_count += 1
    return success_count, missing_output_keys


def flush_new_descriptions(
    new_descriptions: dict,
    descriptions: dict,
    config: IngestConfig,
) -> dict:
    if not new_descriptions:
        return {}

    upsert_dict_objects(new_descriptions, config.mongo_collection)
    descriptions.update(new_descriptions)
    sync_processing_statuses(list(new_descriptions), descriptions, config)
    return {}


def mark_rate_limited(
    missing_keys: list[str],
    error_details: dict[str, dict[str, str]],
) -> list[str]:
    rate_limited_keys = list(missing_keys)
    for key in rate_limited_keys:
        error_details[key] = {
            "stage": "description",
            "reason": "Gemini quota reached while generating descriptions.",
        }

    warnings.warn(
        "Received Gemini 'APIError' while running a description batch: "
        "Quota reached. Stopping image analysis.",
    )
    return rate_limited_keys


def populate_missing(
    descriptions: dict,
    missing_keys: list[str],
    config: IngestConfig,
) -> tuple[dict, list[str], list[str], list[str], dict[str, dict[str, str]]]:
    if not missing_keys or not can_describe_missing(config):
        return descriptions, [], [], [], {}

    flagged_files = load_flagged_analysis_files(config.flagged_analysis_path)
    if not config.include_flagged_files:
        runnable_missing_keys = []
        skipped_flagged_keys = []
        for missing_key in missing_keys:
            metadata = descriptions.get(missing_key, {}).get("metadata") or {}
            file_hash = entry_file_hash(missing_key, metadata)
            if file_hash in flagged_files:
                skipped_flagged_keys.append(missing_key)
                continue
            runnable_missing_keys.append(missing_key)
        missing_keys = runnable_missing_keys
    else:
        skipped_flagged_keys = []

    failed_keys = []
    error_details = {}
    if skipped_flagged_keys:
        for skipped_key in skipped_flagged_keys:
            metadata = descriptions.get(skipped_key, {}).get("metadata") or {}
            file_hash = entry_file_hash(skipped_key, metadata)
            prior_reason = (flagged_files.get(file_hash) or {}).get("last_reason") or ""
            failed_keys.append(skipped_key)
            error_details[skipped_key] = {
                "stage": "description",
                "reason": (
                    "Skipping analysis because this file is flagged from a prior isolated failure."
                    + (
                        f" Last isolated failure: {prior_reason}"
                        if prior_reason
                        else ""
                    )
                ),
            }
        print(f"Skipping flagged analysis files: {len(skipped_flagged_keys)}")

    if not missing_keys:
        return descriptions, [], failed_keys, [], error_details

    new_descriptions = {}
    populated_keys = []
    rate_limited_keys = []
    pending_keys = list(missing_keys)
    total_runnable_keys = len(missing_keys)

    while pending_keys:
        batch_info = next(
            iter_missing_batches(descriptions, pending_keys, config), None
        )
        if not batch_info:
            break
        batch_keys = batch_info["keys"]
        remaining_keys = pending_keys[len(batch_keys) :]
        error_reason = batch_info.get("error_reason")
        if error_reason:
            record_description_failures(
                failed_keys,
                error_details,
                batch_keys,
                error_reason,
            )
            pending_keys = remaining_keys
            continue

        try:
            batch_output = describe_prepared_batch(
                client=config.genai_client,
                batch_request=batch_info["batch_request"],
                use_dummy_descriptions=config.use_dummy_descriptions,
            )
        except genai.errors.APIError as exc:
            if str(exc.code) == "429":
                rate_limited_keys = mark_rate_limited(
                    pending_keys,
                    error_details,
                )
                break
            print(
                "Received Gemini 'APIError' while running a description batch:",
                exc,
            )
            success_count = 0
            batch_output = {}
            missing_output_keys = batch_keys
        except Exception as exc:
            print(
                "Reached an Exception while running a description batch:",
                exc,
                "| batch_keys=",
                batch_keys,
            )
            success_count = 0
            batch_output = {}
            missing_output_keys = batch_keys
        else:
            success_count, missing_output_keys = apply_batch_output(
                descriptions,
                batch_keys,
                batch_output,
                new_descriptions,
                populated_keys,
                config,
            )

        if missing_output_keys:
            flagged_key = missing_output_keys[0]
            deferred_keys = missing_output_keys[1:]
            metadata = descriptions.get(flagged_key, {}).get("metadata") or {}
            reason = (
                "Description batch returned an incomplete response. "
                "This file was flagged as the first unrecovered item from that batch and will be skipped on future analysis runs unless explicitly included."
            )
            flagged_hash = flag_analysis_file(
                flagged_files,
                flagged_key,
                metadata,
                reason,
            )
            save_flagged_analysis_files(
                config.flagged_analysis_path,
                flagged_files,
            )
            failed_keys.append(flagged_key)
            error_details[flagged_key] = {
                "stage": "description",
                "reason": reason,
            }
            print(
                "Flagged analysis file:",
                metadata.get("file_name") or flagged_key,
                f"({flagged_hash})",
            )
            pending_keys = deferred_keys + remaining_keys
        else:
            pending_keys = remaining_keys

        total_success_count = len(populated_keys)
        print(
            "Completed description batch:",
            f"{success_count}/{len(batch_keys)} succeeded in current batch",
            f"({total_success_count}/{total_runnable_keys} total succeeded so far)",
        )
        if callable(config.progress_callback):
            config.progress_callback(
                {
                    "stage": "description_batch",
                    "completed": total_success_count,
                    "total": total_runnable_keys,
                    "remaining": len(pending_keys),
                    "batch_size": len(batch_keys),
                    "batch_succeeded": success_count,
                }
            )

        if len(new_descriptions) >= config.batch_size:
            new_descriptions = flush_new_descriptions(
                new_descriptions,
                descriptions,
                config,
            )

    new_descriptions = flush_new_descriptions(
        new_descriptions,
        descriptions,
        config,
    )

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


def get_chroma_indexed_at(entry: dict) -> str:
    metadata = entry.get("metadata") or {}
    dates = metadata.get("dates") or {}
    value = dates.get("chroma_indexed_at")
    if value:
        return str(value)

    indexing = entry.get("indexing") or {}
    value = indexing.get("chroma_indexed_at")
    return str(value or "")


def has_chroma_index(entry: dict) -> bool:
    return bool(get_chroma_indexed_at(entry))


def mark_chroma_indexed(entry_ids: list[str], config: IngestConfig) -> str | None:
    if not entry_ids:
        return None

    indexed_at = datetime.now(timezone.utc).isoformat()
    upsert_dict_objects(
        objects={
            entry_id: {
                "metadata.dates.chroma_indexed_at": indexed_at,
                "indexing.status": STATUS_INDEXED,
            }
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
    sync_processing_statuses(list(descriptions), descriptions, config)
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
            key: entry for key, entry in descriptions.items() if has_description(entry)
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
                descriptions.setdefault(key, {}).setdefault("metadata", {}).setdefault(
                    "dates", {}
                )["chroma_indexed_at"] = indexed_at
                descriptions.setdefault(key, {}).setdefault("indexing", {})[
                    "status"
                ] = STATUS_INDEXED
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
