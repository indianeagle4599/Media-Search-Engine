"""Reusable media ingestion orchestration."""

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
from utils.prompt import describe_image_batch


DEFAULT_DESCRIPTION_RIGOR = "medium"
DEFAULT_DESCRIPTION_MAX_INLINE_BYTES = 18 * 1024 * 1024
DEFAULT_ANALYSIS_IMAGE_MAX_WIDTH = 1600
DEFAULT_ANALYSIS_IMAGE_MAX_HEIGHT = 1600
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
    use_dummy_descriptions: bool = False
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
        use_dummy_descriptions=use_dummy_descriptions,
        verbose=verbose,
    )


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


def estimate_description_bytes(metadata: dict, config: IngestConfig) -> int:
    if config.use_dummy_descriptions:
        return 0

    file_path = str(metadata.get("file_path") or "")
    if not file_path:
        return 0

    try:
        return os.path.getsize(file_path)
    except OSError:
        return 0


def iter_missing_batches(
    descriptions: dict,
    missing_keys: list[str],
    config: IngestConfig,
):
    max_batch_size = description_batch_size(config)
    max_inline_bytes = max(1, int(config.description_max_inline_bytes or 1))
    batch_keys = []
    batch_bytes = 0

    for missing_key in missing_keys:
        item_bytes = estimate_description_bytes(
            descriptions[missing_key]["metadata"],
            config,
        )
        if batch_keys and (
            len(batch_keys) >= max_batch_size
            or (item_bytes and batch_bytes + item_bytes > max_inline_bytes)
        ):
            yield batch_keys
            batch_keys = []
            batch_bytes = 0

        batch_keys.append(missing_key)
        batch_bytes += item_bytes

        if len(batch_keys) >= max_batch_size:
            yield batch_keys
            batch_keys = []
            batch_bytes = 0

    if batch_keys:
        yield batch_keys


def build_batch_entries(descriptions: dict, batch_keys: list[str]) -> list[dict]:
    return [
        {
            "entry_id": missing_key,
            "metadata": descriptions[missing_key]["metadata"],
        }
        for missing_key in batch_keys
    ]


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


def apply_batch_output(
    descriptions: dict,
    batch_keys: list[str],
    batch_output: dict[str, dict],
    new_descriptions: dict,
    populated_keys: list[str],
    failed_keys: list[str],
    error_details: dict[str, dict[str, str]],
    config: IngestConfig,
) -> None:
    missing_reason = "No description was generated for this media item."
    for missing_key in batch_keys:
        description = batch_output.get(missing_key)
        if not description:
            record_description_failures(
                failed_keys,
                error_details,
                [missing_key],
                missing_reason,
            )
            continue

        metadata = descriptions[missing_key]["metadata"]
        new_descriptions[missing_key] = {
            "description": annotate_description(description, config),
            "metadata": metadata,
        }
        populated_keys.append(missing_key)


def flush_new_descriptions(
    new_descriptions: dict,
    descriptions: dict,
    config: IngestConfig,
) -> dict:
    if not new_descriptions:
        return {}

    upsert_dict_objects(new_descriptions, config.mongo_collection)
    descriptions.update(new_descriptions)
    return {}


def mark_rate_limited(
    missing_keys: list[str],
    processed_count: int,
    error_details: dict[str, dict[str, str]],
) -> list[str]:
    rate_limited_keys = missing_keys[processed_count:]
    for key in rate_limited_keys:
        error_details[key] = {
            "stage": "description",
            "reason": "Gemini quota reached while generating descriptions.",
        }

    warnings.warn(
        "Received Gemini 'APIError' while running 'describe_image_batch': "
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

    new_descriptions = {}
    populated_keys = []
    failed_keys = []
    rate_limited_keys = []
    error_details: dict[str, dict[str, str]] = {}

    processed_count = 0
    for batch_keys in iter_missing_batches(descriptions, missing_keys, config):
        try:
            batch_output = describe_image_batch(
                client=config.genai_client,
                batch_entries=build_batch_entries(descriptions, batch_keys),
                use_dummy_descriptions=config.use_dummy_descriptions,
                analysis_image_max_width=config.analysis_image_max_width,
                analysis_image_max_height=config.analysis_image_max_height,
            )
        except genai.errors.APIError as exc:
            if str(exc.code) == "429":
                rate_limited_keys = mark_rate_limited(
                    missing_keys,
                    processed_count,
                    error_details,
                )
                break
            print(
                "Received Gemini 'APIError' while running 'describe_image_batch':", exc
            )
            record_description_failures(
                failed_keys,
                error_details,
                batch_keys,
                str(exc),
            )
        except Exception as exc:
            print("Reached an Exception while running 'describe_image_batch':", exc)
            record_description_failures(
                failed_keys,
                error_details,
                batch_keys,
                str(exc),
            )
        else:
            apply_batch_output(
                descriptions,
                batch_keys,
                batch_output,
                new_descriptions,
                populated_keys,
                failed_keys,
                error_details,
                config,
            )

        processed_count += len(batch_keys)

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
            entry_id: {"metadata.dates.chroma_indexed_at": indexed_at}
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
