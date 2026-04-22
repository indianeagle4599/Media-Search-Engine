"""
prompt.py

Batch image prompt preparation and response handling.
"""

import json, warnings
from typing import Any

from google import genai

from utils.io import get_analysis_image_bytes
from utils.prompt_assembly import (
    batch_prompt_sections as assemble_batch_prompt_sections,
    batch_response_schema,
    dummy_response,
    render_prompt_template,
)
from utils.prompt_parsing import (
    normalize_response_json_text,
    salvage_partial_batch_payload,
)


REQUEST_TEXT_OVERHEAD_BYTES = 64
REQUEST_INLINE_PART_OVERHEAD_BYTES = 128
REQUEST_CONTAINER_OVERHEAD_BYTES = 512
IMAGE_TASK_TYPE = "describe_image"


def batch_prompt_sections(batch_size: int) -> dict[str, str]:
    return assemble_batch_prompt_sections(IMAGE_TASK_TYPE, batch_size)


def dummy_description(entry_id: str, metadata: dict) -> dict:
    return dummy_response(IMAGE_TASK_TYPE, entry_id, metadata)


def prepare_batch_entry(
    entry_id: str,
    metadata: dict,
    use_dummy_descriptions: bool,
    analysis_image_max_width: int | None = None,
    analysis_image_max_height: int | None = None,
) -> dict | None:
    entry_id = str(entry_id or "")
    metadata = metadata or {}
    filename = metadata.get("file_name") or entry_id
    if metadata.get("media_type") != "image":
        warnings.warn(
            f"Skipping [{filename}] as '{metadata.get('media_type')}/{metadata.get('ext')}' is not supported."
        )
        return None
    if not metadata.get("file_path"):
        warnings.warn(f"Skipping [{filename}] because no file path is available.")
        return None

    prepared = {"entry_id": entry_id, "metadata": metadata}
    if use_dummy_descriptions:
        return prepared

    try:
        mime_type = str(metadata.get("mime_type") or "").strip().lower()
        if not mime_type:
            media_type = str(metadata.get("media_type") or "image").lower()
            ext = str(metadata.get("ext") or "").lower()
            mime_type = f"{media_type}/{ext}" if ext else media_type
        image_bytes, image_mime_type = get_analysis_image_bytes(
            metadata["file_path"],
            mime_type=mime_type,
            max_width=analysis_image_max_width,
            max_height=analysis_image_max_height,
        )
    except OSError as exc:
        warnings.warn(f"Skipping [{filename}] because it could not be read: {exc}")
        return None

    prepared["image_bytes"] = image_bytes
    prepared["image_mime_type"] = image_mime_type
    return prepared


def build_batch_request(
    prepared_entries: list[dict],
    *,
    use_dummy_descriptions: bool = False,
) -> dict:
    prompt_sections = batch_prompt_sections(len(prepared_entries))
    batch_request = {
        "entries": prepared_entries,
        "prompt_sections": prompt_sections,
        "response_schema": batch_response_schema(IMAGE_TASK_TYPE),
        "expected_entry_ids": {entry["entry_id"] for entry in prepared_entries},
        "contents": [],
        "request_bytes": 0,
    }
    if use_dummy_descriptions:
        return batch_request

    batch_request["request_bytes"] = (
        len(prompt_sections["admin"].encode("utf-8"))
        + REQUEST_TEXT_OVERHEAD_BYTES
        + len(prompt_sections["prompt"].encode("utf-8"))
        + REQUEST_TEXT_OVERHEAD_BYTES
        + REQUEST_CONTAINER_OVERHEAD_BYTES
    )
    if prepared_entries:
        batch_request["contents"].append(prompt_sections["prompt"])

    for index, entry in enumerate(prepared_entries, start=1):
        item_prompt = render_prompt_template(
            prompt_sections["item"],
            {
                "item_index": str(index),
                "entry_id": entry["entry_id"],
                "metadata_json": json.dumps(entry["metadata"], indent=2),
            },
        )
        image_bytes = entry["image_bytes"]
        image_mime_type = entry["image_mime_type"]
        batch_request["request_bytes"] += (
            len(item_prompt.encode("utf-8"))
            + REQUEST_TEXT_OVERHEAD_BYTES
            + ((len(image_bytes) + 2) // 3) * 4
            + len(image_mime_type.encode("utf-8"))
            + REQUEST_INLINE_PART_OVERHEAD_BYTES
        )
        batch_request["contents"].append(item_prompt)
        batch_request["contents"].append(
            genai.types.Part.from_bytes(data=image_bytes, mime_type=image_mime_type)
        )

    return batch_request


def parse_batch_response(response: Any, expected_entry_ids: set[str]) -> dict[str, dict]:
    payload = getattr(response, "parsed", None)
    if payload is None:
        response_text = normalize_response_json_text(getattr(response, "text", ""))
        try:
            payload = json.loads(response_text)
        except json.JSONDecodeError:
            payload = salvage_partial_batch_payload(response_text)

    items = payload.get("results", payload) if isinstance(payload, dict) else payload
    if isinstance(items, dict):
        items = [
            {"entry_id": entry_id, "description": description}
            for entry_id, description in items.items()
        ]
    if not isinstance(items, list):
        return {}

    descriptions = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        entry_id = item.get("entry_id")
        description = item.get("description")
        if description is None and "content" in item and "context" in item:
            description = {
                "content": item["content"],
                "context": item["context"],
            }
        if entry_id in expected_entry_ids and isinstance(description, dict):
            descriptions.setdefault(entry_id, description)
    return descriptions


def describe_prepared_batch(
    client: genai.Client | None,
    batch_request: dict,
    use_dummy_descriptions: bool = False,
) -> dict[str, dict]:
    valid_entries = batch_request.get("entries") or []
    if not valid_entries:
        return {}

    if use_dummy_descriptions:
        return {
            entry["entry_id"]: dummy_description(entry["entry_id"], entry["metadata"])
            for entry in valid_entries
        }

    if client is None:
        raise ValueError(
            "A Gemini client is required when dummy descriptions are disabled."
        )

    response = client.models.generate_content(
        model=valid_entries[0]["metadata"].get("model_name"),
        contents=batch_request["contents"],
        config=genai.types.GenerateContentConfig(
            system_instruction=batch_request["prompt_sections"]["admin"],
            response_mime_type="application/json",
            response_json_schema=batch_request["response_schema"],
        ),
    )
    return parse_batch_response(response, batch_request["expected_entry_ids"])


def describe_image_batch(
    client: genai.Client | None,
    batch_entries: list[dict],
    use_dummy_descriptions: bool = False,
    analysis_image_max_width: int | None = None,
    analysis_image_max_height: int | None = None,
) -> dict[str, dict]:
    prepared_entries = []
    for entry in batch_entries or []:
        prepared_entry = prepare_batch_entry(
            entry.get("entry_id"),
            entry.get("metadata") or {},
            use_dummy_descriptions=use_dummy_descriptions,
            analysis_image_max_width=analysis_image_max_width,
            analysis_image_max_height=analysis_image_max_height,
        )
        if prepared_entry is not None:
            prepared_entries.append(prepared_entry)

    return describe_prepared_batch(
        client,
        build_batch_request(
            prepared_entries,
            use_dummy_descriptions=use_dummy_descriptions,
        ),
        use_dummy_descriptions=use_dummy_descriptions,
    )
