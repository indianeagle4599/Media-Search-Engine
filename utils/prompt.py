"""
prompt.py

Contains utilities to create modular and versatile prompts and perform the necessary API calls to get relevant outputs as text or embeddings as relevant.
"""

import os, warnings, json
from functools import lru_cache
from typing import Any

from google import genai

from dotenv import load_dotenv
from utils.io import get_analysis_image_bytes

load_dotenv()
REPO_ROOT = os.getenv("REPO_ROOT")
REQUEST_TEXT_OVERHEAD_BYTES = 64
REQUEST_INLINE_PART_OVERHEAD_BYTES = 128
REQUEST_CONTAINER_OVERHEAD_BYTES = 512


def render_prompt_template(template: str, attachments_dict: dict) -> str:
    for attachment, value in attachments_dict.items():
        template = template.replace(f"__{attachment}__", value)
    return template


@lru_cache(maxsize=16)
def batch_prompt_sections(batch_size: int) -> dict:
    prompt_root = os.path.join(REPO_ROOT, "prompts", "describe_image_batch")
    prompt_sections = {}
    for section in ("admin", "prompt", "item"):
        section_path = os.path.join(prompt_root, f"{section}.md")
        try:
            with open(section_path, "r") as file:
                prompt_sections[section] = render_prompt_template(
                    file.read(),
                    {"batch_size": str(batch_size)},
                )
        except FileNotFoundError:
            print(
                f'Found nothing at "{section_path}" for prompt section named: {section}.'
            )
    return prompt_sections

def dummy_description(entry_id: str, metadata: dict) -> dict:
    filename = metadata.get("file_name") or entry_id
    return {
        "content": {
            "summary": f"Dummy batch description for {filename}",
            "objects": [],
            "text": "",
            "vibe": [],
            "background": "",
            "detailed_description": "Generated locally to validate batch processing.",
            "miscellaneous": "",
        },
        "context": {
            "primary_category": "",
            "intent": "",
            "composition": "",
            "estimated_date": "",
            "event": "Dummy validation run",
            "analysis": "No Gemini request was made.",
            "metadata_relevance": "",
            "other_details": "",
        },
    }


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
    if (
        metadata.get("media_type") != "image"
        or not metadata.get("is_compat")
        or not metadata.get("file_path")
    ):
        warnings.warn(
            f"Skipping [{filename}] as '{metadata.get('media_type')}/{metadata.get('ext')}' is not supported."
        )
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
        "expected_entry_ids": {entry["entry_id"] for entry in prepared_entries},
        "contents": [],
        "request_bytes": 0,
    }
    if use_dummy_descriptions:
        return batch_request

    batch_request["request_bytes"] = (
        len(str(prompt_sections.get("admin", "")).encode("utf-8"))
        + REQUEST_TEXT_OVERHEAD_BYTES
        + len(str(prompt_sections.get("prompt", "")).encode("utf-8"))
        + REQUEST_TEXT_OVERHEAD_BYTES
        + REQUEST_CONTAINER_OVERHEAD_BYTES
    )
    if prepared_entries:
        batch_request["contents"].append(prompt_sections["prompt"])

    item_template = prompt_sections.get("item", "")
    for index, entry in enumerate(prepared_entries, start=1):
        item_prompt = render_prompt_template(
            item_template,
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
            genai.types.Part.from_bytes(
                data=image_bytes,
                mime_type=image_mime_type,
            )
        )

    return batch_request


def parse_batch_response(
    response: Any, expected_entry_ids: set[str]
) -> dict[str, dict]:
    payload = getattr(response, "parsed", None)
    if payload is None:
        payload = json.loads(getattr(response, "text"))

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

    prompt_sections = batch_request["prompt_sections"]
    response = client.models.generate_content(
        model=valid_entries[0]["metadata"].get("model_name"),
        contents=batch_request["contents"],
        config=genai.types.GenerateContentConfig(
            system_instruction=prompt_sections["admin"],
            response_mime_type="application/json",
        ),
    )
    return parse_batch_response(
        response,
        batch_request["expected_entry_ids"],
    )
