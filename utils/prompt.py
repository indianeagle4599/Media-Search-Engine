"""
prompt.py

Contains utilities to create modular and versatile prompts and perform the necessary API calls to get relevant outputs as text or embeddings as relevant.
"""

import os, warnings, json
from typing import Any

from google import genai

from dotenv import load_dotenv
from utils.io import get_analysis_image_bytes

load_dotenv()
REPO_ROOT = os.getenv("REPO_ROOT")


def parse_prompt(prompt_type: str) -> dict:
    prompt_root = os.path.join(REPO_ROOT, "prompts", prompt_type)
    if not os.path.isdir(prompt_root):
        return {}

    parsed_prompts = {}
    for section_file in os.listdir(prompt_root):
        section = os.path.splitext(section_file)[0]
        section_path = os.path.join(prompt_root, section_file)
        try:
            with open(section_path, "r") as file:
                parsed_prompts[section] = file.read()
        except FileNotFoundError:
            print(
                f'Found nothing at "{section_path}" for prompt section named: {section}.'
            )
    return parsed_prompts


def render_prompt_template(template: str, attachments_dict: dict) -> str:
    for attachment, value in attachments_dict.items():
        template = template.replace(f"__{attachment}__", value)
    return template


def assemble_prompt(prompt_type: str, attachments_dict: dict) -> dict:
    assembled_prompts = {}
    for section, section_prompt in parse_prompt(prompt_type).items():
        assembled_prompts[section] = render_prompt_template(
            section_prompt,
            attachments_dict,
        )
    return assembled_prompts


def mime_type_for_metadata(metadata: dict) -> str:
    mime_type = str(metadata.get("mime_type") or "").strip().lower()
    if mime_type:
        return mime_type

    media_type = str(metadata.get("media_type") or "image").lower()
    ext = str(metadata.get("ext") or "").lower()
    return f"{media_type}/{ext}" if ext else media_type


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


def prepare_batch_entries(
    batch_entries: list[dict],
    use_dummy_descriptions: bool,
    analysis_image_max_width: int | None = None,
    analysis_image_max_height: int | None = None,
) -> list[dict]:
    valid_entries = []
    for batch_entry in batch_entries:
        entry_id = str(batch_entry.get("entry_id") or "")
        metadata = batch_entry.get("metadata") or {}
        filename = metadata.get("file_name") or entry_id
        if (
            metadata.get("media_type") != "image"
            or not metadata.get("is_compat")
            or not metadata.get("file_path")
        ):
            warnings.warn(
                f"Skipping [{filename}] as '{metadata.get('media_type')}/{metadata.get('ext')}' is not supported."
            )
            continue

        prepared = {"entry_id": entry_id, "metadata": metadata}
        if not use_dummy_descriptions:
            try:
                image_bytes, image_mime_type = get_analysis_image_bytes(
                    metadata["file_path"],
                    mime_type=mime_type_for_metadata(metadata),
                    max_width=analysis_image_max_width,
                    max_height=analysis_image_max_height,
                )
                prepared["image_bytes"] = image_bytes
                prepared["image_mime_type"] = image_mime_type
            except OSError as exc:
                warnings.warn(
                    f"Skipping [{filename}] because it could not be read: {exc}"
                )
                continue
        valid_entries.append(prepared)
    return valid_entries


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


def describe_image_batch(
    client: genai.Client | None,
    batch_entries: list[dict],
    use_dummy_descriptions: bool = False,
    analysis_image_max_width: int | None = None,
    analysis_image_max_height: int | None = None,
) -> dict[str, dict]:
    valid_entries = prepare_batch_entries(
        batch_entries,
        use_dummy_descriptions,
        analysis_image_max_width=analysis_image_max_width,
        analysis_image_max_height=analysis_image_max_height,
    )
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

    prompt_sections = assemble_prompt(
        "describe_image_batch",
        {
            "batch_size": str(len(valid_entries)),
        },
    )
    contents = [prompt_sections["prompt"]]
    item_template = prompt_sections["item"]
    for index, entry in enumerate(valid_entries, start=1):
        contents.append(
            render_prompt_template(
                item_template,
                {
                    "item_index": str(index),
                    "entry_id": entry["entry_id"],
                    "metadata_json": json.dumps(entry["metadata"], indent=2),
                },
            )
        )
        contents.append(
            genai.types.Part.from_bytes(
                data=entry["image_bytes"],
                mime_type=entry["image_mime_type"],
            )
        )

    response = client.models.generate_content(
        model=valid_entries[0]["metadata"].get("model_name"),
        contents=contents,
        config=genai.types.GenerateContentConfig(
            system_instruction=prompt_sections["admin"],
            response_mime_type="application/json",
        ),
    )
    return parse_batch_response(
        response,
        {entry["entry_id"] for entry in valid_entries},
    )
