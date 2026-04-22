"""
prompt_assembly.py

Manifest-driven prompt assembly helpers for supported media prompts.
"""

import copy, json, os
from functools import lru_cache
from pathlib import Path
from typing import Any


REPO_ROOT = Path(os.getenv("REPO_ROOT") or Path(__file__).resolve().parents[1])
PROMPTS_ROOT = REPO_ROOT / "prompts"
COMMON_PROMPTS_ROOT = PROMPTS_ROOT / "common"
BATCH_PROMPTS_ROOT = PROMPTS_ROOT / "batch"


def render_prompt_template(template: str, values: dict[str, str]) -> str:
    for key, value in values.items():
        template = template.replace(f"__{key}__", value)
    return template


def render_json_templates(value: Any, values: dict[str, str]) -> Any:
    if isinstance(value, str):
        return render_prompt_template(value, values)
    if isinstance(value, list):
        return [render_json_templates(item, values) for item in value]
    if isinstance(value, dict):
        return {
            key: render_json_templates(item, values) for key, item in value.items()
        }
    return value


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_parts(root: Path, parts: list[str], values: dict[str, str] | None = None) -> str:
    values = values or {}
    rendered_parts = []
    for part in parts:
        text = (root / part).resolve().read_text(encoding="utf-8") if part.endswith(".md") else part
        rendered_parts.append(render_prompt_template(text, values))
    return "\n\n".join(part for part in rendered_parts if part)


@lru_cache(maxsize=8)
def load_single_prompt_manifest(task_type: str) -> tuple[Path, dict]:
    prompt_root = PROMPTS_ROOT / task_type
    return prompt_root, load_json(prompt_root / "manifest.json")


@lru_cache(maxsize=1)
def load_batch_prompt_manifest() -> tuple[Path, dict]:
    return BATCH_PROMPTS_ROOT, load_json(BATCH_PROMPTS_ROOT / "manifest.json")


def assemble_admin_text(task_type: str, task_mode: str) -> str:
    single_root, single_manifest = load_single_prompt_manifest(task_type)
    sections = [load_parts(COMMON_PROMPTS_ROOT, ["intro.md"])]
    if task_mode == "single":
        sections.extend(
            [
                load_parts(single_root, single_manifest["admin"]["identity"]),
                load_parts(single_root, single_manifest["admin"]["instructions"]),
                load_parts(single_root, single_manifest["admin"]["schema"]),
                load_parts(single_root, single_manifest["admin"]["output"]),
            ]
        )
    else:
        batch_root, batch_manifest = load_batch_prompt_manifest()
        sections.extend(
            [
                load_parts(batch_root, batch_manifest["admin"]["identity"]),
                load_parts(single_root, single_manifest["admin"]["instructions"]),
                load_parts(single_root, single_manifest["admin"]["schema"]),
                load_parts(batch_root, batch_manifest["admin"]["output"]),
            ]
        )
    sections.extend(
        [
            load_parts(COMMON_PROMPTS_ROOT, ["output.md"]),
            load_parts(COMMON_PROMPTS_ROOT, ["guardrails.md"]),
        ]
    )
    return "\n\n".join(section for section in sections if section)


@lru_cache(maxsize=16)
def batch_prompt_sections(task_type: str, batch_size: int) -> dict[str, str]:
    batch_root, batch_manifest = load_batch_prompt_manifest()
    return {
        "admin": assemble_admin_text(task_type, "batch"),
        "prompt": load_parts(
            batch_root,
            batch_manifest["prompt"],
            {"batch_size": str(batch_size)},
        ),
        "item": load_parts(batch_root, batch_manifest["item"]),
    }


@lru_cache(maxsize=8)
def single_response_schema(task_type: str) -> dict:
    single_root, single_manifest = load_single_prompt_manifest(task_type)
    return load_json(single_root / single_manifest["response_schema"])


def batch_response_schema(task_type: str) -> dict:
    return {
        "type": "object",
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["entry_id", "description"],
                    "properties": {
                        "entry_id": {"type": "string"},
                        "description": copy.deepcopy(single_response_schema(task_type)),
                    },
                },
            }
        },
    }


def dummy_response(task_type: str, entry_id: str, metadata: dict) -> dict:
    single_root, single_manifest = load_single_prompt_manifest(task_type)
    filename = metadata.get("file_name") or entry_id
    return render_json_templates(
        load_json(single_root / single_manifest["dummy_response"]),
        {"filename": filename},
    )
