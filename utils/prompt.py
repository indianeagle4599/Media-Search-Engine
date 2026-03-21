"""
prompt.py

Contains utilities to create modular and versatile prompts and perform the necessary API calls to get relevant outputs as text or embeddings as relevant.
"""

import os, warnings, json
from typing import Literal
from google import genai

from dotenv import load_dotenv

load_dotenv()
REPO_ROOT = os.getenv("REPO_ROOT")


def parse_prompt(prompt_type: Literal["describe_image",]):
    path_dict, parsed_prompts = {}, {}
    if prompt_type == "describe_image":
        prompt_root = os.path.join(REPO_ROOT, "prompts", prompt_type)
        section_files = os.listdir(prompt_root)
        for section_file in section_files:
            section = os.path.splitext(section_file)[0]
            section_path = os.path.join(prompt_root, section_file)
            path_dict[section] = section_path

    if len(path_dict):
        # path_dict = {"admin": "path/to/admin.md", "prompt": "path/to/prompt.md"}
        for section, section_path in path_dict.items():
            section_prompt = ""
            try:
                with open(section_path, "r") as f:
                    section_prompt = "".join(f.readlines())
            except FileNotFoundError:
                print(
                    f'Found nothing at "{section_path}" for prompt section named: {section}.'
                )
            parsed_prompts[section] = section_prompt
    return parsed_prompts


def assemble_prompt(prompt_type: Literal["describe_image",], attachments_dict: dict):
    parsed_prompts = parse_prompt(prompt_type)
    assembled_prompts = {}
    for section, section_prompt in parsed_prompts.items():
        # Add attachments
        for attachments in attachments_dict:
            find_attachements = f"__{attachments}__"
            if find_attachements in section_prompt:
                section_prompt = section_prompt.replace(
                    find_attachements, attachments_dict[attachments]
                )
        assembled_prompts[section] = section_prompt
    return assembled_prompts


def describe_image(client: genai.Client, metadata: dict):
    file_path = metadata.get("file_path", "")
    filename = metadata.get("file_name", "")
    is_compat = metadata.get("is_compat", False)
    media_type = metadata.get("media_type")
    ext = metadata.get("ext")
    model_name = metadata.get("model_name")

    if is_compat and file_path:
        if media_type == "image":
            prompt_type = "describe_image"
            attachments = {"metadata_dict": json.dumps(metadata, indent=2)}
            prompt_sections = assemble_prompt(prompt_type, attachments)

            with open(file_path, "rb") as f:
                image_bytes = f.read()

            response = client.models.generate_content(
                model=model_name,
                contents=[
                    prompt_sections["prompt"],
                    genai.types.Part.from_bytes(
                        data=image_bytes,
                        mime_type=f"{media_type}/{ext}",
                    ),
                ],
                config=genai.types.GenerateContentConfig(
                    system_instruction=prompt_sections["admin"],
                    response_mime_type="application/json",
                ),
            )
            output = json.loads(response.text)
            return output
        else:
            print("Not image")
    else:
        warnings.warn(
            f"Skipping [{filename}] as '{media_type}/{ext}' is not supported."
        )
