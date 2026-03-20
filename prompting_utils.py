"""
prompting_utils.py

Contains utilities to create modular and versatile prompts and perform the necessary API calls to get relevant outputs as text or embeddings as relevant.
"""

import os, warnings, json, hashlib, pymongo
from typing import Literal

from mongo_utils import find_dict_objects, upsert_dict_objects
from dotenv import load_dotenv

load_dotenv()
REPO_ROOT = os.getenv("REPO_ROOT")
GEM_API_KEY = os.getenv("GEM_API_KEY")
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

API_NAME = "gemini"
MODEL_NAME = "gemini-2.5-flash-lite"

from google import genai
from google.genai import types

client = genai.Client(api_key=GEM_API_KEY)
collection = pymongo.MongoClient(MONGO_URL)[MONGO_DB_NAME][MONGO_COLLECTION_NAME]


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


def describe_image(metadata: dict):
    file_path = metadata.get("file_path", "")
    filename = metadata.get("file_name", "")
    is_compat = metadata.get("is_compat", False)
    media_type = metadata.get("media_type")
    ext = metadata.get("ext")

    if is_compat and file_path:
        if media_type == "image":
            prompt_type = "describe_image"
            attachments = {"metadata_dict": json.dumps(metadata, indent=2)}
            prompt_sections = assemble_prompt(prompt_type, attachments)

            with open(file_path, "rb") as f:
                image_bytes = f.read()

            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=[
                    prompt_sections["prompt"],
                    types.Part.from_bytes(
                        data=image_bytes,
                        mime_type=f"{media_type}/{ext}",
                    ),
                ],
                config=types.GenerateContentConfig(
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


def check_if_exists(
    keys_dict: dict,
    collection: pymongo.synchronous.collection.Collection,
):
    search_keys = keys_dict.keys()

    found_objects = find_dict_objects(list(search_keys), collection)
    found_keys = found_objects.keys()

    missing_keys = set(search_keys).difference(set(found_keys))

    return found_objects, list(missing_keys)


if __name__ == "__main__":
    from indexing_utils import index_folder

    images_root = "images_root"

    folder_dict = index_folder(images_root)
    outputs = {}

    for file_hash in folder_dict:
        metadata = folder_dict[file_hash]
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
        outputs[entry_hash] = {"description": {}, "metadata": metadata}
    found_objects, missing_keys = check_if_exists(outputs, collection)
    outputs.update(found_objects)

    missing_outputs = {}
    for missing_key in missing_keys:
        metadata = outputs[missing_key]["metadata"]
        try:
            output = describe_image(metadata)
            if output:
                new_object = {"description": output, "metadata": metadata}
                missing_outputs[missing_key] = new_object
        except genai.errors.APIError as e:
            if str(e.code) == "429":
                warnings.warn("Quota reached! Stopping image analysis.")
                break
            print(e)
        except Exception as e:
            print(e)

        if len(missing_outputs) >= 128:
            upsert_dict_objects(missing_outputs, collection)
            outputs.update(missing_outputs)
            missing_outputs = {}
    upsert_dict_objects(missing_outputs, collection)
    outputs.update(missing_outputs)

    print(json.dumps(outputs, indent=2))
