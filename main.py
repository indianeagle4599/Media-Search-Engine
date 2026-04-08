"""CLI entrypoint for ingesting the media library into MongoDB and ChromaDB."""

import os

import pymongo
from dotenv import load_dotenv
from google import genai

from utils.chroma import get_chroma_client
from utils.ingest import IngestConfig, ingest_folder


DEFAULT_API_NAME = "gemini"
DEFAULT_MODEL_NAME = "gemini-2.5-flash-lite"
DEFAULT_IMAGES_ROOT = "images_root"


def build_ingest_config() -> IngestConfig:
    return IngestConfig(
        api_name=os.getenv("MEDIA_API_NAME", DEFAULT_API_NAME),
        model_name=os.getenv("MEDIA_MODEL_NAME", DEFAULT_MODEL_NAME),
        mongo_collection=pymongo.MongoClient(os.getenv("MONGO_URL"))[
            os.getenv("MONGO_DB_NAME")
        ][os.getenv("MONGO_COLLECTION_NAME")],
        chroma_client=get_chroma_client(path=os.getenv("CHROMA_URL")),
        genai_client=genai.Client(api_key=os.getenv("GEM_API_KEY")),
        update_existing_metadata=True,
        verbose=True,
    )


def main() -> None:
    load_dotenv()
    root_path = os.getenv("MEDIA_INDEX_ROOT", DEFAULT_IMAGES_ROOT)
    result = ingest_folder(root_path=root_path, config=build_ingest_config())

    print(f"Indexed files: {len(result.folder_dict)}")
    print(f"Missing descriptions before ingest: {len(result.missing_keys)}")
    print(f"New descriptions: {len(result.populated_keys)}")
    print(f"Chroma entries attempted: {len(result.chroma_indexed_keys)}")
    for step, seconds in result.timings.items():
        print(f"{step}: {seconds:.2f}s")


if __name__ == "__main__":
    main()
