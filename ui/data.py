"""Data access helpers for the Streamlit UI."""

import os

import streamlit as st


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@st.cache_resource(show_spinner=False)
def get_mongo_collection():
    import pymongo

    client = pymongo.MongoClient(get_required_env("MONGO_URL"))
    return client[get_required_env("MONGO_DB_NAME")][
        get_required_env("MONGO_COLLECTION_NAME")
    ]


@st.cache_resource(show_spinner=False)
def get_chroma_client():
    from utils.chroma import get_chroma_client as create_chroma_client

    return create_chroma_client(path=os.getenv("CHROMA_URL"))


def get_query_results(query: str, top_n: int) -> tuple[list[str], dict[str, list]]:
    from utils.chroma import query_all_collections

    normalized_query = query.strip().lower()
    ranked_queries = query_all_collections(
        chroma_client=get_chroma_client(),
        query_texts=[normalized_query],
        n_results=top_n,
    )
    result = ranked_queries.get(normalized_query) or {}
    return result.get("ids", []), result


def get_entries(entry_ids: list[str]) -> dict:
    if not entry_ids:
        return {}

    from utils.mongo import find_dict_objects

    return find_dict_objects(entry_ids, get_mongo_collection())
