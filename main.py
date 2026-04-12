"""
main.py

Force-sync the media folder into MongoDB, ChromaDB, and a retrieval smoke test.
"""

import json

from dotenv import load_dotenv

from utils.chroma import get_chroma_client, populate_db, query_collections
from utils.ingest import (
    build_ingest_config_from_env,
    has_chroma_index,
    has_description,
    ingest_folder,
    mark_chroma_indexed,
    populate_missing,
)
from utils.mongo import get_mongo_collection
from utils.retrieval import SearchManifest, normalize_search_options


DEFAULT_IMAGES_ROOT = "images_root_test"

FORCE_SYNC_CONFIG = {
    "root_path": DEFAULT_IMAGES_ROOT,
    "verbose": False,
    "test_queries": [],
    "test_query_count": 3,
    "test_top_n": 3,
    "search_options": {
        "preset": SearchManifest.DEFAULT_PRESET,
        "focus": dict(SearchManifest.DEFAULT_FOCUS),
        "enabled_sources": [],
        "disabled_sources": [],
        "enabled_search_types": list(SearchManifest.SEARCH_TYPES),
        "capabilities": [],
    },
    "include_debug": False,
    "trace": False,
}


def build_ingest_config(*, verbose: bool = False):
    return build_ingest_config_from_env(
        mongo_collection=get_mongo_collection(),
        chroma_client=get_chroma_client(),
        update_existing_metadata=True,
        verbose=bool(verbose),
    )


def load_mongo_entries(collection) -> dict[str, dict]:
    entries = {}
    for document in collection.find({}):
        item = dict(document)
        entry_id = str(item.pop("_id"))
        entries[entry_id] = item
    return entries


def derive_test_queries(entries: dict[str, dict], limit: int) -> list[str]:
    queries = []
    seen = set()

    for entry in entries.values():
        description = entry.get("description") or {}
        content = description.get("content") or {}
        context = description.get("context") or {}
        candidates = [
            content.get("summary"),
            context.get("event"),
            content.get("text"),
            context.get("primary_category"),
            context.get("intent"),
        ]
        for candidate in candidates:
            text = " ".join(str(candidate or "").split())
            if not text:
                continue
            text = " ".join(text.split()[:8])
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            queries.append(text)
            if len(queries) >= limit:
                return queries

    return queries


def main(run_config: dict | None = None) -> None:
    load_dotenv()
    run_config = run_config or FORCE_SYNC_CONFIG
    config = build_ingest_config(verbose=bool(run_config.get("verbose")))
    root_path = str(run_config.get("root_path") or DEFAULT_IMAGES_ROOT)

    print(
        f"[1/4] Scanning folder and running the initial Mongo/Chroma sync: {root_path}"
    )
    ingest_result = ingest_folder(root_path=root_path, config=config)
    print(f"Files discovered: {len(ingest_result.folder_dict)}")
    print(
        f"Entries missing descriptions before initial pass: {len(ingest_result.missing_keys)}"
    )
    print(
        f"Descriptions created during initial pass: {len(ingest_result.populated_keys)}"
    )
    print(
        f"Entries sent to Chroma during initial pass: {len(ingest_result.chroma_indexed_keys)}"
    )
    if ingest_result.failed_keys:
        print(
            f"Description failures during initial pass: {len(ingest_result.failed_keys)}"
        )
    if ingest_result.rate_limited_keys:
        print(
            f"Rate-limited descriptions during initial pass: {len(ingest_result.rate_limited_keys)}"
        )
    for step, seconds in ingest_result.timings.items():
        print(f"{step}: {seconds:.2f}s")

    print("[2/4] Reloading MongoDB and backfilling remaining descriptions")
    mongo_entries = load_mongo_entries(config.mongo_collection)
    missing_description_ids = [
        entry_id
        for entry_id, entry in mongo_entries.items()
        if not has_description(entry)
    ]
    print(f"Mongo entries loaded: {len(mongo_entries)}")
    print(
        f"Entries still missing descriptions before backfill: {len(missing_description_ids)}"
    )
    (
        mongo_entries,
        populated_keys,
        failed_keys,
        rate_limited_keys,
        error_details,
    ) = populate_missing(
        descriptions=mongo_entries,
        missing_keys=missing_description_ids,
        config=config,
    )
    remaining_missing_description_ids = [
        entry_id
        for entry_id, entry in mongo_entries.items()
        if not has_description(entry)
    ]
    print(f"Descriptions created during Mongo backfill: {len(populated_keys)}")
    print(
        f"Entries still missing descriptions after backfill: {len(remaining_missing_description_ids)}"
    )
    if failed_keys:
        print(f"Description failures during Mongo backfill: {len(failed_keys)}")
    if rate_limited_keys:
        print(
            f"Rate-limited descriptions during Mongo backfill: {len(rate_limited_keys)}"
        )
    if error_details:
        print(f"Description error records written: {len(error_details)}")

    print("[3/4] Reloading MongoDB and backfilling remaining Chroma entries")
    mongo_entries = load_mongo_entries(config.mongo_collection)
    chroma_entries = {
        entry_id: entry
        for entry_id, entry in mongo_entries.items()
        if has_description(entry) and not has_chroma_index(entry)
    }
    described_entry_count = sum(
        1 for entry in mongo_entries.values() if has_description(entry)
    )
    print(f"Mongo entries with descriptions: {described_entry_count}")
    print(f"Entries still missing Chroma before backfill: {len(chroma_entries)}")
    if chroma_entries:
        populate_db(
            entries=chroma_entries,
            chroma_client=config.chroma_client,
            overwrite=False,
            verbose=config.verbose,
        )
        indexed_at = mark_chroma_indexed(list(chroma_entries), config)
        print(f"Entries written to Chroma during backfill: {len(chroma_entries)}")
        if indexed_at:
            print(f"Chroma indexed at: {indexed_at}")
    else:
        print("No described Mongo entries were missing Chroma records")
    mongo_entries = load_mongo_entries(config.mongo_collection)
    remaining_missing_chroma_ids = [
        entry_id
        for entry_id, entry in mongo_entries.items()
        if has_description(entry) and not has_chroma_index(entry)
    ]
    print(
        f"Entries still missing Chroma after backfill: {len(remaining_missing_chroma_ids)}"
    )

    print("[4/4] Running retrieval smoke test batch")
    search_options = normalize_search_options(run_config.get("search_options"))
    test_queries = [
        str(query).strip()
        for query in (run_config.get("test_queries") or [])
        if str(query).strip()
    ]
    if not test_queries:
        test_queries = derive_test_queries(
            mongo_entries,
            limit=max(1, int(run_config.get("test_query_count", 3) or 3)),
        )

    if not test_queries:
        print("Skipped retrieval smoke test: no query candidates were available")
        return

    smoke_result = query_collections(
        chroma_client=config.chroma_client,
        query_texts=test_queries,
        n_results=max(1, int(run_config.get("test_top_n", 3) or 3)),
        search_options=search_options,
        include_debug=bool(run_config.get("include_debug")),
        trace=bool(run_config.get("trace")),
    )
    summary = {}
    for query_text in test_queries:
        result = smoke_result.get(query_text) or {}
        ids = result.get("ids") or []
        summary[query_text] = {
            "count": len(ids),
            "top_ids": ids[:3],
        }
    if not any(item["count"] for item in summary.values()):
        raise RuntimeError(
            "Retrieval smoke test completed but returned no results for the test batch."
        )
    print(f"Smoke test queries: {len(test_queries)}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
