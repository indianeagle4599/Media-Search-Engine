## DATA FORMAT

Return one result per `entry_id` using the following JSON schema:
```json
{
    "results": [
        {
            "entry_id": "exact manifest entry_id",
            "description": "<single media description schema shown above>"
        }
    ]
}
```

## BATCH RULES

1. Each text item block applies only to the next media item.
2. Every item must produce exactly one `results` entry with the same `entry_id`.
3. Do not omit, merge, reorder, or invent `entry_id` values.
4. Use the metadata from an item block only for the media item immediately following that block.
5. Keep each item's reasoning isolated. Never let one media item influence another item's description.
6. Return `results` in the same order as the input items.
