"""
prompt_parsing.py

Response text normalization and salvage helpers for prompt outputs.
"""

import json


def normalize_response_json_text(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    first_object = text.find("{")
    first_array = text.find("[")
    starts = [index for index in (first_object, first_array) if index >= 0]
    if starts:
        start = min(starts)
        end = max(text.rfind("}"), text.rfind("]"))
        if end >= start:
            text = text[start : end + 1]

    cleaned = []
    in_string = False
    escaped = False
    for char in text:
        if escaped:
            cleaned.append(char)
            escaped = False
            continue
        if char == "\\":
            cleaned.append(char)
            escaped = True
            continue
        if char == '"':
            cleaned.append(char)
            in_string = not in_string
            continue
        if in_string and ord(char) < 32:
            cleaned.append(
                {
                    "\n": "\\n",
                    "\r": "\\r",
                    "\t": "\\t",
                }.get(char, " ")
            )
            continue
        cleaned.append(char)
    return "".join(cleaned)


def salvage_partial_batch_payload(response_text: str) -> dict:
    def iter_closed_objects(text: str, start_index: int):
        index = start_index
        while index < len(text):
            char = text[index]
            if char in " \t\r\n,":
                index += 1
                continue
            if char == "]":
                return
            if char != "{":
                index += 1
                continue

            depth = 0
            in_string = False
            escaped = False
            object_start = index
            while index < len(text):
                current = text[index]
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == '"':
                    in_string = not in_string
                elif not in_string:
                    if current == "{":
                        depth += 1
                    elif current == "}":
                        depth -= 1
                        if depth == 0:
                            yield text[object_start : index + 1]
                            index += 1
                            break
                index += 1
            else:
                return

    results_key = response_text.find('"results"')
    array_start = response_text.find("[", results_key if results_key >= 0 else 0)
    if array_start < 0:
        return {}

    salvaged_items = []
    for object_text in iter_closed_objects(response_text, array_start + 1):
        try:
            item = json.loads(object_text)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            salvaged_items.append(item)

    if salvaged_items:
        print(
            f"Salvaged {len(salvaged_items)} complete item(s) from partial batch response."
        )
    return {"results": salvaged_items}
