"""
corpus_builder.py

Stages and approves a small eval image corpus for the existing MongoDB and ChromaDB flow.
"""

import hashlib, json, os, random, re, shutil, time
from pathlib import Path
from urllib import (
    error as urllib_error,
    parse as urllib_parse,
    request as urllib_request,
)

import cv2
from dotenv import load_dotenv
from PIL import Image, ImageOps

from utils.io import (
    IM_TYPES,
    ensure_heif_registered,
    get_ext,
    get_mime_type,
    index_paths,
)


load_dotenv()

COMMONS_API_URL = "https://commons.wikimedia.org/w/api.php"
NASA_IMAGES_API_URL = "https://images-api.nasa.gov"
PEXELS_API_URL = "https://api.pexels.com/v1/search"
PIXABAY_API_URL = "https://pixabay.com/api/"
HEADERS = {"User-Agent": "MediaSearchCorpusBuilder/1.0"}
HTTP_RETRY_DELAYS = (1.5, 3.0, 6.0)
MAX_PROVIDER_MISSES = 12
REVIEW_WINDOW_NAME = "Corpus Approval"
REVIEW_MAX_WIDTH = 1600
REVIEW_MAX_HEIGHT = 1000
SUPPORTED_EXTS = {ext for ext in IM_TYPES if ext != "svg"}
MIME_EXTS = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/heic": "heic",
    "image/heif": "heif",
    "image/tiff": "tif",
    "image/bmp": "bmp",
    "image/avif": "avif",
}
METADATA_LEVELS = {"unknown": 0, "sparse": 1, "medium": 2, "rich": 3}
OCR_HINTS = {
    "book",
    "caption",
    "document",
    "form",
    "invoice",
    "label",
    "menu",
    "page",
    "paper",
    "poster",
    "receipt",
    "screen",
    "screenshot",
    "sign",
    "slide",
    "text",
    "ticket",
}

DEFAULT_BUCKET_TARGETS = {
    "people_activity": {
        "target": 30,
        "providers": ["pixabay", "pexels", "wikimedia", "nasa", "local_import"],
        "fit": "people person portrait group crowd activity event dance meeting family street",
        "ocr_expected": False,
        "metadata_richness": "medium",
        "pixabay": ["group photo", "people activity", "crowd event"],
        "pexels": ["group of people", "people activity", "family gathering"],
        "wikimedia": [
            ("search", "people activity photograph"),
            ("search", "group portrait"),
            ("category", "People by occupation"),
        ],
        "nasa": ["astronaut training", "scientists working", "crew portrait"],
    },
    "nature_landscape": {
        "target": 30,
        "providers": ["pixabay", "pexels", "wikimedia", "nasa", "local_import"],
        "fit": "nature landscape mountain forest sunset river beach sky lake tree outdoor",
        "ocr_expected": False,
        "metadata_richness": "medium",
        "pixabay": ["nature landscape", "mountain sunset", "forest trail"],
        "pexels": ["nature landscape", "mountain scenery", "forest trail"],
        "wikimedia": [
            ("search", "nature landscape photograph"),
            ("search", "mountain scenery"),
            ("category", "Landscapes"),
        ],
        "nasa": ["earth landscape", "earth from orbit", "mars surface"],
    },
    "objects_closeup": {
        "target": 30,
        "providers": ["pixabay", "pexels", "wikimedia", "nasa", "local_import"],
        "fit": "object tool product closeup macro device container food flower item detail",
        "ocr_expected": False,
        "metadata_richness": "medium",
        "pixabay": ["close up object", "macro product", "tool closeup"],
        "pexels": ["object close up", "macro object", "tool close up"],
        "wikimedia": [
            ("search", "close up object photo"),
            ("search", "macro photography"),
            ("category", "Close-up photographs"),
        ],
        "nasa": ["space tool closeup", "equipment closeup", "instrument detail"],
    },
    "documents_ocr": {
        "target": 30,
        "providers": ["pixabay", "pexels", "wikimedia", "local_import"],
        "fit": "document text page receipt invoice menu form book paper screen screenshot",
        "ocr_expected": True,
        "metadata_richness": "medium",
        "pixabay": ["document scan", "receipt paper", "computer screenshot"],
        "pexels": ["document paper", "receipt", "computer screen"],
        "wikimedia": [
            ("search", "scanned document"),
            ("search", "book page photograph"),
            ("category", "Documents"),
        ],
    },
    "urban_indoor": {
        "target": 30,
        "providers": ["pixabay", "pexels", "wikimedia", "local_import"],
        "fit": "city urban street building room interior office store restaurant indoor architecture",
        "ocr_expected": False,
        "metadata_richness": "medium",
        "pixabay": ["urban street", "office interior", "indoor cafe"],
        "pexels": ["urban street", "office interior", "indoor cafe"],
        "wikimedia": [
            ("search", "urban street photography"),
            ("search", "indoor architecture"),
            ("category", "Streets in cities"),
        ],
    },
    "date_metadata": {
        "target": 30,
        "providers": ["wikimedia", "pixabay", "pexels", "nasa", "local_import"],
        "fit": "dated timestamp calendar clock archive historic event newspaper poster sign metadata",
        "ocr_expected": True,
        "metadata_richness": "rich",
        "pixabay": ["calendar page", "clock face", "newspaper photo"],
        "pexels": ["calendar page", "clock face", "newspaper"],
        "wikimedia": [
            ("search", "dated photograph"),
            ("search", "historic newspaper"),
            ("category", "Images with Exif metadata"),
        ],
        "nasa": ["historic nasa photograph", "mission patch", "space mission archive"],
    },
}


def clean(value):
    return " ".join(str(value or "").split()).strip()


def tokens(value):
    return re.findall(r"[a-z0-9]+", clean(value).lower())


def split_tags(value):
    parts = (
        value if isinstance(value, list) else re.split(r"[,|;/\n]+", str(value or ""))
    )
    out, seen = [], set()
    for part in parts:
        tag = clean(part)
        if tag and tag.lower() not in seen:
            seen.add(tag.lower())
            out.append(tag)
    return out


def strip_html(value):
    return clean(re.sub(r"<[^>]+>", " ", str(value or "")))


def paths(corpus_root):
    root = Path(corpus_root).resolve()
    return {
        "root": root,
        "incoming": root / "incoming",
        "approved": root / "approved",
        "manifests": root / "manifests",
        "staging_manifest": root / "manifests" / "staging_manifest.json",
        "corpus_manifest": root / "manifests" / "corpus_manifest.json",
    }


def ensure_layout(layout):
    for key in ("incoming", "approved", "manifests"):
        layout[key].mkdir(parents=True, exist_ok=True)


def read_manifest(path):
    if not path.exists():
        return {"assets": [], "rejections": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("assets", [])
    payload.setdefault("rejections", [])
    return payload


def write_manifest(path, manifest):
    manifest["assets"] = sorted(
        manifest.get("assets", []),
        key=lambda item: (item.get("bucket", ""), item.get("asset_id", "")),
    )
    manifest["rejections"] = sorted(
        manifest.get("rejections", []),
        key=lambda item: (
            item.get("bucket", ""),
            item.get("asset_id", ""),
            item.get("reason", ""),
        ),
    )
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")


def request_url(url, parse_json=False, headers=None):
    request = urllib_request.Request(url, headers=headers or HEADERS, method="GET")
    for attempt, delay in enumerate((0.0, *HTTP_RETRY_DELAYS), start=1):
        try:
            with urllib_request.urlopen(request) as response:
                data = response.read()
                if parse_json:
                    return json.loads(data.decode("utf-8"))
                return data, response.headers.get_content_type() or ""
        except urllib_error.HTTPError as exc:
            if exc.code != 429 or attempt > len(HTTP_RETRY_DELAYS):
                if exc.code == 429:
                    kind = "JSON" if parse_json else "file"
                    raise RuntimeError(
                        f"rate limited while fetching {kind}: {url}"
                    ) from exc
                raise
            time.sleep(delay)


def build_asset_id(provider, provider_asset_id, source_url, query_label):
    return hashlib.sha1(
        "|".join(
            [
                clean(provider),
                clean(provider_asset_id),
                clean(source_url),
                clean(query_label),
            ]
        ).encode("utf-8")
    ).hexdigest()


def average_hash(file_path):
    ensure_heif_registered(file_path)
    with Image.open(file_path) as image:
        image = ImageOps.exif_transpose(image).convert("L").resize((8, 8))
        pixels = list(
            image.get_flattened_data()
            if hasattr(image, "get_flattened_data")
            else image.getdata()
        )
    avg = sum(pixels) / max(1, len(pixels))
    return f"{int(''.join('1' if pixel >= avg else '0' for pixel in pixels), 2):016x}"


def metadata_richness(file_metadata, provider_level):
    score = len(
        [
            1
            for value in (file_metadata.get("embedded_metadata") or {}).values()
            if value not in ("", None, [], {})
        ]
    )
    dates = file_metadata.get("dates") or {}
    if dates.get("master_date"):
        score += 2
    if dates.get("date_reliability") == "high":
        score += 2
    elif dates.get("date_reliability") == "medium":
        score += 1
    local_level = (
        "rich"
        if score >= 10
        else "medium" if score >= 4 else "sparse" if score >= 1 else "unknown"
    )
    return max(
        (provider_level or "unknown", local_level),
        key=lambda value: METADATA_LEVELS.get(value, 0),
    )


def fit_score(asset, bucket_spec):
    bag = set(
        tokens(asset.get("title"))
        + tokens(asset.get("description"))
        + tokens(" ".join(asset.get("tags") or []))
        + tokens(asset.get("query_label"))
    )
    fit = set(tokens(bucket_spec.get("fit")))
    score = len(bag & fit) / max(1, len(fit))
    if bucket_spec.get("ocr_expected") and asset.get("ocr_expected"):
        score += 0.15
    if bucket_spec.get("ocr_expected") and (bag & OCR_HINTS):
        score += 0.15
    if (
        bucket_spec.get("metadata_richness") == "rich"
        and asset.get("metadata_richness") == "rich"
    ):
        score += 0.15
    return round(score, 4)


def summarize_assets(assets):
    summary = {
        "asset_ids": set(),
        "provider_ids": set(),
        "source_urls": set(),
        "sha256": set(),
        "phashes": [],
        "bucket_counts": {},
        "provider_counts": {},
        "query_counts": {},
    }
    for asset in assets:
        if asset.get("asset_id"):
            summary["asset_ids"].add(asset["asset_id"])
        if asset.get("provider") and asset.get("provider_asset_id"):
            summary["provider_ids"].add((asset["provider"], asset["provider_asset_id"]))
        if asset.get("source_url"):
            summary["source_urls"].add(asset["source_url"])
        if asset.get("sha256"):
            summary["sha256"].add(asset["sha256"])
        if asset.get("phash"):
            summary["phashes"].append((asset["asset_id"], asset["phash"]))
        if asset.get("bucket"):
            summary["bucket_counts"][asset["bucket"]] = (
                summary["bucket_counts"].get(asset["bucket"], 0) + 1
            )
        if asset.get("provider"):
            summary["provider_counts"][asset["provider"]] = (
                summary["provider_counts"].get(asset["provider"], 0) + 1
            )
        if asset.get("query_label"):
            summary["query_counts"][asset["query_label"]] = (
                summary["query_counts"].get(asset["query_label"], 0) + 1
            )
    return summary


def reject(staging, asset, reason):
    record = {
        "asset_id": asset.get("asset_id"),
        "provider": asset.get("provider"),
        "provider_asset_id": asset.get("provider_asset_id"),
        "bucket": asset.get("bucket"),
        "query_label": asset.get("query_label"),
        "source_url": asset.get("source_url"),
        "reason": clean(reason),
    }
    key = (record["asset_id"], record["reason"])
    seen = {
        (item.get("asset_id"), item.get("reason"))
        for item in staging.get("rejections", [])
    }
    if key not in seen:
        staging.setdefault("rejections", []).append(record)


def near_duplicate(existing, phash):
    if not phash:
        return ""
    for existing_id, existing_phash in existing:
        if (
            existing_phash
            and sum(left != right for left, right in zip(existing_phash, phash)) <= 5
        ):
            return existing_id
    return ""


def candidate(
    provider,
    bucket,
    query_label,
    title="",
    description="",
    tags=None,
    width=0,
    height=0,
    ext="",
    mime_type="",
    metadata_richness_value="unknown",
    ocr_expected=False,
    provider_asset_id="",
    source_url="",
    download_url="",
    license_name="",
    attribution="",
):
    return {
        "asset_id": build_asset_id(
            provider, provider_asset_id, source_url, query_label
        ),
        "status": "staged",
        "provider": clean(provider),
        "provider_asset_id": clean(provider_asset_id),
        "source_url": clean(source_url),
        "download_url": clean(download_url),
        "license_name": clean(license_name),
        "attribution": clean(attribution),
        "bucket": bucket,
        "query_label": clean(query_label),
        "title": clean(title) or clean(description) or "Untitled",
        "description": clean(description) or clean(title) or "Untitled",
        "tags": split_tags(tags or []),
        "width": int(width or 0),
        "height": int(height or 0),
        "ext": clean(ext).lower(),
        "mime_type": clean(mime_type).lower(),
        "sha256": "",
        "phash": "",
        "metadata_richness": metadata_richness_value or "unknown",
        "ocr_expected": bool(ocr_expected),
        "review_notes": "",
        "local_path": "",
    }


def pixabay_candidates(bucket, bucket_spec, config):
    api_key = os.getenv("PIXABAY_API_KEY", "").strip()
    if not api_key:
        print("Skipping Pixabay: PIXABAY_API_KEY is not set.")
        return []
    out, seen = [], set()
    for query in bucket_spec.get("pixabay", []):
        params = {
            "key": api_key,
            "q": query,
            "image_type": "photo",
            "safesearch": "true",
            "per_page": max(3, int(config.get("max_per_query", 8) or 8)),
        }
        payload = request_url(
            f"{PIXABAY_API_URL}?{urllib_parse.urlencode(params)}", parse_json=True
        )
        for hit in payload.get("hits", []):
            provider_asset_id = str(hit.get("id") or "").strip()
            if not provider_asset_id or provider_asset_id in seen:
                continue
            seen.add(provider_asset_id)
            image_url = hit.get("largeImageURL") or hit.get("webformatURL") or ""
            tags = split_tags(hit.get("tags"))
            out.append(
                candidate(
                    "pixabay",
                    bucket,
                    query,
                    title=tags[0] if tags else query,
                    description=", ".join(tags),
                    tags=tags,
                    width=hit.get("imageWidth"),
                    height=hit.get("imageHeight"),
                    ext=get_ext(image_url),
                    mime_type=get_mime_type(image_url, "image"),
                    metadata_richness_value="sparse",
                    ocr_expected=bucket_spec.get("ocr_expected"),
                    provider_asset_id=provider_asset_id,
                    source_url=hit.get("pageURL") or image_url,
                    download_url=image_url,
                    license_name="Pixabay Content License",
                    attribution=hit.get("user") or "Pixabay contributor",
                )
            )
    return out


def pexels_candidates(bucket, bucket_spec, config):
    api_key = os.getenv("PEXELS_API_KEY", "").strip()
    if not api_key:
        print("Skipping Pexels: PEXELS_API_KEY is not set.")
        return []
    out, seen = [], set()
    headers = {**HEADERS, "Authorization": api_key}
    for query in bucket_spec.get("pexels", []):
        params = {
            "query": query,
            "per_page": max(3, int(config.get("max_per_query", 8) or 8)),
            "page": 1,
        }
        payload = request_url(
            f"{PEXELS_API_URL}?{urllib_parse.urlencode(params)}",
            parse_json=True,
            headers=headers,
        )
        for photo in payload.get("photos", []):
            provider_asset_id = str(photo.get("id") or "").strip()
            if not provider_asset_id or provider_asset_id in seen:
                continue
            seen.add(provider_asset_id)
            src = photo.get("src") or {}
            image_url = (
                src.get("large2x")
                or src.get("large")
                or src.get("original")
                or src.get("medium")
                or ""
            )
            title = clean(photo.get("alt")) or query
            out.append(
                candidate(
                    "pexels",
                    bucket,
                    query,
                    title=title,
                    description=title,
                    tags=split_tags(title),
                    width=photo.get("width"),
                    height=photo.get("height"),
                    ext=get_ext(image_url),
                    mime_type=get_mime_type(image_url, "image"),
                    metadata_richness_value="sparse",
                    ocr_expected=bucket_spec.get("ocr_expected"),
                    provider_asset_id=provider_asset_id,
                    source_url=photo.get("url") or image_url,
                    download_url=image_url,
                    license_name="Pexels License",
                    attribution=clean(photo.get("photographer"))
                    or "Pexels contributor",
                )
            )
    return out


def wikimedia_candidates(bucket, bucket_spec, config):
    def commons_pages(mode, query, limit):
        if mode == "category":
            category = (
                query if str(query).startswith("Category:") else f"Category:{query}"
            )
            payload = request_url(
                f"{COMMONS_API_URL}?{urllib_parse.urlencode({'action': 'query', 'format': 'json', 'list': 'categorymembers', 'cmtitle': category, 'cmnamespace': 6, 'cmlimit': limit})}",
                parse_json=True,
            )
            titles = [
                item["title"]
                for item in (payload.get("query") or {}).get("categorymembers", [])
            ]
            if not titles:
                return []
            params = [
                ("action", "query"),
                ("format", "json"),
                ("prop", "imageinfo"),
                ("iiprop", "url|mime|size|extmetadata"),
            ] + [("titles", title) for title in titles]
            return list(
                (
                    (
                        request_url(
                            f"{COMMONS_API_URL}?{urllib_parse.urlencode(params)}",
                            parse_json=True,
                        ).get("query")
                        or {}
                    ).get("pages")
                    or {}
                ).values()
            )
        params = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": query,
            "gsrnamespace": 6,
            "gsrlimit": limit,
            "prop": "imageinfo",
            "iiprop": "url|mime|size|extmetadata",
        }
        return list(
            (
                (
                    request_url(
                        f"{COMMONS_API_URL}?{urllib_parse.urlencode(params)}",
                        parse_json=True,
                    ).get("query")
                    or {}
                ).get("pages")
                or {}
            ).values()
        )

    out, seen, limit = [], set(), max(3, int(config.get("max_per_query", 8) or 8))
    for mode, query in bucket_spec.get("wikimedia", []):
        for page in commons_pages(mode, query, limit):
            info = ((page.get("imageinfo") or [])[:1] or [None])[0]
            if not info:
                continue
            provider_asset_id = str(
                page.get("pageid") or page.get("title") or ""
            ).strip()
            if not provider_asset_id or provider_asset_id in seen:
                continue
            seen.add(provider_asset_id)
            meta = info.get("extmetadata") or {}
            title = clean(str(page.get("title") or "").replace("File:", ""))
            description = (
                strip_html((meta.get("ImageDescription") or {}).get("value"))
                or strip_html((meta.get("ObjectName") or {}).get("value"))
                or title
            )
            tags = split_tags(
                " | ".join(
                    filter(
                        None,
                        [
                            strip_html((meta.get("Categories") or {}).get("value")),
                            strip_html((meta.get("ObjectName") or {}).get("value")),
                            strip_html(
                                (meta.get("ImageDescription") or {}).get("value")
                            ),
                        ],
                    )
                )
            )
            populated = sum(
                1 for value in meta.values() if strip_html((value or {}).get("value"))
            )
            out.append(
                candidate(
                    "wikimedia",
                    bucket,
                    query,
                    title=title,
                    description=description,
                    tags=tags,
                    width=info.get("width"),
                    height=info.get("height"),
                    ext=get_ext(title),
                    mime_type=info.get("mime") or "",
                    metadata_richness_value="rich" if populated >= 5 else "medium",
                    ocr_expected=bucket_spec.get("ocr_expected"),
                    provider_asset_id=provider_asset_id,
                    source_url=info.get("descriptionurl") or info.get("url") or "",
                    download_url=info.get("url") or "",
                    license_name=strip_html(
                        (meta.get("LicenseShortName") or {}).get("value")
                    )
                    or "Wikimedia Commons",
                    attribution=strip_html((meta.get("Artist") or {}).get("value"))
                    or "Wikimedia Commons contributor",
                )
            )
    return out


def nasa_candidates(bucket, bucket_spec, config):
    out, seen = [], set()
    for query in bucket_spec.get("nasa", []):
        params = {"q": query, "media_type": "image", "page": 1}
        payload = request_url(
            f"{NASA_IMAGES_API_URL}/search?{urllib_parse.urlencode(params)}",
            parse_json=True,
        )
        items = ((payload.get("collection") or {}).get("items") or [])[
            : max(3, int(config.get("max_per_query", 8) or 8))
        ]
        for item in items:
            data = ((item.get("data") or [])[:1] or [None])[0] or {}
            provider_asset_id = clean(data.get("nasa_id")) or clean(item.get("href"))
            if not provider_asset_id or provider_asset_id in seen:
                continue
            links = item.get("links") or []
            image_url = next(
                (
                    link.get("href")
                    for link in links
                    if clean(link.get("render")).lower() == "image"
                    or "image" in clean(link.get("href")).lower()
                ),
                "",
            )
            if not image_url:
                continue
            seen.add(provider_asset_id)
            title = clean(data.get("title")) or query
            description = clean(data.get("description")) or title
            tags = split_tags(data.get("keywords") or []) or split_tags(title)
            out.append(
                candidate(
                    "nasa",
                    bucket,
                    query,
                    title=title,
                    description=description,
                    tags=tags,
                    ext=get_ext(image_url),
                    mime_type=get_mime_type(image_url, "image"),
                    metadata_richness_value=(
                        "rich" if data.get("date_created") else "medium"
                    ),
                    ocr_expected=bucket_spec.get("ocr_expected"),
                    provider_asset_id=provider_asset_id,
                    source_url=f"https://images.nasa.gov/details-{provider_asset_id}",
                    download_url=image_url,
                    license_name="NASA Images and Media Guidelines",
                    attribution=clean(data.get("photographer"))
                    or clean(data.get("center"))
                    or "NASA",
                )
            )
    return out


def local_import_candidates(bucket, config):
    out = []
    for item in config.get("local_import_paths") or []:
        spec = item if isinstance(item, dict) else {"path": item}
        source_path = clean(spec.get("path"))
        if not source_path or clean(spec.get("bucket")) != bucket:
            continue
        title = clean(spec.get("title")) or Path(source_path).stem
        out.append(
            candidate(
                "local_import",
                bucket,
                clean(spec.get("query_label")) or "local_import",
                title=title,
                description=clean(spec.get("description")) or title,
                tags=spec.get("tags")
                or split_tags(title.replace("_", " ").replace("-", " ")),
                ext=get_ext(source_path),
                mime_type=get_mime_type(source_path, "image"),
                metadata_richness_value=spec.get("metadata_richness") or "unknown",
                ocr_expected=bool(
                    spec.get(
                        "ocr_expected",
                        config["bucket_targets"][bucket].get("ocr_expected"),
                    )
                ),
                provider_asset_id=source_path,
                source_url=spec.get("source_url") or source_path,
                license_name=spec.get("license_name") or "Local import",
                attribution=spec.get("attribution") or "Local import",
            )
        )
    return out


def fetch_candidates(provider, bucket, bucket_spec, config):
    if provider == "pixabay":
        return pixabay_candidates(bucket, bucket_spec, config)
    if provider == "pexels":
        return pexels_candidates(bucket, bucket_spec, config)
    if provider == "wikimedia":
        return wikimedia_candidates(bucket, bucket_spec, config)
    if provider == "nasa":
        return nasa_candidates(bucket, bucket_spec, config)
    if provider == "local_import":
        return local_import_candidates(bucket, config)
    return []


def stage_candidate(asset, bucket_spec, config, layout, summary, staging):
    if (
        not asset.get("license_name")
        or not asset.get("attribution")
        or not asset.get("source_url")
    ):
        reason = "missing license, attribution, or source metadata"
        reject(staging, asset, reason)
        return False, reason
    if asset["provider"] != "local_import" and not asset.get("download_url"):
        reason = "missing download metadata"
        reject(staging, asset, reason)
        return False, reason
    if (
        asset["asset_id"] in summary["asset_ids"]
        or (asset["provider"], asset["provider_asset_id"]) in summary["provider_ids"]
        or asset["source_url"] in summary["source_urls"]
    ):
        reason = "asset already staged or approved"
        reject(staging, asset, reason)
        return False, reason

    bucket_dir = layout["incoming"] / asset["bucket"]
    bucket_dir.mkdir(parents=True, exist_ok=True)
    target = None
    try:
        if asset["provider"] == "local_import":
            ext = get_ext(asset.get("ext") or asset["provider_asset_id"]) or "jpg"
            target = bucket_dir / f"{asset['asset_id']}.{ext}"
            shutil.copy2(asset["provider_asset_id"], target)
        else:
            data, mime_type = request_url(asset["download_url"])
            ext = get_ext(asset.get("ext")) or MIME_EXTS.get(mime_type) or "jpg"
            target = bucket_dir / f"{asset['asset_id']}.{ext}"
            target.write_bytes(data)
            asset["mime_type"] = asset.get("mime_type") or mime_type
            asset["ext"] = ext

        ensure_heif_registered(str(target))
        with Image.open(target) as image:
            image = ImageOps.exif_transpose(image)
            image.load()
            asset["width"], asset["height"] = image.size
        if asset["ext"] not in SUPPORTED_EXTS:
            raise ValueError(f"unsupported extension {asset['ext']!r}")
        if asset["width"] < int(config.get("min_width", 0) or 0) or asset[
            "height"
        ] < int(config.get("min_height", 0) or 0):
            raise ValueError(
                f"image below minimum size {config.get('min_width')}x{config.get('min_height')}"
            )
        asset["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
        asset["phash"] = average_hash(str(target))
        if asset["sha256"] in summary["sha256"]:
            raise ValueError("duplicate sha256")
        duplicate = near_duplicate(summary["phashes"], asset["phash"])
        if duplicate:
            raise ValueError(f"near-duplicate perceptual hash matched {duplicate}")
        file_metadata = next(iter(index_paths([str(target)]).values()), {})
        asset["metadata_richness"] = metadata_richness(
            file_metadata, asset.get("metadata_richness")
        )
        if not asset.get("ocr_expected"):
            asset["ocr_expected"] = bool(
                set(
                    tokens(
                        " ".join(asset.get("tags") or [])
                        + " "
                        + asset.get("description", "")
                    )
                )
                & OCR_HINTS
            )
        score = fit_score(asset, bucket_spec)
        if score <= 0:
            raise ValueError(f"bucket fit score too low ({score:.4f})")
        asset["review_notes"] = (
            f"bucket_fit={score:.4f}; provider={asset['provider']}; query={asset['query_label']}; metadata={asset['metadata_richness']}"
        )
        asset["local_path"] = target.resolve().relative_to(layout["root"]).as_posix()
    except Exception as exc:
        if target is not None:
            try:
                target.unlink(missing_ok=True)
            except OSError:
                pass
        reason = clean(str(exc))
        reject(staging, asset, reason)
        return False, reason

    staging.setdefault("assets", []).append(asset)
    return True, ""


def move_asset_to_approved(asset, layout):
    incoming = layout["root"] / asset["local_path"]
    approved_dir = layout["approved"] / asset["bucket"]
    approved_dir.mkdir(parents=True, exist_ok=True)
    destination = approved_dir / incoming.name
    if incoming.exists():
        if destination.exists():
            destination.unlink()
        shutil.move(str(incoming), str(destination))
        asset["local_path"] = (
            destination.resolve().relative_to(layout["root"]).as_posix()
        )
    asset["status"] = "approved"
    return asset


def render_review_frame(image_path, asset):
    frame = cv2.imread(str(image_path))
    if frame is None:
        raise ValueError(f"unable to open image for review: {image_path}")
    height, width = frame.shape[:2]
    scale = min(
        REVIEW_MAX_WIDTH / max(1, width), REVIEW_MAX_HEIGHT / max(1, height), 1.0
    )
    if scale != 1.0:
        frame = cv2.resize(
            frame,
            (max(1, int(width * scale)), max(1, int(height * scale))),
            interpolation=cv2.INTER_AREA,
        )
    overlay = frame.copy()
    cv2.rectangle(overlay, (0, 0), (frame.shape[1], 110), (0, 0, 0), -1)
    frame = cv2.addWeighted(overlay, 0.45, frame, 0.55, 0.0)
    for idx, line in enumerate(
        (
            f"{asset.get('bucket', '')} | {asset.get('provider', '')}",
            asset.get("title", ""),
            "A approve | R remove | S skip | Q quit",
        )
    ):
        cv2.putText(
            frame,
            line[:120],
            (20, 32 + idx * 34),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.75,
            (255, 255, 255),
            2,
            cv2.LINE_AA,
        )
    return frame


def interactive_approve_assets(staging_assets, layout, staging, corpus):
    approved_assets = list(corpus.get("assets", []))
    approved_ids = {asset.get("asset_id") for asset in approved_assets}
    remaining_assets, approved_count, removed_count = [], 0, 0
    print("Review keys: A approve, R remove, S skip, Q quit")
    for index, asset in enumerate(staging_assets):
        asset_id = asset.get("asset_id")
        incoming = layout["root"] / asset["local_path"]
        if asset_id in approved_ids:
            continue
        if not incoming.exists():
            remaining_assets.append(asset)
            continue
        cv2.imshow(REVIEW_WINDOW_NAME, render_review_frame(incoming, asset))
        key = cv2.waitKeyEx(0)
        key_char = chr(key & 0xFF).lower() if 0 <= (key & 0xFF) < 256 else ""
        if key in {27} or key_char == "q":
            remaining_assets.append(asset)
            remaining_assets.extend(staging_assets[index + 1 :])
            break
        if key_char == "a":
            approved_assets.append(move_asset_to_approved(asset, layout))
            approved_ids.add(asset_id)
            approved_count += 1
            continue
        if key_char == "r":
            incoming.unlink(missing_ok=True)
            reject(staging, asset, "removed during interactive review")
            removed_count += 1
            continue
        remaining_assets.append(asset)
    cv2.destroyAllWindows()
    return approved_assets, remaining_assets, approved_count, removed_count


def stage_corpus(config):
    layout = paths(config["corpus_root"])
    ensure_layout(layout)
    staging = read_manifest(layout["staging_manifest"])
    corpus = read_manifest(layout["corpus_manifest"])
    deferred_ids = {
        item.get("asset_id")
        for item in staging.get("rejections", [])
        if str(item.get("reason", "")).startswith("rate limited")
    }
    summary = summarize_assets(
        list(corpus.get("assets", [])) + list(staging.get("assets", []))
    )
    downloaded_assets = []
    rng = random.Random(int(config.get("seed", 0) or 0))
    new_assets = 0
    print(
        f"Accumulated assets: approved={len(corpus.get('assets', []))} "
        f"staged={len(staging.get('assets', []))}"
    )

    for bucket, bucket_spec in (config.get("bucket_targets") or {}).items():
        remaining = int(bucket_spec.get("target", 0) or 0) - summary[
            "bucket_counts"
        ].get(bucket, 0)
        if remaining <= 0:
            continue
        for provider in bucket_spec.get("providers") or []:
            if provider not in (config.get("providers") or []):
                continue
            provider_quota = int(
                (config.get("provider_quotas") or {}).get(provider, 999999) or 999999
            )
            if summary["provider_counts"].get(provider, 0) >= provider_quota:
                print(
                    f"Skipping {provider} for {bucket}: provider quota reached "
                    f"({summary['provider_counts'].get(provider, 0)}/{provider_quota})"
                )
                continue
            print(f"Trying {provider} for {bucket}")
            try:
                candidates = fetch_candidates(provider, bucket, bucket_spec, config)
            except Exception as exc:
                print(f"Skipping {provider} for {bucket}: {exc}")
                continue
            if not candidates:
                print(f"No candidates from {provider} for {bucket}")
                continue
            rng.shuffle(candidates)
            candidates.sort(key=lambda asset: asset.get("asset_id") in deferred_ids)
            misses = 0
            for asset in candidates:
                if (
                    remaining <= 0
                    or summary["provider_counts"].get(provider, 0) >= provider_quota
                ):
                    break
                if summary["query_counts"].get(asset["query_label"], 0) >= int(
                    config.get("max_per_query", 8) or 8
                ):
                    continue
                accepted, reason = stage_candidate(
                    asset, bucket_spec, config, layout, summary, staging
                )
                if accepted:
                    new_assets += 1
                    remaining -= 1
                    misses = 0
                    downloaded_assets.append(
                        {
                            "asset_id": asset["asset_id"],
                            "provider": asset["provider"],
                            "bucket": asset["bucket"],
                            "query_label": asset["query_label"],
                            "title": asset["title"],
                            "local_path": asset["local_path"],
                        }
                    )
                    summary = summarize_assets(
                        list(corpus.get("assets", [])) + list(staging.get("assets", []))
                    )
                    continue
                misses += 1
                if reason.startswith("rate limited"):
                    print(f"Stopping {provider} for {bucket}: {reason}")
                    break
                if misses >= MAX_PROVIDER_MISSES:
                    print(
                        f"Stopping {provider} for {bucket}: {misses} consecutive misses without progress."
                    )
                    break

    write_manifest(layout["staging_manifest"], staging)
    return {
        "mode": "stage",
        "staged_assets": len(staging.get("assets", [])),
        "new_assets": new_assets,
        "rejections": len(staging.get("rejections", [])),
        "downloaded_assets": downloaded_assets,
    }


def approve_corpus(config):
    layout = paths(config["corpus_root"])
    ensure_layout(layout)
    staging = read_manifest(layout["staging_manifest"])
    corpus = read_manifest(layout["corpus_manifest"])
    approval_ids = {
        clean(item) for item in (config.get("approval_ids") or []) if clean(item)
    }

    if not approval_ids:
        approved_assets, remaining_assets, approved_count, removed_count = (
            interactive_approve_assets(
                list(staging.get("assets", [])), layout, staging, corpus
            )
        )
        staging["assets"] = remaining_assets
        corpus["assets"] = approved_assets
        write_manifest(layout["staging_manifest"], staging)
        write_manifest(layout["corpus_manifest"], corpus)
        return {
            "mode": "approve",
            "approved_assets": approved_count,
            "removed_assets": removed_count,
            "remaining_staged_assets": len(staging.get("assets", [])),
            "corpus_assets": len(corpus.get("assets", [])),
        }

    approved_assets = list(corpus.get("assets", []))
    approved_ids = {asset.get("asset_id") for asset in approved_assets}
    remaining_assets, approved_count = [], 0
    for asset in staging.get("assets", []):
        asset_id = asset.get("asset_id")
        if asset_id not in approval_ids:
            remaining_assets.append(asset)
            continue
        if asset_id in approved_ids:
            continue
        approved_assets.append(move_asset_to_approved(asset, layout))
        approved_ids.add(asset_id)
        approved_count += 1

    staging["assets"] = remaining_assets
    corpus["assets"] = approved_assets
    write_manifest(layout["staging_manifest"], staging)
    write_manifest(layout["corpus_manifest"], corpus)
    return {
        "mode": "approve",
        "approved_assets": approved_count,
        "remaining_staged_assets": len(staging.get("assets", [])),
        "corpus_assets": len(corpus.get("assets", [])),
    }


CORPUS_BUILDER_CONFIG = {
    "mode": "approve",
    "corpus_root": "corpora/eval_corpus",
    "providers": ["pixabay", "pexels", "wikimedia", "nasa", "local_import"],
    "bucket_targets": DEFAULT_BUCKET_TARGETS,
    "provider_quotas": {
        "pixabay": 90,
        "pexels": 90,
        "wikimedia": 90,
        "nasa": 45,
        "local_import": 60,
    },
    "local_import_paths": [],
    "approval_ids": [],
    "seed": 13,
    "min_width": 640,
    "min_height": 480,
    "max_per_query": 8,
}


def main(run_config=None):
    config = dict(CORPUS_BUILDER_CONFIG)
    config.update(run_config or {})
    mode = clean(config.get("mode")).lower() or "stage"
    if mode == "stage":
        result = stage_corpus(config)
        print(f"Mode: {result['mode']}")
        print(f"New assets: {result['new_assets']}")
        print(f"Staged assets: {result['staged_assets']}")
        print(f"Rejections: {result['rejections']}")
        for asset in result.get("downloaded_assets", []):
            print(
                f"- {asset['provider']} | {asset['bucket']} | {asset['query_label']} | {asset['title']} -> {asset['local_path']}"
            )
        return result
    if mode == "approve":
        result = approve_corpus(config)
        print(f"Mode: {result['mode']}")
        print(f"Approved assets: {result['approved_assets']}")
        if "removed_assets" in result:
            print(f"Removed assets: {result['removed_assets']}")
        print(f"Remaining staged assets: {result['remaining_staged_assets']}")
        print(f"Corpus assets: {result['corpus_assets']}")
        return result
    raise ValueError(f"Unsupported corpus builder mode: {mode}")


if __name__ == "__main__":
    main()
