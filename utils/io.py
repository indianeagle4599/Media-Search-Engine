"""
io.py

Contains utilities to understand folder structure, read media and read associated metadata for clearer context and better standardization.
"""

import os, json, hashlib, warnings, io, mimetypes

from PIL import Image, ExifTags, ImageOps
from PIL.TiffImagePlugin import IFDRational

from utils.date import (
    extract_ifd_date_items,
    extract_text_date_items,
    resolve_file_dates,
)

IFD_CODES = {i.value: i.name for i in ExifTags.IFD}
HEIF_REGISTERED = False
RESAMPLE = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
IM_TYPES = [
    "jpg",
    "jpeg",
    "png",
    "webp",
    "heic",
    "heif",
    "avif",
    "tif",
    "tiff",
    "bmp",
    "svg",
]
VI_TYPES = ["mp4", "mpeg", "mov", "avi", "x-flv", "mpg", "webm", "wmv", "3gpp"]
MIME_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
    "bmp": "image/bmp",
    "heic": "image/heic",
    "heif": "image/heif",
    "avif": "image/avif",
    "tif": "image/tiff",
    "tiff": "image/tiff",
    "svg": "image/svg+xml",
    "mp4": "video/mp4",
    "mpeg": "video/mpeg",
    "mov": "video/quicktime",
    "avi": "video/x-msvideo",
    "x-flv": "video/x-flv",
    "mpg": "video/mpeg",
    "webm": "video/webm",
    "wmv": "video/x-ms-wmv",
    "3gpp": "video/3gpp",
}
METADATA_DEBUG = os.getenv("MEDIA_METADATA_DEBUG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
COMPAT_TYPES = [
    # Images
    "jpg",
    "jpeg",
    "png",
    "webp",
    "heic",
    "heif",
    # Videos
    "mp4",
    "mpeg",
    "mov",
    "avi",
    "x-flv",
    "mpg",
    "webm",
    "wmv",
    "3gpp",
]
DIRECT_ANALYSIS_IMAGE_TYPES = {"jpg", "jpeg", "png", "webp", "heic", "heif"}
PNG_TEXT_METADATA_MAP = {
    "author": "Artist",
    "comment": "UserComment",
    "copyright": "Copyright",
    "description": "ImageDescription",
    "software": "Software",
}
PNG_DATE_METADATA_MAP = {
    "creation time": ("DateTimeOriginal", 100),
    "date:create": ("DateTimeOriginal", 90),
    "date:modify": ("DateTime", 90),
    "modification time": ("DateTime", 100),
    "modify time": ("DateTime", 95),
    "date:timestamp": ("DateTimeDigitized", 80),
}
XMP_TEXT_METADATA_MAP = {
    "artist": "Artist",
    "creator": "Artist",
    "copyright": "Copyright",
    "description": "ImageDescription",
    "rights": "Copyright",
    "title": "ImageDescription",
}
XMP_DATE_METADATA_MAP = {
    "createdate": ("DateTimeOriginal", 100),
    "datecreated": ("DateTimeOriginal", 95),
    "datetimeoriginal": ("DateTimeOriginal", 100),
    "modifydate": ("DateTime", 100),
    "metadatadate": ("DateTimeDigitized", 90),
}
EXIF_WHITELIST = {
    # File
    "Filename",
    "Ext",
    "MimeType",
    "DateItems",
    # Time
    "DateTime",
    "DateTimeOriginal",
    "DateTimeDigitized",
    "OffsetTime",
    "OffsetTimeOriginal",
    "OffsetTimeDigitized",
    "SubsecTime",
    "SubsecTimeOriginal",
    "SubsecTimeDigitized",
    "GPSDateStamp",
    "GPSTimeStamp",
    # Place
    "GPSLatitude",
    "GPSLatitudeRef",
    "GPSLongitude",
    "GPSLongitudeRef",
    "GPSAltitude",
    "GPSAltitudeRef",
    "GPSImgDirection",
    "GPSImgDirectionRef",
    "GPSDestBearing",
    "GPSDestBearingRef",
    "GPSAreaInformation",
    # About image
    "DigitalZoomRatio",
    "XResolution",
    "YResolution",
    "ExifImageWidth",
    "ExifImageHeight",
    "SceneCaptureType",
    # About device
    "Make",
    "Model",
    "LensMake",
    "LensModel",
    "Orientation",
    "FNumber",
    "FocalLength",
    "ApertureValue",
    "Flash",
    "ExposureTime",
    "ShutterSpeedValue",
    "Software",
    # Misc
    "ImageDescription",
    "UserComment",
    "SceneType",
    "SubjectArea",
    "SubjectLocation",
    "AmbientTemperature",
    "Humidity",
    "Pressure",
    "SubjectDistance",
    "Artist",
    "Copyright",
    "Rating",
    "ISO",
    "ISOSpeedRatings",
    "XPTitle",
    "XPComment",
    "XPAuthor",
    "XPKeywords",
    "XPSubject",
}


def warn(msg: str):
    warnings.warn(msg, category=UserWarning, stacklevel=2)


def get_ext(path: str):
    cleaned_path = str(path or "").split("?", 1)[0].split("#", 1)[0]
    return os.path.splitext(cleaned_path)[-1][1:].lower()


def get_mime_type(file_path: str, media_type: str = "") -> str:
    ext = get_ext(file_path)
    mime_type = MIME_TYPES.get(ext)
    if mime_type:
        return mime_type

    guessed_type, _ = mimetypes.guess_type(file_path)
    if guessed_type:
        return guessed_type

    if media_type and ext:
        return f"{media_type}/{ext}"
    return ""


def ensure_heif_registered(file_path: str = "") -> None:
    global HEIF_REGISTERED
    if HEIF_REGISTERED or get_ext(file_path) not in {"heic", "heif"}:
        return

    from pillow_heif import register_heif_opener

    register_heif_opener()
    HEIF_REGISTERED = True


def normalize_analysis_max_size(
    max_width: int | None,
    max_height: int | None,
) -> tuple[int, int] | None:
    width = int(max_width or 0)
    height = int(max_height or 0)
    if width <= 0 and height <= 0:
        return None
    if width <= 0:
        width = 10000
    if height <= 0:
        height = 10000
    return width, height


def fit_image_size_within_bounds(
    width: int,
    height: int,
    max_width: int | None,
    max_height: int | None,
) -> tuple[int, int]:
    if width <= 0 or height <= 0:
        raise ValueError("Image width and height must be positive integers.")

    max_size = normalize_analysis_max_size(max_width, max_height)
    if not max_size:
        return width, height

    width_scale = max_size[0] / width
    height_scale = max_size[1] / height
    scale = min(width_scale, height_scale, 1.0)
    return max(1, int(width * scale)), max(1, int(height * scale))


def read_file_bytes(file_path: str) -> bytes:
    with open(file_path, "rb") as file:
        return file.read()


def get_analysis_image_bytes(
    file_path: str,
    mime_type: str = "",
    *,
    max_width: int | None = None,
    max_height: int | None = None,
    quality: int = 86,
) -> tuple[bytes, str]:
    file_ext = get_ext(file_path)
    max_size = normalize_analysis_max_size(max_width, max_height)
    fallback_mime_type = mime_type or get_mime_type(file_path, "image")
    requires_proxy = file_ext not in DIRECT_ANALYSIS_IMAGE_TYPES
    if not max_size and not requires_proxy:
        return read_file_bytes(file_path), fallback_mime_type

    try:
        ensure_heif_registered(file_path)
        with Image.open(file_path) as image:
            image = ImageOps.exif_transpose(image)
            if image.mode == "P":
                image = image.convert("RGBA")
            elif image.mode not in {"RGB", "RGBA", "L", "LA"}:
                image = image.convert("RGB")

            target_size = (
                fit_image_size_within_bounds(
                    image.width,
                    image.height,
                    max_size[0],
                    max_size[1],
                )
                if max_size
                else (image.width, image.height)
            )
            if not requires_proxy and target_size == (image.width, image.height):
                return read_file_bytes(file_path), fallback_mime_type

            if target_size != (image.width, image.height):
                image = image.resize(target_size, RESAMPLE)
            buffer = io.BytesIO()
            if not requires_proxy and image.mode in {"RGBA", "LA"}:
                image.save(buffer, format="PNG", optimize=True)
                return buffer.getvalue(), "image/png"

            if image.mode in {"RGBA", "LA"}:
                background = Image.new("RGB", image.size, (255, 255, 255))
                background.paste(image, mask=image.getchannel("A"))
                image = background
            elif image.mode != "RGB":
                image = image.convert("RGB")
            image.save(buffer, format="JPEG", quality=quality, optimize=True)
            return buffer.getvalue(), "image/jpeg"
    except Exception as exc:
        if requires_proxy:
            raise OSError(
                f"Failed to create JPEG analysis proxy for '{file_path}': {exc}"
            ) from exc
        warn(f"Failed to create analysis proxy for '{file_path}': {exc}")
        return read_file_bytes(file_path), fallback_mime_type


def decode_bytes(data: bytes):
    try:
        if isinstance(data, bytes):
            encodings = (
                ("utf-16le", "utf-8", "latin-1")
                if b"\x00" in data
                else ("utf-8", "utf-16le", "latin-1")
            )
            for encoding in encodings:
                try:
                    return data.decode(encoding).strip("\x00").strip()
                except UnicodeDecodeError:
                    continue
            return
        elif isinstance(data, IFDRational):
            return float(data.numerator) / float(data.denominator)
        elif isinstance(data, tuple):
            return tuple([decode_bytes(i) for i in data])
    except:
        return
    return data


def get_hash(file_path: str):
    try:
        image_hash = ""
        with open(file_path, "rb") as f:
            image_hash = hashlib.sha256(f.read()).hexdigest()
        if image_hash:
            return image_hash
    except Exception as e:
        warn(f"Error occurred while getting hash for '{file_path}': {e}")
    raise ValueError(f"Unable to get file hash for: '{file_path}'")


# Metadata cleaning and extraction
def clean_exif_tags(extracted_meta: dict):
    clean_meta = {}
    for key, value in extracted_meta.items():
        if isinstance(value, dict):
            clean_subdict = clean_exif_tags(value)
            if clean_subdict:
                clean_meta[key] = clean_subdict
        elif key in EXIF_WHITELIST:
            clean_meta[key] = value
    return clean_meta


def get_exif_dict(img_exif: dict, extracted_meta: dict):
    date_items = {}
    for ifd_key, ifd_value in img_exif.items():
        if ifd_key in IFD_CODES:
            ifd_name = IFD_CODES[ifd_key]
            ifd_data = {}
            ifd_subdata = img_exif.get_ifd(ifd_key).items()
            for ifd_subkey, ifd_subvalue in ifd_subdata:
                ifd_subname = (
                    ExifTags.GPSTAGS.get(ifd_subkey, None)
                    or ExifTags.TAGS.get(ifd_subkey, None)
                    or ifd_subkey
                )
                ifd_data[ifd_subname] = decode_bytes(ifd_subvalue)
            if isinstance(ifd_data, dict) and ifd_name in ["Exif", "GPSInfo"]:
                date_items = extract_ifd_date_items(ifd_name, ifd_data, date_items)
        else:
            ifd_name = ExifTags.TAGS.get(ifd_key, ifd_key)
            ifd_data = decode_bytes(img_exif.get(ifd_key))
        if ifd_name and ifd_data:
            extracted_meta[ifd_name] = ifd_data
    if date_items:
        extracted_meta["DateItems"] = date_items
    return extracted_meta


def get_png_text_metadata(image_info: dict) -> dict:
    extracted_meta = {}

    for raw_key, raw_value in (image_info or {}).items():
        key = str(raw_key or "").strip()
        normalized_key = key.lower()
        value = decode_bytes(raw_value)
        if value is None:
            continue

        text_value = str(value).strip()
        if not text_value:
            continue

        metadata_key = PNG_TEXT_METADATA_MAP.get(normalized_key)
        if metadata_key:
            extracted_meta[metadata_key] = text_value

    date_items = extract_text_date_items(image_info, PNG_DATE_METADATA_MAP)
    if date_items:
        extracted_meta["DateItems"] = date_items
    return extracted_meta


def get_xmp_metadata(image) -> dict:
    xmp_reader = getattr(image, "getxmp", None)
    if not callable(xmp_reader):
        return {}

    try:
        xmp_data = xmp_reader() or {}
    except Exception as exc:
        if METADATA_DEBUG:
            warn(f"Failed to extract XMP metadata: {exc}")
        return {}

    if not isinstance(xmp_data, dict) or not xmp_data:
        return {}

    flat_xmp = {}

    def collect_xmp_values(value, key_name=""):
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                collect_xmp_values(
                    child_value,
                    str(child_key).rsplit(":", 1)[-1].rsplit("}", 1)[-1].lower(),
                )
            return
        if isinstance(value, list):
            for item in value:
                collect_xmp_values(item, key_name)
            return
        if not key_name:
            return
        text_value = str(value).strip()
        if text_value:
            flat_xmp.setdefault(key_name, text_value)

    collect_xmp_values(xmp_data)
    if not flat_xmp:
        return {}

    extracted_meta = {}
    for raw_key, metadata_key in XMP_TEXT_METADATA_MAP.items():
        text_value = flat_xmp.get(raw_key)
        if text_value:
            extracted_meta[metadata_key] = text_value

    date_items = extract_text_date_items(flat_xmp, XMP_DATE_METADATA_MAP)
    if date_items:
        extracted_meta["DateItems"] = date_items
    return extracted_meta


def get_embedded_metadata(image_path: str):
    supported_exts = {"jpg", "jpeg", "png", "heif", "heic"}
    extracted_meta = {}
    file_ext = get_ext(image_path)
    if file_ext not in supported_exts:
        return extracted_meta

    try:
        ensure_heif_registered(image_path)
        with Image.open(image_path) as image:
            extracted_meta["Filename"] = os.path.basename(image_path)
            extracted_meta["Ext"] = file_ext
            extracted_meta["MimeType"] = get_mime_type(image_path, "image")

            if file_ext == "png":
                image.load()

            img_exif = image.getexif()
            if METADATA_DEBUG:
                print(
                    "Metadata debug | phase=open"
                    f" | file={image_path!r}"
                    f" | ext={file_ext!r}"
                    f" | format={getattr(image, 'format', None)!r}"
                    f" | info_keys={sorted(str(key) for key in image.info.keys())!r}"
                    f" | text_keys={sorted(str(key) for key in (getattr(image, 'text', None) or {}).keys())!r}"
                    f" | exif_keys={sorted(str(ExifTags.TAGS.get(key, key)) for key in img_exif.keys())!r}"
                )
            if img_exif:
                extracted_meta = get_exif_dict(img_exif, extracted_meta)
            if file_ext == "png":
                png_text_metadata = get_png_text_metadata(
                    getattr(image, "text", None) or image.info
                )
                if png_text_metadata:
                    extracted_meta.update(png_text_metadata)
            xmp_metadata = get_xmp_metadata(image)
            if xmp_metadata:
                xmp_date_items = xmp_metadata.pop("DateItems", {})
                if xmp_date_items:
                    merged_date_items = dict(extracted_meta.get("DateItems", {}))
                    for key, value in xmp_date_items.items():
                        merged_date_items.setdefault(key, value)
                    extracted_meta["DateItems"] = merged_date_items
                for key, value in xmp_metadata.items():
                    extracted_meta.setdefault(key, value)
    except Exception as exc:
        warn(f"Failed to extract embedded metadata for '{image_path}': {exc}")
        return {}

    cleaned_meta = clean_exif_tags(extracted_meta=extracted_meta)
    if METADATA_DEBUG:
        print(
            "Metadata debug | phase=embedded"
            f" | file={image_path!r}"
            f" | ext={file_ext!r}"
            f" | raw_keys={sorted(extracted_meta.keys())!r}"
            f" | date_items={extracted_meta.get('DateItems', {})!r}"
            f" | cleaned_keys={sorted(cleaned_meta.keys())!r}"
        )
    return cleaned_meta


# Main indexing function
def build_file_metadata(
    file_path: str,
    metadata_override: dict | None = None,
) -> tuple[str, dict]:
    file_path = os.path.abspath(os.path.normpath(file_path)).replace("\\", "/")
    file = os.path.basename(file_path)
    file_ext = os.path.splitext(file)[-1][1:].lower()
    media_type = ""
    is_compat = False
    if file_ext in IM_TYPES:
        media_type = "image"
    elif file_ext in VI_TYPES:
        media_type = "video"
    if file_ext in COMPAT_TYPES:
        is_compat = True

    file_hash = get_hash(file_path)
    metadata = {
        "file_hash": file_hash,
        "file_path": file_path,
        "file_name": file,
        "media_type": media_type,
        "ext": file_ext,
        "mime_type": get_mime_type(file_path, media_type),
        "is_compat": is_compat,
        "dates": {},
        "embedded_metadata": {},
    }

    embedded_metadata = get_embedded_metadata(file_path)
    extracted_date_items = embedded_metadata.pop("DateItems", {})
    metadata["dates"] = resolve_file_dates(
        file_path,
        extracted_date_items,
    )
    metadata["embedded_metadata"] = embedded_metadata
    if METADATA_DEBUG and media_type == "image":
        print(
            "Metadata debug | phase=resolved"
            f" | file={file_path!r}"
            f" | ext={file_ext!r}"
            f" | extracted_date_items={extracted_date_items!r}"
            f" | resolved_dates={metadata['dates']!r}"
            f" | embedded_metadata={embedded_metadata!r}"
        )

    if metadata_override:
        metadata.update(metadata_override)
        metadata["file_hash"] = file_hash
        metadata["file_path"] = file_path
        metadata["ext"] = file_ext
        metadata["mime_type"] = get_mime_type(file_path, media_type)

    return file_hash, metadata


def index_paths(
    file_paths: list[str],
    metadata_overrides: dict[str, dict] | None = None,
    verbose: bool = False,
):
    files_index = {}
    metadata_overrides = metadata_overrides or {}
    for file_path in file_paths:
        normalized_path = os.path.abspath(os.path.normpath(file_path)).replace(
            "\\", "/"
        )
        override = metadata_overrides.get(normalized_path) or metadata_overrides.get(
            file_path
        )
        file_hash, metadata = build_file_metadata(normalized_path, override)
        files_index[file_hash] = metadata

    if verbose:
        print(json.dumps(files_index, indent=2))
    return files_index


def index_folder(file_root: str, verbose: bool = False):
    file_paths = []
    for path, dirs, files in os.walk(file_root):
        for file in files:
            file_paths.append(os.path.join(path, file))
    return index_paths(file_paths, verbose=verbose)


if __name__ == "__main__":
    import sys
    from datetime import datetime

    image_root = (
        sys.argv[1] if len(sys.argv) > 1 else r"C:\Users\hites\Desktop\test_images"
    )
    if (
        not os.path.exists(image_root)
        and len(image_root) >= 3
        and image_root[1:3] == ":\\"
    ):
        drive = image_root[0].lower()
        relative_root = image_root[3:].replace("\\", "/")
        image_root = f"/mnt/{drive}/{relative_root}"

    today = datetime.now().astimezone().date().isoformat()
    scan_exts = {"jpg", "jpeg", "png"}

    for path, _, files in os.walk(image_root):
        for file_name in files:
            file_path = os.path.join(path, file_name)
            file_ext = get_ext(file_path)
            if file_ext not in scan_exts:
                continue

            embedded_metadata = get_embedded_metadata(file_path)
            extracted_date_items = embedded_metadata.pop("DateItems", {})
            dates = resolve_file_dates(file_path, extracted_date_items)

            reasons = []
            if not extracted_date_items:
                reasons.append("no_embedded_date_items")
            if not embedded_metadata:
                reasons.append("no_embedded_metadata")
            if str(dates.get("master_date", "")).startswith(today):
                reasons.append("master_date_is_today")
            if str(dates.get("true_creation_date", "")).startswith(today):
                reasons.append("true_creation_date_is_today")
            if "modification_before_creation" in (dates.get("flags") or []):
                reasons.append("modification_before_creation")

            if not reasons:
                continue

            source_probe = {}
            try:
                with Image.open(file_path) as image:
                    if file_ext == "png":
                        image.load()
                    raw_exif = image.getexif()
                    raw_xmp = {}
                    xmp_reader = getattr(image, "getxmp", None)
                    if callable(xmp_reader):
                        try:
                            raw_xmp = xmp_reader() or {}
                        except Exception:
                            raw_xmp = {}
                    source_probe = {
                        "info_keys": sorted(str(key) for key in image.info.keys()),
                        "text_keys": sorted(
                            str(key)
                            for key in (getattr(image, "text", None) or {}).keys()
                        ),
                        "exif_keys": sorted(
                            str(ExifTags.TAGS.get(key, key)) for key in raw_exif.keys()
                        ),
                        "xmp_keys": (
                            sorted(str(key) for key in raw_xmp.keys())
                            if isinstance(raw_xmp, dict)
                            else []
                        ),
                    }
            except Exception as exc:
                source_probe = {"probe_error": str(exc)}

            embedded_date_fields = {
                key: embedded_metadata.get(key)
                for key in (
                    "DateTime",
                    "DateTimeOriginal",
                    "DateTimeDigitized",
                    "GPSDateStamp",
                    "Software",
                    "ImageDescription",
                    "UserComment",
                    "Artist",
                    "Copyright",
                )
                if key in embedded_metadata
            }
            print(
                json.dumps(
                    {
                        "file": file_path,
                        "ext": file_ext,
                        "reasons": reasons,
                        "source_probe": source_probe,
                        "extracted_date_items": extracted_date_items,
                        "embedded_date_fields": embedded_date_fields,
                        "resolved_dates": {
                            key: dates.get(key)
                            for key in (
                                "master_date",
                                "true_creation_date",
                                "true_modification_date",
                                "creation_date",
                                "modification_date",
                                "index_date",
                                "date_reliability",
                                "flags",
                            )
                        },
                    },
                    ensure_ascii=True,
                )
            )
