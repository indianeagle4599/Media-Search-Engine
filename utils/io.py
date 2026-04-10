"""
io.py

Contains utilities to understand folder structure, read media and read associated metadata for clearer context and better standardization.
"""

import os, json, hashlib, warnings

from PIL import Image, ExifTags
from PIL.TiffImagePlugin import IFDRational

from utils.date import (
    extract_ifd_date_items,
    get_os_dates,
    resolve_dates,
)

IFD_CODES = {i.value: i.name for i in ExifTags.IFD}
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
EXIF_WHITELIST = {
    # File
    "Filename",
    "Ext",
    "MimeType",
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
}


def warn(msg: str):
    warnings.warn(msg, category=UserWarning, stacklevel=2)


def get_ext(path: str):
    return os.path.splitext(path)[-1][1:].lower()


def decode_bytes(data: bytes):
    try:
        if isinstance(data, bytes):
            return data.decode().strip("\x00").strip()
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


def get_embedded_metadata(image_path: str):
    jpgs, pngs, heifs = ["jpg", "jpeg"], ["png"], ["heif", "heic"]

    extracted_meta = {}
    file_ext = get_ext(image_path)

    if file_ext in jpgs:
        img = Image.open(image_path)
        img_exif = img.getexif()
        extracted_meta = get_exif_dict(img_exif, extracted_meta)
        extracted_meta = clean_exif_tags(extracted_meta=extracted_meta)
    elif file_ext in pngs:
        # TODO: Extract basic metadata for PNGs
        warn("PNG Metadata extraction currently not supported")
    elif file_ext in heifs:
        # TODO: Extract basic metadata for HEIF/HEIC
        warn("HEIF/HEIC Metadata extraction currently not supported")
    return extracted_meta


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
        "is_compat": is_compat,
        "dates": {},
        "embedded_metadata": {},
    }

    embedded_metadata = get_embedded_metadata(file_path)
    dates = get_os_dates(file_path)
    dates.update(embedded_metadata.pop("DateItems", {}))
    metadata["dates"] = resolve_dates(dates)
    metadata["embedded_metadata"] = embedded_metadata

    if metadata_override:
        metadata.update(metadata_override)
        metadata["file_hash"] = file_hash
        metadata["file_path"] = file_path
        metadata["ext"] = file_ext

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
    index_folder("images_root", verbose=True)
