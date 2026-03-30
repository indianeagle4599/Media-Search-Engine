"""
io.py

Contains utilities to understand folder structure, read media and read associated metadata for clearer context and better standardization.
"""

import os, platform, json, hashlib, warnings
from datetime import datetime, timezone, timedelta
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo

from PIL import Image, ExifTags
from PIL.TiffImagePlugin import IFDRational

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
DATE_KEYS = (
    "creation_date",
    "modification_date",
    "index_date",
    "DateTime",
    "DateTimeOriginal",
    "DateTimeDigitized",
    "GPSDateStamp",
)
TF = TimezoneFinder()
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


# Date Management
def format_datetime(value):
    def clean(dt):
        if not dt:
            return None
        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc) + timedelta(days=1)
        return dt.isoformat() if 1980 <= dt.year and dt <= now else None

    if isinstance(value, (float, int)):
        return clean(datetime.fromtimestamp(value, tz=timezone.utc))

    if isinstance(value, (list, tuple)):
        return type(value)(format_datetime(v) for v in value)

    if isinstance(value, str):
        try:
            return clean(datetime.fromisoformat(value))
        except ValueError:
            return None

    return None


def get_windows_times(file_path: str):
    now = datetime.now().timestamp()
    try:
        ctime, mtime = os.path.getctime(file_path), os.path.getmtime(file_path)
        return ctime, mtime, now
    except (FileNotFoundError, PermissionError) as e:
        warn(f"Error getting file times for {file_path}: {e}")
    return now, now, now


def get_unix_times(file_path: str):
    now = datetime.now().timestamp()
    try:
        stat = os.stat(file_path)
        try:
            ctime = stat.st_birthtime
        except AttributeError:
            ctime = min(stat.st_mtime, stat.st_ctime)
        mtime = os.path.getmtime(file_path)
        return ctime, mtime, now
    except (FileNotFoundError, PermissionError) as e:
        warn(f"Error getting file times for {file_path}: {e}")
    return now, now, now


def get_time_function():
    support_dict = {
        "Windows": get_windows_times,
        "Linux": get_unix_times,
        "Darwin": get_unix_times,
    }
    curr_os = platform.system()
    return support_dict.get(curr_os, get_unix_times)


def get_os_dates(file_path: str):
    creation_date, modification_date, index_date = get_time_function()(file_path)
    return {
        "creation_date": format_datetime(creation_date),
        "modification_date": format_datetime(modification_date),
        "index_date": datetime.fromtimestamp(index_date)
        .astimezone()
        .isoformat(),  # Use local timezone for index date
    }


def _resolve_date_values(vals: dict):
    flags = []

    if (
        vals["creation_date"]
        and vals["modification_date"]
        and vals["creation_date"] > vals["modification_date"]
    ):
        vals["creation_date"] = vals["modification_date"]
        flags.append("os_creation_after_modification_corrected")

    created = (
        vals["DateTimeOriginal"]
        or vals["DateTimeDigitized"]
        or vals["GPSDateStamp"]
        or vals["creation_date"]
    )
    modified = vals["DateTime"] or vals["modification_date"]

    if created and modified and modified < created:
        flags.append("modification_before_creation")
        if created == vals["creation_date"] and vals["DateTime"]:
            created = modified
            flags.append("os_creation_downgraded_to_modification")

    return created, modified, flags


def resolve_dates(dates: dict) -> dict:
    vals = {k: dates.get(k) for k in DATE_KEYS}
    created, modified, flags = _resolve_date_values(vals)

    return {
        "master_date": created or modified,
        "true_creation_date": created,
        "true_modification_date": modified,
        "index_date": vals["index_date"],
        "creation_date": vals["creation_date"],
        "modification_date": vals["modification_date"],
        "date_reliability": (
            "invalid"
            if not (created or modified)
            else (
                "high"
                if vals["DateTimeOriginal"]
                and "modification_before_creation" not in flags
                else (
                    "medium"
                    if (vals["DateTimeDigitized"] or vals["GPSDateStamp"])
                    and "modification_before_creation" not in flags
                    else "low"
                )
            )
        ),
        "flags": flags,
    }


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


def get_local_gps_time(gps_info: dict, utc_dt: datetime) -> str:
    """Finds timezone from coordinates and shifts UTC time to Local time."""
    lat, lat_ref = gps_info.get("GPSLatitude"), gps_info.get("GPSLatitudeRef")
    lon, lon_ref = gps_info.get("GPSLongitude"), gps_info.get("GPSLongitudeRef")

    if lat and lat_ref and lon and lon_ref:
        try:
            lat_dec = float(lat[0]) + float(lat[1]) / 60 + float(lat[2]) / 3600
            if lat_ref.upper() == "S":
                lat_dec = -lat_dec

            lon_dec = float(lon[0]) + float(lon[1]) / 60 + float(lon[2]) / 3600
            if lon_ref.upper() == "W":
                lon_dec = -lon_dec

            tz_str = TF.timezone_at(lng=lon_dec, lat=lat_dec)
            if tz_str:
                return utc_dt.astimezone(ZoneInfo(tz_str)).isoformat()
        except Exception as e:
            warn(f"Error converting GPS time to local: {e}")
    return utc_dt.isoformat()


def extract_ifd_date_items(ifd_name: str, ifd_data: dict, date_items: dict):
    if ifd_name == "Exif":
        temp_date_items = {}
        dt_keys = {
            "DateTime": ["SubsecTime", "OffsetTime"],
            "DateTimeOriginal": ["SubsecTimeOriginal", "OffsetTimeOriginal"],
            "DateTimeDigitized": ["SubsecTimeDigitized", "OffsetTimeDigitized"],
        }
        for base_key, (ss_key, ofs_key) in dt_keys.items():
            if base_key not in ifd_data:
                continue

            raw_date = str(ifd_data.get(base_key, "")).strip()
            if not raw_date:
                continue

            base_date = raw_date.replace(":", "-", 2).replace(" ", "T", 1)
            subsec = str(ifd_data.get(ss_key, "000")).strip()
            offset = str(ifd_data.get(ofs_key, "+00:00")).strip()

            temp_date_items[base_key] = format_datetime(f"{base_date}.{subsec}{offset}")
        date_items.update(temp_date_items)

    elif ifd_name == "GPSInfo":
        raw_date = ifd_data.get("GPSDateStamp", None)
        if not raw_date:
            return date_items

        gps_date = str(raw_date).replace(":", "-", 2)
        h_raw, m_raw, s_raw = ifd_data.get("GPSTimeStamp", [0.0, 0.0, 0.0])
        h, m, s_float, s_int = int(h_raw), int(m_raw), float(s_raw), int(s_raw)
        ss = int(round((s_float % 1) * 1_000_000))

        base_utc_str = f"{gps_date}T{h:02d}:{m:02d}:{s_int:02d}.{ss:06d}+00:00".strip()
        try:
            base_utc_date = datetime.fromisoformat(base_utc_str)
            date_items["GPSDateStamp"] = format_datetime(
                get_local_gps_time(ifd_data, base_utc_date)
            )
        except Exception as e:
            warn(f"Error parsing GPS datetime: {e}")
    return date_items


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
def index_folder(file_root: str, verbose: bool = False):
    files_index = {}
    dummy_meta = {
        "file_hash": "",
        # Essentials
        "file_path": "",
        "file_name": "",  # Name shall be normalized later
        "media_type": "",  # Media type is "image" or "video"
        "ext": "",  # Media type extension
        "is_compat": False,
        # Dates
        "dates": {},
        # Detailed metadata
        "metadata": {},
    }
    for path, dirs, files in os.walk(file_root):
        for file in files:
            file_ext = os.path.splitext(file)[-1][1:].lower()
            media_type = ""
            is_compat = False
            if file_ext in IM_TYPES:
                media_type = "image"
            elif file_ext in VI_TYPES:
                media_type = "video"
            if file_ext in COMPAT_TYPES:
                is_compat = True

            file_path = os.path.abspath(
                os.path.normpath(os.path.join(path, file))
            ).replace("\\", "/")
            file_hash = get_hash(file_path)
            files_index[file_hash] = dummy_meta.copy()

            files_index[file_hash].update(
                {
                    "file_path": file_path,
                    "file_name": file,
                    "media_type": media_type,
                    "ext": file_ext,
                    "is_compat": is_compat,
                }
            )
            metadata = get_embedded_metadata(file_path)
            dates = get_os_dates(file_path)
            dates.update(metadata.pop("DateItems", {}))

            files_index[file_hash]["dates"] = resolve_dates(dates)
            files_index[file_hash]["metadata"] = metadata

    if verbose:
        print(json.dumps(files_index, indent=2))
    return files_index


if __name__ == "__main__":
    index_folder("images_root", verbose=True)
