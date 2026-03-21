"""
io.py

Contains utilities to understand folder structure, read media and read associated metadata for clearer context and better standardization.
"""

import os, json, hashlib
from datetime import datetime

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
        print(e)
    raise f"Unable to get file hash for: '{file_path}'"


def clean_exif_tags(extracted_meta: dict):
    whitelist = {
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
        "GPSLongitude",
        "GPSAltitude",
        "GPSImgDirection",
        "GPSDestBearing",
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
    clean_meta = {}
    for key, value in extracted_meta.items():
        if type(value) == dict:
            clean_subdict = clean_exif_tags(value)
            if clean_subdict:
                clean_meta[key] = clean_subdict
        elif key in whitelist:
            clean_meta[key] = value
    return clean_meta


def get_embedded_metadata(image_path: str):
    # Extract:
    # - DateTime orig, OffsetTime/TimeOrig, SubsecTimeOrig
    # - GPSInfo, GPSDestBearing, AmbientTemp, Humidity
    # - LensModel/Software, HostComputer
    # - ImageDescription
    jpgs, pngs, heifs = ["jpg", "jpeg"], ["png"], ["heif", "heic"]

    extracted_meta = {}
    file_ext = get_ext(image_path)

    if file_ext in jpgs:
        img = Image.open(image_path)
        img_exif = img.getexif()
        for ifd_key, ifd_value in img_exif.items():
            if ifd_key in IFD_CODES:
                ifd_name = IFD_CODES[ifd_key]
                ifd_subdata = img_exif.get_ifd(ifd_key).items()
                ifd_data = {}
                for ifd_subkey, ifd_subvalue in ifd_subdata:
                    ifd_subname = (
                        ExifTags.GPSTAGS.get(ifd_subkey, None)
                        or ExifTags.TAGS.get(ifd_subkey, None)
                        or ifd_subkey
                    )
                    ifd_data[ifd_subname] = decode_bytes(ifd_subvalue)
            else:
                ifd_name = ExifTags.TAGS.get(ifd_key, ifd_key)
                ifd_data = decode_bytes(img_exif.get(ifd_key))
            extracted_meta[ifd_name] = ifd_data
        extracted_meta = clean_exif_tags(extracted_meta=extracted_meta)
    elif file_ext in pngs:
        print("PNG Not supported")
    elif file_ext in heifs:
        print("HEIF/HEIC Not supported")

    # print("=" * 20)
    # print(json.dumps(extracted_meta, indent=2))
    return extracted_meta


def format_datetime(timestamp: float | tuple | list):
    input_type = type(timestamp)
    if input_type == float:
        formatted_timestamp = str(datetime.fromtimestamp(timestamp))
    elif input_type == list or input_type == tuple:
        formatted_timestamp = []
        for timestamp_i in timestamp:
            formatted_timestamp.append(format_datetime(timestamp_i))
        formatted_timestamp = input_type(formatted_timestamp)
    else:
        formatted_timestamp = datetime.now()
    return formatted_timestamp


def get_windows_times(file_path: str):
    ctime, mtime = os.path.getctime(file_path), os.path.getmtime(file_path)
    return ctime, mtime, datetime.now().timestamp()


def get_unix_times(file_path: str):
    stat = os.stat(file_path)
    try:
        ctime = stat.st_birthtime
    except AttributeError:
        ctime = stat.st_ctime
    mtime = os.path.getmtime(file_path)
    return ctime, mtime, datetime.now().timestamp()


def get_time_function():
    import platform

    support_dict = {
        "Windows": get_windows_times,
        "Linux": get_unix_times,
        "Darwin": get_unix_times,
    }
    curr_os = platform.system()
    if curr_os in support_dict.keys():
        return support_dict[curr_os]
    return None


def get_dates(file_path: str):
    creation_date, modification_date, index_date = format_datetime(
        get_time_function()(file_path)
    )
    return {
        "creation_date": creation_date,
        "modification_date": modification_date,
        "index_date": index_date,
    }


def index_folder(file_root: str, verbose: bool = False):
    files_index = {}
    dummy_meta = {
        "file_hash": "",
        # Essentials
        "file_name": "",  # Name shall be normalized later
        "media_type": "",  # Media type is "image" or "video"
        "ext": "",  # Media type extension
        "is_compat": False,
        # Dates
        "creation_date": "",
        "modification_date": "",
        "index_date": "",
        # Detailed metadata
        "extracted_metadata": {},
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

            file_path = os.path.abspath(os.path.normpath(os.path.join(path, file)))
            file_hash = get_hash(file_path)
            files_index[file_hash] = dummy_meta.copy()

            files_index[file_hash].update(
                {
                    "file_path": file_path,
                    "file_name": file,
                    "media_type": media_type,
                    "ext": file_ext,
                    "is_compat": is_compat,
                    "extracted_metadata": get_embedded_metadata(file_path),
                }
            )
            files_index[file_hash].update(get_dates(file_path))

    if verbose:
        print(json.dumps(files_index, indent=2))
    return files_index


if __name__ == "__main__":
    index_folder("images_root", verbose=True)
