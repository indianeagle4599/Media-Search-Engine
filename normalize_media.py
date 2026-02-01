import os
import json

IM_TYPES = ["jpg", "jpeg", "png", "webp", "heic", "heif", "avif"]
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


def get_dates(file_path: str):
    return ""


def check_file_types(file_root: str):
    files_found = {}
    dummy_meta = {
        "filename": "",  # Name shall be normalized later
        "media_type": "",  # Media type is "image" or "video"
        "ext": "",  # Media type extension
        "is_compat": False,
        # Dates
        "creation_date": "",  # Find out from metadata
        "mod_date": "",  # Find out from metadata
        "index_date": "",  # Get system date
    }
    for path, dirs, files in os.walk(file_root):
        for file in files:
            file_ext = os.path.splitext(file)[-1][1:]
            media_type = ""
            is_compat = False
            if file_ext in IM_TYPES:
                media_type = "image"
            elif file_ext in VI_TYPES:
                media_type = "video"
            if file_ext in COMPAT_TYPES:
                is_compat = True

            file_path = os.path.abspath(os.path.normpath(os.path.join(path, file)))
            files_found[file_path] = dummy_meta.copy()
            files_found[file_path]["filename"] = file
            files_found[file_path]["media_type"] = media_type
            files_found[file_path]["ext"] = file_ext
            files_found[file_path]["is_compat"] = is_compat

    print(json.dumps(files_found, indent=2))


if __name__ == "__main__":
    check_file_types("images_root")
