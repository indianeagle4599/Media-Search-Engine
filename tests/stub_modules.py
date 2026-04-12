import sys
import types
from types import SimpleNamespace
from unittest.mock import Mock


def install_common_test_stubs(*, with_genai_part: bool = False) -> None:
    timezonefinder = types.ModuleType("timezonefinder")

    class TimezoneFinder:
        def timezone_at(self, lng, lat):
            return None

    timezonefinder.TimezoneFinder = TimezoneFinder
    sys.modules["timezonefinder"] = timezonefinder

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv

    pil = types.ModuleType("PIL")
    image = types.ModuleType("PIL.Image")
    image_ops = types.ModuleType("PIL.ImageOps")
    exif_tags = types.ModuleType("PIL.ExifTags")
    tiff_image = types.ModuleType("PIL.TiffImagePlugin")

    class IFDRational:
        numerator = 0
        denominator = 1

    image.Image = object
    image.LANCZOS = 1
    image.Resampling = SimpleNamespace(LANCZOS=1)
    image_ops.exif_transpose = lambda image_obj: image_obj
    exif_tags.IFD = []
    exif_tags.TAGS = {}
    exif_tags.GPSTAGS = {}
    tiff_image.IFDRational = IFDRational
    pil.Image = image
    pil.ImageOps = image_ops
    pil.ExifTags = exif_tags
    sys.modules["PIL"] = pil
    sys.modules["PIL.Image"] = image
    sys.modules["PIL.ImageOps"] = image_ops
    sys.modules["PIL.ExifTags"] = exif_tags
    sys.modules["PIL.TiffImagePlugin"] = tiff_image

    google = types.ModuleType("google")
    genai = types.ModuleType("google.genai")

    class Client:
        def __init__(self, *args, **kwargs):
            pass

    class APIError(Exception):
        def __init__(self, message="", code=None):
            super().__init__(message)
            self.code = code

    genai.Client = Client
    genai.errors = SimpleNamespace(APIError=APIError)
    if with_genai_part:
        class Part:
            @staticmethod
            def from_bytes(data, mime_type):
                return {"data": data, "mime_type": mime_type}

        class GenerateContentConfig:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        genai.types = SimpleNamespace(
            Part=Part,
            GenerateContentConfig=GenerateContentConfig,
        )
    google.genai = genai
    sys.modules["google"] = google
    sys.modules["google.genai"] = genai


def install_prompt_test_stubs() -> None:
    install_common_test_stubs(with_genai_part=True)


def install_upload_test_stubs() -> None:
    install_common_test_stubs()

    pymongo = types.ModuleType("pymongo")
    pymongo.collection = types.SimpleNamespace(Collection=object)
    pymongo.UpdateOne = object
    sys.modules["pymongo"] = pymongo

    streamlit = types.ModuleType("streamlit")
    streamlit.session_state = {}
    streamlit.cache_resource = lambda **kwargs: (lambda fn: fn)
    streamlit.cache_data = lambda **kwargs: (lambda fn: fn)
    streamlit.dialog = lambda *args, **kwargs: (lambda fn: fn)
    streamlit.data_editor = lambda rows, **kwargs: rows
    streamlit.components = SimpleNamespace(
        v1=SimpleNamespace(html=lambda *args, **kwargs: None)
    )
    sys.modules["streamlit"] = streamlit

    chroma = types.ModuleType("utils.chroma")
    chroma.populate_db = Mock()
    chroma.get_chroma_client = Mock(return_value=object())
    chroma.delete_entry_ids = Mock()
    sys.modules["utils.chroma"] = chroma

    prompt = types.ModuleType("utils.prompt")
    prompt.describe_image_batch = Mock(
        side_effect=lambda client, batch_entries, use_dummy_descriptions=False, **kwargs: {
            entry["entry_id"]: {
                "content": {"summary": "generated description"},
                "context": {},
            }
            for entry in batch_entries
        }
    )
    prompt.describe_image = Mock(
        return_value={"content": {"summary": "generated description"}}
    )
    sys.modules["utils.prompt"] = prompt
