import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


def install_external_stubs():
    timezonefinder = types.ModuleType("timezonefinder")

    class TimezoneFinder:
        def timezone_at(self, lng, lat):
            return None

    timezonefinder.TimezoneFinder = TimezoneFinder
    sys.modules["timezonefinder"] = timezonefinder

    pil = types.ModuleType("PIL")
    image = types.ModuleType("PIL.Image")
    exif_tags = types.ModuleType("PIL.ExifTags")
    tiff_image = types.ModuleType("PIL.TiffImagePlugin")

    class IFDRational:
        numerator = 0
        denominator = 1

    exif_tags.IFD = []
    exif_tags.TAGS = {}
    exif_tags.GPSTAGS = {}
    tiff_image.IFDRational = IFDRational
    pil.Image = image
    pil.ExifTags = exif_tags
    sys.modules["PIL"] = pil
    sys.modules["PIL.Image"] = image
    sys.modules["PIL.ExifTags"] = exif_tags
    sys.modules["PIL.TiffImagePlugin"] = tiff_image

    pymongo = types.ModuleType("pymongo")
    pymongo.collection = types.SimpleNamespace(Collection=object)
    pymongo.UpdateOne = object
    sys.modules["pymongo"] = pymongo

    google = types.ModuleType("google")
    genai = types.ModuleType("google.genai")

    class Client:
        pass

    class APIError(Exception):
        code = None

    genai.Client = Client
    genai.errors = types.SimpleNamespace(APIError=APIError)
    google.genai = genai
    sys.modules["google"] = google
    sys.modules["google.genai"] = genai

    chroma = types.ModuleType("utils.chroma")
    chroma.populate_db = Mock()
    sys.modules["utils.chroma"] = chroma

    prompt = types.ModuleType("utils.prompt")
    prompt.describe_image = Mock(
        return_value={"content": {"summary": "generated description"}}
    )
    sys.modules["utils.prompt"] = prompt


install_external_stubs()

from utils.io import index_paths
from utils import ingest


class IndexPathsTests(unittest.TestCase):
    def test_metadata_override_preserves_original_upload_filename(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            stored_path = Path(tmp_dir) / "hashed-name.mp4"
            stored_path.write_bytes(b"video bytes")
            normalized_path = str(stored_path.resolve()).replace("\\", "/")

            result = index_paths(
                [str(stored_path)],
                metadata_overrides={
                    normalized_path: {"file_name": "Original Upload.mov"}
                },
            )

        metadata = next(iter(result.values()))
        self.assertEqual(metadata["file_path"], normalized_path)
        self.assertEqual(metadata["file_name"], "Original Upload.mov")
        self.assertEqual(metadata["ext"], "mp4")
        self.assertIn("master_date", metadata["dates"])


class IngestUploadTests(unittest.TestCase):
    def config(self, update_existing_metadata=False):
        return ingest.IngestConfig(
            api_name="gemini",
            model_name="test-model",
            mongo_collection=object(),
            chroma_client=object(),
            genai_client=object(),
            update_existing_metadata=update_existing_metadata,
        )

    def test_duplicate_upload_does_not_overwrite_existing_metadata(self):
        config = self.config()
        entry_id = ingest.entry_id_for_file("filehash", config)
        existing = {
            entry_id: {
                "description": {"content": {"summary": "already described"}},
                "metadata": {
                    "file_hash": "filehash",
                    "file_path": "/canonical/original.jpg",
                    "file_name": "original.jpg",
                    "model_hash": ingest.model_hash(config.api_name, config.model_name),
                    "api_name": config.api_name,
                    "model_name": config.model_name,
                },
            }
        }
        folder_dict = {
            "filehash": {
                "file_hash": "filehash",
                "file_path": "/uploads/hash.jpg",
                "file_name": "uploaded.jpg",
            }
        }

        with (
            patch.object(ingest, "check_if_exists", return_value=(existing, [])),
            patch.object(ingest, "upsert_dict_objects") as upsert_mock,
            patch.object(ingest, "populate_db") as populate_chroma_mock,
        ):
            result = ingest.ingest_index(folder_dict, config)

        self.assertEqual(result.duplicate_existing_keys, [entry_id])
        self.assertEqual(result.metadata_updated_keys, [])
        self.assertEqual(
            result.descriptions[entry_id]["metadata"]["file_path"],
            "/canonical/original.jpg",
        )
        upsert_mock.assert_not_called()
        populate_chroma_mock.assert_not_called()

    def test_new_upload_metadata_description_and_chroma_are_populated(self):
        config = self.config()
        entry_id = ingest.entry_id_for_file("filehash", config)
        folder_dict = {
            "filehash": {
                "file_hash": "filehash",
                "file_path": "/uploads/hash.jpg",
                "file_name": "Original Upload.jpg",
            }
        }
        upserts = []

        def fake_upsert(objects, collection):
            upserts.append(objects)

        with (
            patch.object(ingest, "check_if_exists", return_value=({}, [entry_id])),
            patch.object(ingest, "upsert_dict_objects", side_effect=fake_upsert),
            patch.object(
                ingest,
                "describe_image",
                return_value={"content": {"summary": "new description"}},
            ),
            patch.object(ingest, "populate_db") as populate_chroma_mock,
        ):
            result = ingest.ingest_index(folder_dict, config)

        self.assertEqual(result.metadata_updated_keys, [entry_id])
        self.assertEqual(result.populated_keys, [entry_id])
        self.assertEqual(result.chroma_indexed_keys, [entry_id])
        self.assertEqual(len(upserts), 2)
        populate_chroma_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
