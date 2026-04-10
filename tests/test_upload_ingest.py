import copy
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
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
    image_ops = types.ModuleType("PIL.ImageOps")
    exif_tags = types.ModuleType("PIL.ExifTags")
    tiff_image = types.ModuleType("PIL.TiffImagePlugin")

    class IFDRational:
        numerator = 0
        denominator = 1

    image.Image = object
    image.LANCZOS = 1
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

    pymongo = types.ModuleType("pymongo")
    pymongo.collection = types.SimpleNamespace(Collection=object)
    pymongo.UpdateOne = object
    sys.modules["pymongo"] = pymongo

    streamlit = types.ModuleType("streamlit")
    streamlit.session_state = {}
    streamlit.cache_resource = lambda **kwargs: (lambda fn: fn)
    streamlit.cache_data = lambda **kwargs: (lambda fn: fn)
    streamlit.dialog = lambda *args, **kwargs: (lambda fn: fn)
    streamlit.components = types.SimpleNamespace(
        v1=types.SimpleNamespace(html=lambda *args, **kwargs: None)
    )
    sys.modules["streamlit"] = streamlit

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv

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
    genai.errors = types.SimpleNamespace(APIError=APIError)
    google.genai = genai
    sys.modules["google"] = google
    sys.modules["google.genai"] = genai

    chroma = types.ModuleType("utils.chroma")
    chroma.populate_db = Mock()
    chroma.get_chroma_client = Mock(return_value=object())
    chroma.delete_entry_ids = Mock()
    sys.modules["utils.chroma"] = chroma

    prompt = types.ModuleType("utils.prompt")
    prompt.describe_image = Mock(
        return_value={"content": {"summary": "generated description"}}
    )
    sys.modules["utils.prompt"] = prompt


install_external_stubs()

from utils.io import index_paths
from utils import ingest
from ui import app, components, data, gallery, media, upload


def get_values(value, parts):
    if not parts:
        return [value]
    if isinstance(value, list):
        values = []
        for item in value:
            values.extend(get_values(item, parts))
        return values
    if not isinstance(value, dict):
        return [None]
    key = parts[0]
    if key not in value:
        return [None]
    return get_values(value[key], parts[1:])


def matches_query(doc, query):
    for field, condition in (query or {}).items():
        values = get_values(doc, field.split("."))
        present_values = [value for value in values if value is not None]
        if isinstance(condition, dict):
            if "$exists" in condition and bool(present_values) != bool(condition["$exists"]):
                return False
            if "$ne" in condition and any(value == condition["$ne"] for value in values):
                return False
            if "$in" in condition and not any(
                value in condition["$in"] for value in present_values
            ):
                return False
        elif not any(value == condition for value in present_values):
            return False
    return True


class FakeCollection:
    def __init__(self, documents=None):
        self.documents = {
            doc["_id"]: copy.deepcopy(doc) for doc in (documents or [])
        }

    def find(self, query=None):
        return [
            copy.deepcopy(document)
            for document in self.documents.values()
            if matches_query(document, query or {})
        ]

    def delete_many(self, query):
        keys = [
            key
            for key, document in self.documents.items()
            if matches_query(document, query or {})
        ]
        for key in keys:
            self.documents.pop(key, None)


class UploadedFile:
    def __init__(self, name, payload):
        self.name = name
        self.payload = payload

    def getvalue(self):
        return self.payload


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


class IngestTests(unittest.TestCase):
    def config(self, *, with_analysis=False):
        return ingest.IngestConfig(
            api_name="gemini",
            model_name="test-model",
            mongo_collection=object(),
            chroma_client=object() if with_analysis else None,
            genai_client=object() if with_analysis else None,
            update_existing_metadata=False,
        )

    def test_metadata_only_ingest_updates_mongo_without_description_or_chroma(self):
        config = self.config(with_analysis=False)
        entry_id = ingest.entry_id_for_file("filehash", config)
        folder_dict = {
            "filehash": {
                "file_hash": "filehash",
                "file_path": "/uploads/hash.jpg",
                "file_name": "Upload.jpg",
            }
        }

        with (
            patch.object(ingest, "check_if_exists", return_value=({}, [entry_id])),
            patch.object(ingest, "upsert_dict_objects") as upsert_mock,
            patch.object(ingest, "populate_db") as populate_chroma_mock,
        ):
            result = ingest.ingest_index(folder_dict, config)

        self.assertEqual(result.metadata_updated_keys, [entry_id])
        self.assertEqual(result.populated_keys, [])
        self.assertEqual(result.chroma_indexed_keys, [])
        upsert_mock.assert_called_once()
        populate_chroma_mock.assert_not_called()

    def test_rate_limited_analysis_marks_remaining_entries_clearly(self):
        config = self.config(with_analysis=True)
        first_entry = ingest.entry_id_for_file("first", config)
        second_entry = ingest.entry_id_for_file("second", config)
        folder_dict = {
            "first": {"file_hash": "first", "file_path": "/uploads/first.jpg"},
            "second": {"file_hash": "second", "file_path": "/uploads/second.jpg"},
        }

        def quota_error(*args, **kwargs):
            raise ingest.genai.errors.APIError("quota", code=429)

        with (
            patch.object(
                ingest,
                "check_if_exists",
                return_value=({}, [first_entry, second_entry]),
            ),
            patch.object(ingest, "upsert_dict_objects"),
            patch.object(ingest, "describe_image", side_effect=quota_error),
            patch.object(ingest, "populate_db") as populate_chroma_mock,
        ):
            result = ingest.ingest_index(folder_dict, config)

        self.assertEqual(result.rate_limited_keys, [first_entry, second_entry])
        self.assertEqual(result.error_details[first_entry]["stage"], "description")
        self.assertEqual(
            result.error_details[first_entry]["reason"],
            "Gemini quota reached while generating descriptions.",
        )
        populate_chroma_mock.assert_not_called()

    def test_existing_description_without_chroma_marker_is_sent_to_chroma(self):
        config = self.config(with_analysis=True)
        entry_id = ingest.entry_id_for_file("filehash", config)
        folder_dict = {
            "filehash": {
                "file_hash": "filehash",
                "file_path": "/uploads/hash.jpg",
                "file_name": "Upload.jpg",
            }
        }
        existing = {
            entry_id: {
                "metadata": folder_dict["filehash"].copy(),
                "description": {"content": {"summary": "done"}},
            }
        }

        with (
            patch.object(ingest, "check_if_exists", return_value=(existing, [])),
            patch.object(ingest, "upsert_dict_objects") as upsert_mock,
            patch.object(ingest, "populate_db") as populate_chroma_mock,
        ):
            result = ingest.ingest_index(folder_dict, config)

        self.assertEqual(result.chroma_indexed_keys, [entry_id])
        populate_chroma_mock.assert_called_once()
        self.assertFalse(
            result.descriptions[entry_id].get("indexing", {}).get("chroma_indexed_at")
            is None
        )
        indexed_state = upsert_mock.call_args.kwargs["objects"][entry_id]
        self.assertIn("indexing.chroma_indexed_at", indexed_state)

    def test_existing_description_with_chroma_marker_is_not_reindexed(self):
        config = self.config(with_analysis=True)
        entry_id = ingest.entry_id_for_file("filehash", config)
        folder_dict = {
            "filehash": {
                "file_hash": "filehash",
                "file_path": "/uploads/hash.jpg",
                "file_name": "Upload.jpg",
            }
        }
        existing = {
            entry_id: {
                "metadata": folder_dict["filehash"].copy(),
                "description": {"content": {"summary": "done"}},
                "indexing": {"chroma_indexed_at": "2026-04-10T10:00:00+00:00"},
            }
        }

        with (
            patch.object(ingest, "check_if_exists", return_value=(existing, [])),
            patch.object(ingest, "upsert_dict_objects") as upsert_mock,
            patch.object(ingest, "populate_db") as populate_chroma_mock,
        ):
            result = ingest.ingest_index(folder_dict, config)

        self.assertEqual(result.chroma_indexed_keys, [])
        populate_chroma_mock.assert_not_called()
        upsert_mock.assert_not_called()


class UploadFlowTests(unittest.TestCase):
    def setUp(self):
        sys.modules["streamlit"].session_state.clear()

    def metadata_only_config(self):
        return ingest.IngestConfig(
            api_name="gemini",
            model_name="test-model",
            mongo_collection=object(),
            chroma_client=None,
            genai_client=None,
            update_existing_metadata=False,
        )

    def test_classify_uploaded_files_detects_duplicates_before_commit(self):
        first_hash = upload.hash_bytes(b"first")
        second_hash = upload.hash_bytes(b"second")

        def fake_existing(file_hash):
            if file_hash == second_hash:
                return {"_id": "existing-entry"}
            return None

        with patch.object(upload, "get_uploaded_entry_by_hash", side_effect=fake_existing):
            selections, duplicate_count = upload.classify_uploaded_files(
                [
                    UploadedFile("one.jpg", b"first"),
                    UploadedFile("one-copy.jpg", b"first"),
                    UploadedFile("two.jpg", b"second"),
                ]
            )

        self.assertEqual(duplicate_count, 1)
        self.assertEqual([selection["file_hash"] for selection in selections], [first_hash, second_hash])
        self.assertEqual(selections[1]["default_action"], upload.ACTION_IGNORE)

    def test_duplicate_ignore_skips_db_reset_and_new_ingest(self):
        selection = {
            "file_hash": "hash-one",
            "payload": b"image-bytes",
            "original_filename": "Photo.jpg",
            "existing_entry": {"_id": "existing-entry"},
            "default_action": upload.ACTION_IGNORE,
        }

        with (
            patch.object(upload, "build_ingest_config", return_value=self.metadata_only_config()),
            patch.object(upload, "ingest_files") as ingest_mock,
            patch.object(upload, "delete_existing_upload") as delete_mock,
        ):
            rows = upload.store_selected_uploads([selection], {})

        self.assertEqual(rows[0]["status"], upload.STATUS_IGNORED)
        self.assertEqual(rows[0]["entry_id"], "existing-entry")
        ingest_mock.assert_not_called()
        delete_mock.assert_not_called()

    def test_reupload_deletes_old_mongo_and_chroma_state_then_creates_fresh_metadata_doc(self):
        old_document = {
            "_id": "old-entry",
            "metadata": {
                "file_hash": "hash-one",
                "file_path": "/uploads/old.jpg",
                "file_name": "Old.jpg",
            },
        }
        collection = FakeCollection([old_document])
        selection = {
            "file_hash": "hash-one",
            "payload": b"image-bytes",
            "original_filename": "New.jpg",
            "existing_entry": {"_id": "old-entry"},
            "default_action": upload.ACTION_IGNORE,
        }
        config = self.metadata_only_config()
        seen_at = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        chroma_client = object()

        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch.object(upload, "get_mongo_collection", return_value=collection),
            patch.object(upload, "get_chroma_client", return_value=chroma_client),
            patch.object(upload, "delete_entry_ids") as delete_chroma_mock,
            patch.object(upload, "get_upload_root", return_value=Path(tmp_dir)),
            patch.object(upload, "build_ingest_config", return_value=config),
            patch.object(upload, "ingest_files") as ingest_mock,
        ):
            rows = upload.store_selected_uploads(
                [selection],
                {"hash-one": upload.ACTION_REUPLOAD},
                seen_at=seen_at,
            )

        self.assertEqual(rows[0]["status"], upload.STATUS_REUPLOADED)
        self.assertEqual(collection.documents, {})
        delete_chroma_mock.assert_called_once_with(chroma_client, ["old-entry"])
        _, kwargs = ingest_mock.call_args
        overrides = kwargs["metadata_overrides"]
        override = next(iter(overrides.values()))
        self.assertEqual(override["file_name"], "New.jpg")
        self.assertEqual(override["uploaded_at"], seen_at.astimezone().isoformat())

    def test_pending_uploads_include_missing_description_and_missing_chroma(self):
        with patch.object(
            upload,
            "list_uploaded_entries",
            return_value=[
                {
                    "_id": "pending",
                    "metadata": {
                        "file_hash": "one",
                        "file_path": "/uploads/one.jpg",
                        "file_name": "one.jpg",
                        "uploaded_at": "2026-04-08T12:00:00+00:00",
                    },
                    "description": {},
                },
                {
                    "_id": "pending-chroma",
                    "metadata": {
                        "file_hash": "two",
                        "file_path": "/uploads/two.jpg",
                        "file_name": "two.jpg",
                        "uploaded_at": "2026-04-08T12:01:00+00:00",
                    },
                    "description": {"content": {"summary": "done"}},
                },
                {
                    "_id": "indexed",
                    "metadata": {
                        "file_hash": "three",
                        "file_path": "/uploads/three.jpg",
                        "file_name": "three.jpg",
                        "uploaded_at": "2026-04-08T12:02:00+00:00",
                    },
                    "description": {"content": {"summary": "done"}},
                    "indexing": {
                        "chroma_indexed_at": "2026-04-08T12:03:00+00:00"
                    },
                },
            ],
        ):
            pending = upload.pending_upload_entries()

        self.assertEqual(
            [entry["_id"] for entry in pending],
            ["pending", "pending-chroma"],
        )

    def test_retry_pending_uploads_after_quota_returns(self):
        pending_entry = {
            "_id": "pending-entry",
            "metadata": {
                "file_hash": "hash-one",
                "file_path": "/uploads/pending.jpg",
                "file_name": "Pending.jpg",
                "uploaded_at": "2026-04-08T12:00:00+00:00",
            },
            "description": {},
        }
        config = ingest.IngestConfig(
            api_name="gemini",
            model_name="test-model",
            mongo_collection=object(),
            chroma_client=object(),
            genai_client=object(),
            update_existing_metadata=False,
        )

        with (
            patch.object(upload, "pending_upload_entries", return_value=[pending_entry]),
            patch.object(upload, "build_ingest_config", return_value=config),
            patch.object(
                upload,
                "ingest_files",
                return_value=SimpleNamespace(
                    chroma_indexed_keys=[],
                    rate_limited_keys=["pending-entry"],
                    failed_keys=[],
                    error_details={
                        "pending-entry": {
                            "reason": "Gemini quota reached while generating descriptions."
                        }
                    },
                ),
            ),
        ):
            rate_limited = upload.analyze_pending_uploads()

        self.assertEqual(rate_limited[0]["status"], upload.STATUS_RATE_LIMITED)

        with (
            patch.object(upload, "pending_upload_entries", return_value=[pending_entry]),
            patch.object(upload, "build_ingest_config", return_value=config),
            patch.object(
                upload,
                "ingest_files",
                return_value=SimpleNamespace(
                    chroma_indexed_keys=["pending-entry"],
                    rate_limited_keys=[],
                    failed_keys=[],
                    error_details={},
                ),
            ),
        ):
            indexed = upload.analyze_pending_uploads()

        self.assertEqual(indexed[0]["status"], upload.STATUS_INDEXED)


class GalleryTests(unittest.TestCase):
    def make_entries(self):
        return [
            {
                "_id": "a",
                "metadata": {
                    "file_hash": "a",
                    "file_path": "/uploads/a.jpg",
                    "file_name": "A.jpg",
                    "ext": "jpg",
                    "uploaded_at": "2026-04-10T08:00:00+00:00",
                    "dates": {"true_creation_date": "2022-01-01T00:00:00+00:00"},
                },
                "description": {"content": {"summary": "done"}},
                "indexing": {"chroma_indexed_at": "2026-04-10T08:05:00+00:00"},
            },
            {
                "_id": "b",
                "metadata": {
                    "file_hash": "b",
                    "file_path": "/uploads/b.jpg",
                    "file_name": "B.jpg",
                    "ext": "jpg",
                    "uploaded_at": "2026-04-08T08:00:00+00:00",
                    "dates": {"true_creation_date": "2024-06-01T00:00:00+00:00"},
                },
                "description": {"content": {"summary": "done"}},
                "indexing": {"chroma_indexed_at": "2026-04-08T08:05:00+00:00"},
            },
            {
                "_id": "c",
                "metadata": {
                    "file_hash": "c",
                    "file_path": "/uploads/c.jpg",
                    "file_name": "C.jpg",
                    "ext": "jpg",
                    "uploaded_at": "2026-04-09T08:00:00+00:00",
                    "dates": {},
                },
                "description": {},
            },
        ]

    def test_gallery_sorts_by_upload_date(self):
        with patch.object(gallery, "list_uploaded_entries", return_value=self.make_entries()):
            records = gallery.get_gallery_records(limit=3, sort_by=gallery.DEFAULT_SORT)

        self.assertEqual([record["_id"] for record in records], ["a", "c", "b"])

    def test_gallery_sorts_by_creation_date_with_missing_values_last(self):
        with patch.object(gallery, "list_uploaded_entries", return_value=self.make_entries()):
            records = gallery.get_gallery_records(
                limit=3,
                sort_by="Creation date (newest first)",
            )

        self.assertEqual([record["_id"] for record in records], ["b", "a", "c"])
        self.assertEqual(records[2]["status"], "pending_indexing")

    def test_gallery_metadata_markup_uses_labeled_rows(self):
        markup = gallery.gallery_metadata_markup(
            {
                "status": "indexed",
                "uploaded_at": "2026-04-08T17:25:20+00:00",
                "creation_date": "2020-08-20T00:00:00+00:00",
            }
        )

        self.assertIn("Status", markup)
        self.assertIn("Uploaded", markup)
        self.assertIn("Created", markup)
        self.assertIn("Indexed", markup)
        self.assertNotIn(" · ", markup)


class DetailStateTests(unittest.TestCase):
    def setUp(self):
        components.st.session_state.clear()

    def test_description_presence_drives_shared_detail_state(self):
        self.assertTrue(
            data.entry_has_description(
                {"description": {"content": {"summary": "done"}}}
            )
        )
        self.assertFalse(data.entry_has_description({"description": {}}))
        self.assertTrue(
            data.entry_is_fully_indexed(
                {
                    "description": {"content": {"summary": "done"}},
                    "indexing": {"chroma_indexed_at": "2026-04-10T10:00:00+00:00"},
                }
            )
        )
        self.assertFalse(
            data.entry_is_fully_indexed(
                {"description": {"content": {"summary": "done"}}}
            )
        )

    def test_result_preview_click_uses_session_modal_state(self):
        def click_button(*args, **kwargs):
            callback = kwargs.get("on_click")
            if callback:
                callback(*(kwargs.get("args") or ()))
            return True

        with (
            patch.object(
                components.st,
                "button",
                side_effect=click_button,
                create=True,
            ) as button_mock,
            patch.object(components.st, "markdown", create=True),
        ):
            components.render_result_preview_card(
                file_path="",
                file_name="A.jpg",
                ext="txt",
                detail_entry_id="entry-1",
                detail_title="A.jpg",
            )

        self.assertEqual(components.get_selected_entry_id(), "entry-1")
        self.assertEqual(button_mock.call_args.args[0], "Details")
        self.assertEqual(
            button_mock.call_args.kwargs["key"],
            components.detail_trigger_key("entry-1"),
        )

    def test_search_result_cards_pass_entry_ids_to_shared_trigger(self):
        with patch.object(components, "render_media_card") as render_media_card:
            components.render_result_card(
                entry_id="entry-2",
                entry={
                    "metadata": {
                        "file_path": "/uploads/b.jpg",
                        "file_name": "B.jpg",
                        "ext": "jpg",
                    }
                },
                rank=2,
                score=0.9,
            )

        self.assertEqual(
            render_media_card.call_args.kwargs["detail_entry_id"],
            "entry-2",
        )

    def test_gallery_cards_pass_entry_ids_to_shared_trigger(self):
        class DummyColumn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with (
            patch.object(
                gallery.st,
                "columns",
                return_value=[DummyColumn()],
                create=True,
            ),
            patch.object(gallery, "render_media_card") as render_media_card,
        ):
            gallery.render_gallery_grid(
                [
                    {
                        "_id": "entry-3",
                        "file_path": "/uploads/c.jpg",
                        "file_name": "C.jpg",
                        "ext": "jpg",
                        "status": "indexed",
                    }
                ],
                columns=1,
            )

        self.assertEqual(
            render_media_card.call_args.kwargs["detail_entry_id"],
            "entry-3",
        )
        self.assertIn(
            "gallery-card__meta",
            render_media_card.call_args.kwargs["overlay_details_html"],
        )


class SearchControlsTests(unittest.TestCase):
    def setUp(self):
        app.st.session_state.clear()
        app.st.session_state["top_n"] = 10

    def test_enter_submission_path_is_reserved_for_search(self):
        class DummyContext:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def make_columns(spec, **kwargs):
            return [DummyContext() for _ in spec]

        with (
            patch.object(app.st, "columns", side_effect=make_columns, create=True) as columns_mock,
            patch.object(app.st, "form", return_value=DummyContext(), create=True),
            patch.object(app.st, "text_input", create=True),
            patch.object(
                app.st,
                "form_submit_button",
                return_value=False,
                create=True,
            ) as submit_mock,
            patch.object(app.st, "markdown", create=True),
            patch.object(app, "active_filters_from_state", return_value={}),
            patch.object(app, "filters_are_active", return_value=False),
            patch.object(app, "search_history_dialog") as history_dialog_mock,
            patch.object(app, "search_settings_dialog") as settings_dialog_mock,
        ):
            submitted = app.render_search_controls()

        self.assertFalse(submitted)
        self.assertEqual(
            columns_mock.call_args_list[1].args[0],
            [5.6, 0.72, 0.56, 0.56],
        )
        self.assertEqual(
            [call.kwargs["key"] for call in submit_mock.call_args_list],
            ["search_submit", "search_history", "search_configure"],
        )
        history_dialog_mock.assert_not_called()
        settings_dialog_mock.assert_not_called()


class MediaTests(unittest.TestCase):
    def test_fullscreen_markup_requests_browser_fullscreen(self):
        markup = media.fullscreen_image_markup(
            preview_src="data:image/jpeg;base64,preview",
            full_src="data:image/jpeg;base64,full",
            file_name="A.jpg",
            element_id="detail-media-1",
        )

        self.assertIn("Open full size", markup)
        self.assertIn("requestFullscreen", markup)
        self.assertIn("window.open(fullSrc", markup)


if __name__ == "__main__":
    unittest.main()
