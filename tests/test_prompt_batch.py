import importlib, json, os, sys, tempfile, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tests.stub_modules import install_prompt_test_stubs


REPO_ROOT = Path(__file__).resolve().parents[1]


install_prompt_test_stubs()
os.environ["REPO_ROOT"] = str(REPO_ROOT)
sys.modules.pop("utils.prompt", None)
prompt = importlib.import_module("utils.prompt")


class PromptBatchTests(unittest.TestCase):
    def image_metadata(self, file_path: str, file_name: str) -> dict:
        return {
            "file_path": file_path,
            "file_name": file_name,
            "media_type": "image",
            "ext": "jpg",
            "mime_type": "image/jpeg",
            "is_compat": True,
            "model_name": "test-model",
            "dates": {"master_date": "2024-01-02T03:04:05+00:00"},
        }

    def test_dummy_mode_returns_entry_keyed_descriptions(self):
        batch_entries = [
            {
                "entry_id": "first",
                "metadata": self.image_metadata("/tmp/first.jpg", "First.jpg"),
            },
            {
                "entry_id": "second",
                "metadata": self.image_metadata("/tmp/second.jpg", "Second.jpg"),
            },
        ]

        result = prompt.describe_image_batch(
            client=None,
            batch_entries=batch_entries,
            use_dummy_descriptions=True,
        )

        self.assertEqual(set(result), {"first", "second"})
        self.assertIn("First.jpg", result["first"]["content"]["summary"])
        self.assertEqual(result["second"]["context"]["event"], "Dummy validation run")
        self.assertEqual(result["first"]["context"]["estimated_date"], "")

    def test_live_mode_builds_batched_request_and_parses_results(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            first_path = Path(tmp_dir) / "first.jpg"
            second_path = Path(tmp_dir) / "second.jpg"
            first_path.write_bytes(b"first-image")
            second_path.write_bytes(b"second-image")

            batch_entries = [
                {
                    "entry_id": "first",
                    "metadata": self.image_metadata(str(first_path), "First.jpg"),
                },
                {
                    "entry_id": "second",
                    "metadata": self.image_metadata(str(second_path), "Second.jpg"),
                },
            ]
            response = SimpleNamespace(
                text=json.dumps(
                    {
                        "results": [
                            {
                                "entry_id": "first",
                                "description": {
                                    "content": {"summary": "one"},
                                    "context": {},
                                },
                            },
                            {
                                "entry_id": "second",
                                "description": {
                                    "content": {"summary": "two"},
                                    "context": {},
                                },
                            },
                        ]
                    }
                )
            )
            generate_content = Mock(return_value=response)
            client = SimpleNamespace(
                models=SimpleNamespace(generate_content=generate_content)
            )

            with patch.object(
                prompt,
                "get_analysis_image_bytes",
                side_effect=lambda file_path, mime_type="", **kwargs: (
                    Path(file_path).read_bytes(),
                    mime_type or "image/jpeg",
                ),
            ):
                result = prompt.describe_image_batch(
                    client=client,
                    batch_entries=batch_entries,
                )

        self.assertEqual(
            result,
            {
                "first": {"content": {"summary": "one"}, "context": {}},
                "second": {"content": {"summary": "two"}, "context": {}},
            },
        )
        call_kwargs = generate_content.call_args.kwargs
        self.assertEqual(call_kwargs["model"], "test-model")
        self.assertEqual(len(call_kwargs["contents"]), 5)
        self.assertIn("BATCH OVERVIEW", call_kwargs["contents"][0])
        self.assertIn("entry_id: first", call_kwargs["contents"][1])
        self.assertEqual(call_kwargs["contents"][2]["mime_type"], "image/jpeg")
        self.assertIn("entry_id: second", call_kwargs["contents"][3])
        self.assertEqual(call_kwargs["contents"][4]["mime_type"], "image/jpeg")

    def test_live_mode_passes_resize_bounds_to_analysis_proxy_loader(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            image_path = Path(tmp_dir) / "large.jpg"
            image_path.write_bytes(b"original-image")

            batch_entries = [
                {
                    "entry_id": "large",
                    "metadata": self.image_metadata(str(image_path), "Large.jpg"),
                }
            ]
            response = SimpleNamespace(
                text=json.dumps(
                    {
                        "results": [
                            {
                                "entry_id": "large",
                                "description": {
                                    "content": {"summary": "large"},
                                    "context": {},
                                },
                            }
                        ]
                    }
                )
            )
            generate_content = Mock(return_value=response)
            client = SimpleNamespace(
                models=SimpleNamespace(generate_content=generate_content)
            )

            with patch.object(
                prompt,
                "get_analysis_image_bytes",
                return_value=(b"proxy-image", "image/jpeg"),
            ) as proxy_mock:
                prompt.describe_image_batch(
                    client=client,
                    batch_entries=batch_entries,
                    analysis_image_max_width=1000,
                    analysis_image_max_height=1000,
                )

        proxy_mock.assert_called_once_with(
            str(image_path),
            mime_type="image/jpeg",
            max_width=1000,
            max_height=1000,
        )
        self.assertEqual(
            generate_content.call_args.kwargs["contents"][2]["data"],
            b"proxy-image",
        )


if __name__ == "__main__":
    unittest.main()
