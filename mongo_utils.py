"""
mongo_utils.py

Contains utilities and adapters to connect to, search in and update mongoDB collections.
"""

import os, pymongo


def find_dict_objects(
    objects: list | dict | str,
    collection: pymongo.synchronous.collection.Collection,
    batch_size: int = 2048,
) -> dict:
    if isinstance(objects, dict):
        object_keys = list(objects.keys())
    elif isinstance(objects, list):
        object_keys = objects
    elif isinstance(objects, str):
        object_keys = [
            objects,
        ]

    n = 0
    result_dict = {}
    while n < len(object_keys):
        find_from = object_keys[n : n + batch_size]
        n += batch_size
        result = {
            doc.pop("_id"): doc for doc in collection.find({"_id": {"$in": find_from}})
        }
        result_dict.update(result)
    return result_dict


def upsert_dict_objects(
    objects: dict,
    collection: pymongo.synchronous.collection.Collection,
    batch_size: int = 2048,
) -> None:
    updates = []
    for key, value in objects.items():
        filter_dict = {"_id": key}
        update_dict = {"$set": value}
        updates.append(
            pymongo.UpdateOne(filter=filter_dict, update=update_dict, upsert=True)
        )
        if len(updates) >= batch_size:
            collection.bulk_write(updates)
            updates = []

    if updates:
        collection.bulk_write(updates)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    MONGO_URL = os.getenv("MONGO_URL")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

    collection = pymongo.MongoClient(MONGO_URL)[MONGO_DB_NAME]["test"]

    one_key = "3d5c982af3cfc9d80710b2e9a79596450a8a020a68378343e4a018651019477c_aa0a2eab8c654ad30a64336aac6481331db75779"
    many_keys = [
        "cf34fd8681457f1464e22b4f0d101e268f8a4fd486bb1878ceddcb71efd1f41e_aa0a2eab8c654ad30a64336aac6481331db75779",
        "3194da00b5fd4a81b7073864c02d97df09e8aaa9a36c954af5e533ead8963698_aa0a2eab8c654ad30a64336aac6481331db75779",
        "5b9307779d9baadc29ec73a9b6dcfa8a1b50433c2b520ecbb02d9a98a9c04fcf_aa0a2eab8c654ad30a64336aac6481331db75779",
        "e69ec4fe45f61b51efd8ce2506071e3d2bd3a983c0ccd1ca30346b45cb0bc5f7_aa0a2eab8c654ad30a64336aac6481331db75779",
        "570f0a2571f0815c68e22f50410faf29c1e7e3f25f771c2aac97c68a65303f23_aa0a2eab8c654ad30a64336aac6481331db75779",
    ]
    print(find_dict_objects(objects=one_key, collection=collection))
    print(find_dict_objects(objects=many_keys, collection=collection))

    one_dict = {
        "3d5c982af3cfc9d80710b2e9a79596450a8a020a68378343e4a018651019477c_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "3d5c982af3cfc9d80710b2e9a79596450a8a020a68378343e4a018651019477c",
                "file_name": "00D63E95-128B-4E88-B7D2-9240DCF67056.HEIC",
                "media_type": "image",
                "ext": "heic",
                "is_compat": True,
                "creation_date": "2026-02-06 13:57:08.517637",
                "modification_date": "2019-03-23 15:56:00",
                "index_date": "2026-03-20 13:18:31.383663",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\00D63E95-128B-4E88-B7D2-9240DCF67056.HEIC",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
    }
    many_dicts = {
        "cf34fd8681457f1464e22b4f0d101e268f8a4fd486bb1878ceddcb71efd1f41e_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "cf34fd8681457f1464e22b4f0d101e268f8a4fd486bb1878ceddcb71efd1f41e",
                "file_name": "0DE1CE7B-C9C2-4940-BFA2-EA0F4C28985B.HEIC",
                "media_type": "image",
                "ext": "heic",
                "is_compat": True,
                "creation_date": "2026-02-06 13:57:16.686991",
                "modification_date": "2019-03-23 15:57:56",
                "index_date": "2026-03-20 13:18:31.383663",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\0DE1CE7B-C9C2-4940-BFA2-EA0F4C28985B.HEIC",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "3194da00b5fd4a81b7073864c02d97df09e8aaa9a36c954af5e533ead8963698_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "3194da00b5fd4a81b7073864c02d97df09e8aaa9a36c954af5e533ead8963698",
                "file_name": "2013-12-01 13.15.27.png",
                "media_type": "image",
                "ext": "png",
                "is_compat": True,
                "creation_date": "2026-02-06 13:50:43.719836",
                "modification_date": "2014-04-22 23:39:36.857920",
                "index_date": "2026-03-20 13:18:31.383663",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\2013-12-01 13.15.27.png",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "5b9307779d9baadc29ec73a9b6dcfa8a1b50433c2b520ecbb02d9a98a9c04fcf_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "5b9307779d9baadc29ec73a9b6dcfa8a1b50433c2b520ecbb02d9a98a9c04fcf",
                "file_name": "20141227_152100_36061.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-06 13:51:58.131537",
                "modification_date": "2017-06-24 13:42:30",
                "index_date": "2026-03-20 13:18:31.390852",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\20141227_152100_36061.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "e69ec4fe45f61b51efd8ce2506071e3d2bd3a983c0ccd1ca30346b45cb0bc5f7_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "e69ec4fe45f61b51efd8ce2506071e3d2bd3a983c0ccd1ca30346b45cb0bc5f7",
                "file_name": "60e7ae50-03dc-11f0-a387-437e2fb661fc.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-01 17:13:36.277562",
                "modification_date": "2026-02-01 17:13:38.258370",
                "index_date": "2026-03-20 13:18:31.390852",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\60e7ae50-03dc-11f0-a387-437e2fb661fc.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "570f0a2571f0815c68e22f50410faf29c1e7e3f25f771c2aac97c68a65303f23_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "570f0a2571f0815c68e22f50410faf29c1e7e3f25f771c2aac97c68a65303f23",
                "file_name": "757ca6497cbbe61c6ab895d8c7eb85e592435a93.jpeg",
                "media_type": "image",
                "ext": "jpeg",
                "is_compat": True,
                "creation_date": "2026-02-01 17:16:34.035137",
                "modification_date": "2026-02-01 17:16:36.292171",
                "index_date": "2026-03-20 13:18:31.390852",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\757ca6497cbbe61c6ab895d8c7eb85e592435a93.jpeg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "06d1fbafa5b88d9e73d6e987eee950a23d8f56cff26e632e3e0edaad5ab46993_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "06d1fbafa5b88d9e73d6e987eee950a23d8f56cff26e632e3e0edaad5ab46993",
                "file_name": "citations in google docs.png",
                "media_type": "image",
                "ext": "png",
                "is_compat": True,
                "creation_date": "2026-02-01 17:16:11.312204",
                "modification_date": "2026-02-01 17:16:13.266353",
                "index_date": "2026-03-20 13:18:31.390852",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\citations in google docs.png",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "4476ae9a7bde1aceb67c30631ec18dfbfe3c9cd79a8651f3d8053c8a05caddf3_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "4476ae9a7bde1aceb67c30631ec18dfbfe3c9cd79a8651f3d8053c8a05caddf3",
                "file_name": "Design.png",
                "media_type": "image",
                "ext": "png",
                "is_compat": True,
                "creation_date": "2026-03-14 18:40:02.913699",
                "modification_date": "2026-03-14 18:40:02.943239",
                "index_date": "2026-03-20 13:18:31.391852",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\Design.png",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "91617166c798ef9f0319584d7ae4395813205832ec367d26cbe2adb14f1070b5_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "91617166c798ef9f0319584d7ae4395813205832ec367d26cbe2adb14f1070b5",
                "file_name": "DSC02989.JPG",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-06 12:33:43.181072",
                "modification_date": "2025-06-16 08:33:04",
                "index_date": "2026-03-20 13:18:31.392859",
                "extracted_metadata": {
                    "Exif": {
                        "DateTimeOriginal": "2025:06:04 10:56:57",
                        "DateTimeDigitized": "2025:06:04 10:56:57",
                        "Flash": 16,
                        "FocalLength": 50.0,
                        "UserComment": "",
                        "ExifImageWidth": 1920,
                        "SceneCaptureType": 0,
                        "OffsetTime": "+05:30",
                        "OffsetTimeOriginal": "+05:30",
                        "OffsetTimeDigitized": "+05:30",
                        "SubsecTime": "083",
                        "SubsecTimeOriginal": "083",
                        "SubsecTimeDigitized": "083",
                        "ExifImageHeight": 1280,
                        "ExposureTime": 0.004,
                        "FNumber": 1.4,
                        "SceneType": "\u0001",
                        "ISOSpeedRatings": 125,
                        "LensModel": "50mm F1.4 DG DN | Art 023",
                        "DigitalZoomRatio": 1.0,
                    },
                    "Make": "SONY",
                    "Model": "ILCE-7M4",
                    "Software": "ILCE-7M4 v4.00",
                    "Orientation": 1,
                    "DateTime": "2025:06:04 10:56:57",
                    "XResolution": 350.0,
                    "YResolution": 350.0,
                },
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\DSC02989.JPG",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "840cfdebd4d993bfdbcb1930339c19172ab7f85ee465e8a6adc4267c4873e747_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "840cfdebd4d993bfdbcb1930339c19172ab7f85ee465e8a6adc4267c4873e747",
                "file_name": "images.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-01 17:13:08.755840",
                "modification_date": "2026-02-01 17:13:11.431686",
                "index_date": "2026-03-20 13:18:31.393857",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\images.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "92fd45854e97c7df1923ab1c0c82b948432a407ceab307da27fe8bfbd6b7b785_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "92fd45854e97c7df1923ab1c0c82b948432a407ceab307da27fe8bfbd6b7b785",
                "file_name": "img.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-06 12:48:50.168392",
                "modification_date": "2026-02-06 12:49:00.044719",
                "index_date": "2026-03-20 13:18:31.394858",
                "extracted_metadata": {
                    "GPSInfo": {
                        "GPSDateStamp": "2020:03:16",
                        "GPSTimeStamp": [8.0, 58.0, 25.0],
                    },
                    "Exif": {
                        "ShutterSpeedValue": 10.304,
                        "DateTimeOriginal": "2020:03:16 14:28:26",
                        "DateTimeDigitized": "2002:12:08 12:00:00",
                        "ApertureValue": 2.27,
                        "ExifImageWidth": 2448,
                        "Flash": 16,
                        "FocalLength": 2.93,
                        "ExifImageHeight": 3264,
                        "ExposureTime": 0.0007905138339920949,
                        "FNumber": 2.2,
                        "ISOSpeedRatings": 100,
                    },
                    "Make": "OPPO",
                    "Model": "A37fw",
                    "XResolution": 72.0,
                    "YResolution": 72.0,
                },
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\img.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "0ff13a088ac4ac90eda2a59d5af423552dc25f04bab93f116c72e9ba4e614c53_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "0ff13a088ac4ac90eda2a59d5af423552dc25f04bab93f116c72e9ba4e614c53",
                "file_name": "istockphoto-1550071750-612x612.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-01 17:12:15.414582",
                "modification_date": "2026-02-01 17:12:45.821755",
                "index_date": "2026-03-20 13:18:31.395855",
                "extracted_metadata": {
                    "XResolution": 300.0,
                    "YResolution": 300.0,
                    "ImageDescription": "Green tea tree leaves camellia sinensis in organic farm sunlight. Fresh young tender bud herbal farm on summer morning. Sunlight Green tea tree plant. Close up Tree tea plant green nature in morning",
                },
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\istockphoto-1550071750-612x612.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "aff668a824d94749f7f2721ab4677d76e9334896de4f689a7c08086851c07511_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "aff668a824d94749f7f2721ab4677d76e9334896de4f689a7c08086851c07511",
                "file_name": "monarch-beautiful-butterflygraphy-beautiful-butterfly-on-flower-macrography-beautyful-nature-photo.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-01 17:13:43.369420",
                "modification_date": "2026-02-01 17:13:46.151376",
                "index_date": "2026-03-20 13:18:31.395855",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\monarch-beautiful-butterflygraphy-beautiful-butterfly-on-flower-macrography-beautyful-nature-photo.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "9696307adcf00b4796fb227519fb57c2a22e8f4b31d84cc8b27499556629b55e_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "9696307adcf00b4796fb227519fb57c2a22e8f4b31d84cc8b27499556629b55e",
                "file_name": "outstanding-nature-photos-reddit-featured.jpg",
                "media_type": "image",
                "ext": "jpg",
                "is_compat": True,
                "creation_date": "2026-02-01 17:14:14.773129",
                "modification_date": "2026-02-01 17:14:16.930329",
                "index_date": "2026-03-20 13:18:31.395855",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\outstanding-nature-photos-reddit-featured.jpg",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
        "4304511c03ffeb5c54abfd59d59764511658f6f380c490577f3f88ef6d72f52a_aa0a2eab8c654ad30a64336aac6481331db75779": {
            "description": {"LOL BITCH": "Nope B)"},
            "metadata": {
                "file_hash": "4304511c03ffeb5c54abfd59d59764511658f6f380c490577f3f88ef6d72f52a",
                "file_name": "word-random-text.png",
                "media_type": "image",
                "ext": "png",
                "is_compat": True,
                "creation_date": "2026-02-01 17:16:00.293548",
                "modification_date": "2026-02-01 17:16:02.854309",
                "index_date": "2026-03-20 13:18:31.396858",
                "extracted_metadata": {},
                "file_path": "C:\\Users\\hites\\Desktop\\Media Search Engine\\images_root\\word-random-text.png",
                "model_hash": "aa0a2eab8c654ad30a64336aac6481331db75779",
                "api_name": "gemini",
                "model_name": "gemini-2.5-flash-lite",
            },
        },
    }
    upsert_dict_objects(objects=one_dict, collection=collection)
    upsert_dict_objects(objects=many_dicts, collection=collection)
