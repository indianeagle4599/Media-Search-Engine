import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEM_API_KEY")

from google import genai
from google.genai import types


with open("images_root/60e7ae50-03dc-11f0-a387-437e2fb661fc.jpg", "rb") as f:
    image_bytes = f.read()

client = genai.Client(api_key=api_key)
response = client.models.generate_content(
    model="gemini-3-flash-preview",
    contents=[
        types.Part.from_bytes(
            data=image_bytes,
            mime_type="image/jpeg",
        ),
        "Caption this image.",
    ],
)

print(response.text)
