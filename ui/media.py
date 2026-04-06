"""Media rendering helpers for the Streamlit UI."""

import base64
import html
import io
from pathlib import Path

import streamlit as st
from PIL import Image, ImageOps

from ui.config import HEIF_EXTENSIONS, IMAGE_EXTENSIONS, VIDEO_EXTENSIONS


HEIF_REGISTERED = False
RESAMPLE = getattr(getattr(Image, "Resampling", Image), "LANCZOS")


def ensure_heif_registered() -> None:
    global HEIF_REGISTERED
    if HEIF_REGISTERED:
        return

    from pillow_heif import register_heif_opener

    register_heif_opener()
    HEIF_REGISTERED = True


def load_image(file_path: str) -> Image.Image:
    ext = Path(file_path).suffix.lstrip(".").lower()
    if ext in HEIF_EXTENSIONS:
        ensure_heif_registered()

    with Image.open(file_path) as image:
        image = ImageOps.exif_transpose(image)
        if image.mode == "P":
            image = image.convert("RGBA")
        elif image.mode not in {"RGB", "RGBA", "L", "LA"}:
            image = image.convert("RGB")
        return image.copy()


def image_to_encoded_image(
    file_path: str,
    max_size: tuple[int, int],
    quality: int = 84,
    fit_square: bool = False,
) -> tuple[str, bytes]:
    image = load_image(file_path)
    if fit_square:
        image = ImageOps.fit(image, max_size, method=RESAMPLE)
    else:
        image.thumbnail(max_size, RESAMPLE)

    buffer = io.BytesIO()
    if image.mode in {"RGBA", "LA"}:
        image.save(buffer, format="PNG")
        mime_type = "image/png"
    else:
        if image.mode != "RGB":
            image = image.convert("RGB")
        image.save(buffer, format="JPEG", quality=quality, optimize=True)
        mime_type = "image/jpeg"

    return mime_type, buffer.getvalue()


def image_data_uri(file_path: str, max_size: tuple[int, int], **kwargs) -> str:
    mime_type, payload = image_to_encoded_image(
        file_path,
        max_size=max_size,
        **kwargs,
    )
    encoded = base64.b64encode(payload).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


@st.cache_data(show_spinner=False, max_entries=512)
def get_thumbnail_data_uri(file_path: str, modified_ns: int) -> str:
    return image_data_uri(
        file_path,
        max_size=(560, 560),
        quality=78,
        fit_square=True,
    )


@st.cache_data(show_spinner=False, max_entries=128)
def get_preview_data_uri(file_path: str, modified_ns: int) -> str:
    return image_data_uri(
        file_path,
        max_size=(1600, 1200),
        quality=88,
    )


def render_media(file_path: str, ext: str) -> None:
    path = Path(file_path)
    if not file_path or not path.is_file():
        st.warning("Media file was not found on disk.")
        st.caption(file_path or "No file path stored in metadata.")
        return

    if ext in IMAGE_EXTENSIONS:
        preview = get_preview_data_uri(str(path), path.stat().st_mtime_ns)
        title = html.escape(path.name)
        st.markdown(
            f"""
            <div class="detail-image-card">
              <img src="{preview}" alt="{title}" title="{title}">
              <a class="detail-image-card__preview" href="{preview}" target="_blank">
                Open full preview
              </a>
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif ext in VIDEO_EXTENSIONS:
        st.video(str(path))
    else:
        st.info(f"Preview is not configured for .{ext} files.")
        st.caption(str(path))
