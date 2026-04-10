"""Media rendering helpers for the Streamlit UI."""

import base64
import html
import inspect
import io
import json
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
    max_size: tuple[int, int] | None,
    quality: int = 84,
    fit_square: bool = False,
) -> tuple[str, bytes]:
    image = load_image(file_path)
    if max_size:
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


def image_data_uri(
    file_path: str,
    max_size: tuple[int, int] | None,
    **kwargs,
) -> str:
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


@st.cache_data(show_spinner=False, max_entries=64)
def get_full_data_uri(file_path: str, modified_ns: int) -> str:
    return image_data_uri(
        file_path,
        max_size=None,
        quality=90,
    )


def fullscreen_image_markup(
    *,
    preview_src: str,
    full_src: str,
    file_name: str,
    element_id: str,
) -> str:
    safe_title = html.escape(file_name, quote=True)
    image_id = f"{element_id}-image"
    button_id = f"{element_id}-button"
    return f"""
    <style>
      .detail-image-card {{
        position: relative;
        overflow: hidden;
        border-radius: 1rem;
        background: rgba(128, 128, 128, 0.10);
      }}
      .detail-image-card img {{
        display: block;
        width: 100%;
        max-height: 70vh;
        object-fit: contain;
      }}
      .detail-image-card__preview {{
        position: absolute;
        right: 0.8rem;
        bottom: 0.8rem;
        padding: 0.35rem 0.7rem;
        border: 1px solid rgba(255, 255, 255, 0.34);
        border-radius: 999px;
        background: rgba(0, 0, 0, 0.62);
        color: #ffffff;
        font-size: 0.82rem;
        font-weight: 750;
        cursor: pointer;
      }}
      .detail-image-card__preview:hover {{
        background: rgba(0, 0, 0, 0.78);
      }}
    </style>
    <div class="detail-image-card">
      <img id="{html.escape(image_id, quote=True)}" src={json.dumps(preview_src)} alt="{safe_title}" title="{safe_title}">
      <button id="{html.escape(button_id, quote=True)}" class="detail-image-card__preview" type="button">
        Open full size
      </button>
    </div>
    <script>
      (() => {{
        const image = document.getElementById({json.dumps(image_id)});
        const button = document.getElementById({json.dumps(button_id)});
        const fullSrc = {json.dumps(full_src)};
        if (!image || !button) {{
          return;
        }}

        const requestFullscreen = async () => {{
          if (image.getAttribute("src") !== fullSrc) {{
            image.setAttribute("src", fullSrc);
          }}
          try {{
            if (image.requestFullscreen) {{
              await image.requestFullscreen();
              return;
            }}
            if (image.webkitRequestFullscreen) {{
              image.webkitRequestFullscreen();
              return;
            }}
            if (image.msRequestFullscreen) {{
              image.msRequestFullscreen();
              return;
            }}
          }} catch (error) {{
          }}
          window.open(fullSrc, "_blank", "noopener,noreferrer");
        }};

        button.addEventListener("click", requestFullscreen);
      }})();
    </script>
    """


def render_html_block(markup: str, *, height: int) -> None:
    html_fn = getattr(st, "html", None)
    if html_fn:
        try:
            parameters = inspect.signature(html_fn).parameters
        except (TypeError, ValueError):
            parameters = {}
        kwargs = {}
        if "unsafe_allow_javascript" in parameters:
            kwargs["unsafe_allow_javascript"] = True
        html_fn(markup, **kwargs)
        return

    component_html = getattr(getattr(getattr(st, "components", None), "v1", None), "html", None)
    if component_html:
        component_html(markup, height=height, scrolling=False)
        return

    st.markdown(markup, unsafe_allow_html=True)


def render_media(file_path: str, ext: str) -> None:
    path = Path(file_path)
    if not file_path or not path.is_file():
        st.warning("Media file was not found on disk.")
        st.caption(file_path or "No file path stored in metadata.")
        return

    if ext in IMAGE_EXTENSIONS:
        preview = get_preview_data_uri(str(path), path.stat().st_mtime_ns)
        full_preview = get_full_data_uri(str(path), path.stat().st_mtime_ns)
        render_html_block(
            fullscreen_image_markup(
                preview_src=preview,
                full_src=full_preview,
                file_name=path.name,
                element_id=f"detail-media-{path.stat().st_mtime_ns}",
            ),
            height=720,
        )
    elif ext in VIDEO_EXTENSIONS:
        st.video(str(path))
    else:
        st.info(f"Preview is not configured for .{ext} files.")
        st.caption(str(path))
