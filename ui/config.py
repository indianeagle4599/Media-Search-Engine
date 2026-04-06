"""UI configuration constants."""

DEFAULT_TOP_N = 10
GRID_COLUMNS = 4
CHROMA_VIEWER_DEFAULT_LIMIT = 100

IMAGE_EXTENSIONS = {
    "jpg",
    "jpeg",
    "png",
    "webp",
    "bmp",
    "tif",
    "tiff",
    "heic",
    "heif",
}
VIDEO_EXTENSIONS = {"mp4", "mpeg", "mov", "avi", "mpg", "webm", "wmv", "3gpp"}
HEIF_EXTENSIONS = {"heic", "heif"}

APP_CSS = """
<style>
.block-container {
  max-width: 1240px;
  padding-top: 1.35rem;
  padding-bottom: 3.5rem;
}
.hero {
  margin: 0.15rem auto 1rem;
  padding: 1rem 1.15rem;
  border: 1px solid rgba(128, 128, 128, 0.18);
  border-radius: 1.15rem;
  background:
    radial-gradient(circle at top left, rgba(99, 102, 241, 0.18), transparent 32rem),
    linear-gradient(135deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.01));
}
.hero h1 {
  margin-bottom: 0.25rem;
  letter-spacing: -0.035em;
}
.hero p,
.muted {
  color: rgba(128, 128, 128, 0.98);
}
.search-hint {
  margin: 0.25rem 0 1.2rem;
  text-align: center;
  font-size: 0.86rem;
}
.result-card {
  position: relative;
  overflow: hidden;
  padding: 0.35rem;
  border: 1px solid rgba(128, 128, 128, 0.14);
  border-radius: 1.2rem;
  background: rgba(128, 128, 128, 0.055);
  transition:
    border-color 140ms ease,
    transform 140ms ease,
    background 140ms ease;
}
.result-card:hover {
  border-color: rgba(99, 102, 241, 0.42);
  background: rgba(99, 102, 241, 0.08);
  transform: translateY(-1px);
}
.result-card img {
  width: 100%;
  aspect-ratio: 1 / 1;
  object-fit: cover;
  border-radius: 0.9rem;
  display: block;
}
.result-card__title {
  position: absolute;
  left: 0.7rem;
  right: 0.7rem;
  bottom: 0.7rem;
  padding: 0.4rem 0.55rem;
  border-radius: 0.75rem;
  background: rgba(0, 0, 0, 0.64);
  color: #ffffff;
  font-size: 0.82rem;
  font-weight: 700;
  opacity: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  transition: opacity 140ms ease;
  white-space: nowrap;
}
.result-card:hover .result-card__title {
  opacity: 1;
}
.result-placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  aspect-ratio: 1 / 1;
  padding: 1rem;
  border-radius: 1rem;
  background: rgba(128, 128, 128, 0.10);
  color: rgba(128, 128, 128, 0.98);
  text-align: center;
}
.detail-path {
  overflow-wrap: anywhere;
  color: rgba(128, 128, 128, 0.98);
  font-size: 0.85rem;
}
.detail-image-card {
  position: relative;
  overflow: hidden;
  border-radius: 1rem;
  background: rgba(128, 128, 128, 0.10);
}
.detail-image-card img {
  display: block;
  width: 100%;
  max-height: 70vh;
  object-fit: contain;
}
.detail-image-card__preview {
  position: absolute;
  right: 0.8rem;
  bottom: 0.8rem;
  padding: 0.35rem 0.7rem;
  border: 1px solid rgba(255, 255, 255, 0.34);
  border-radius: 999px;
  background: rgba(0, 0, 0, 0.62);
  color: #ffffff !important;
  font-size: 0.82rem;
  font-weight: 750;
  opacity: 0;
  text-decoration: none;
  transition: opacity 140ms ease;
}
.detail-image-card:hover .detail-image-card__preview {
  opacity: 1;
}
</style>
"""
