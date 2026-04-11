"""UI configuration constants."""

DEFAULT_TOP_N = 10
GRID_COLUMNS = 4
CHROMA_VIEWER_DEFAULT_LIMIT = 100
FILTERED_SEARCH_MULTIPLIER = 5
MAX_FILTERED_CANDIDATES = 100
SEARCH_HISTORY_LIMIT = 30
SEARCH_HISTORY_COLLECTION = "media_search_history"
UPLOAD_ROOT = "images_root/uploads"

IMAGE_EXTENSIONS = {
    "avif",
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
  max-width: 1200px;
  padding-top: 0.9rem;
  padding-bottom: 3rem;
}
.app-header {
  margin: 0 auto 0.7rem;
  padding: 0.95rem 1.15rem 1rem;
  border: 1px solid rgba(128, 128, 128, 0.18);
  border-radius: 1.2rem;
  background:
    radial-gradient(circle at top left, rgba(99, 102, 241, 0.18), transparent 32rem),
    linear-gradient(135deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.01));
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
}
.app-header h1 {
  margin: 0;
  letter-spacing: -0.035em;
}
.muted {
  color: rgba(128, 128, 128, 0.98);
}
.search-hint {
  margin: 0.5rem 0 0.95rem;
  text-align: center;
  font-size: 0.86rem;
}
div[data-testid="stForm"] {
  border: 1px solid rgba(128, 128, 128, 0.18);
  border-radius: 1rem;
  padding: 0.55rem 0.74rem 0.58rem;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.02), rgba(255, 255, 255, 0.01)),
    rgba(128, 128, 128, 0.045);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
}
div[data-testid="stForm"] div[data-testid="stTextInput"] {
  margin-bottom: 0.05rem;
}
div[data-testid="stForm"] div[data-testid="stTextInput"] input {
  border: none;
  background: transparent;
  font-size: 1rem;
  padding: 0.5rem 0.3rem 0.45rem 0.72rem;
}
div[data-testid="stForm"] div[data-testid="stTextInput"] > div {
  border: none;
  background: transparent;
  box-shadow: none;
}
div[data-testid="stForm"] div[data-testid="stHorizontalBlock"] {
  align-items: end;
  gap: 0.38rem;
}
.st-key-search_submit,
.st-key-search_history,
.st-key-search_configure {
  align-self: end;
}
.st-key-search_history {
  order: 1;
}
.st-key-search_configure {
  order: 2;
}
.st-key-search_submit {
  order: 3;
}
.st-key-search_submit button,
.st-key-search_history button,
.st-key-search_configure button {
  min-height: 2.18rem;
  height: 2.18rem;
  border-radius: 0.8rem;
  padding: 0 0.72rem;
  font-size: 0.98rem;
  line-height: 1;
  box-shadow: none;
  transition:
    border-color 140ms ease,
    background 140ms ease,
    transform 140ms ease;
}
.st-key-search_history button,
.st-key-search_configure button {
  width: 2.18rem;
  border: 1px solid rgba(128, 128, 128, 0.2) !important;
  background: rgba(128, 128, 128, 0.08) !important;
}
.st-key-search_submit button {
  min-width: 2.5rem;
  border: 1px solid rgba(99, 102, 241, 0.28) !important;
  background: linear-gradient(
    135deg,
    rgba(99, 102, 241, 0.96),
    rgba(79, 70, 229, 0.88)
  ) !important;
  color: #ffffff !important;
}
.st-key-search_submit button:hover,
.st-key-search_history button:hover,
.st-key-search_configure button:hover {
  transform: translateY(-1px);
}
.empty-state {
  margin: 1.15rem 0 0;
  text-align: center;
  font-size: 0.95rem;
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
[class*="st-key-result_card_detail_trigger_"] {
  display: flex;
  justify-content: center;
  margin-top: 0.5rem;
}
[class*="st-key-result_card_detail_trigger_"] button {
  min-width: 6.5rem !important;
  padding: 0.32rem 0.9rem;
  border: 1px solid rgba(128, 128, 128, 0.22) !important;
  border-radius: 999px;
  background: rgba(12, 16, 24, 0.62) !important;
  color: #ffffff !important;
  font-size: 0.82rem;
  font-weight: 700;
  line-height: 1;
  box-shadow: none;
}
[class*="st-key-result_card_detail_trigger_"] button:hover {
  background: rgba(12, 16, 24, 0.82) !important;
}
.result-card__rank {
  position: absolute;
  top: 0.8rem;
  left: 0.8rem;
  padding: 0.24rem 0.48rem;
  border-radius: 999px;
  background: rgba(0, 0, 0, 0.64);
  color: #ffffff;
  font-size: 0.78rem;
  font-weight: 700;
  opacity: 0;
  transition: opacity 140ms ease;
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
.result-card__overlay {
  position: absolute;
  left: 0.7rem;
  right: 0.7rem;
  bottom: 0.7rem;
  padding: 0.5rem 0.6rem 0.55rem;
  border-radius: 0.75rem;
  background: rgba(0, 0, 0, 0.68);
  color: #ffffff;
  opacity: 0;
  transition: opacity 140ms ease;
}
.result-card:hover .result-card__overlay {
  opacity: 1;
}
.result-card__overlay-title {
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 0.82rem;
  font-weight: 700;
  white-space: nowrap;
}
.result-card__overlay-divider {
  height: 1px;
  margin: 0.4rem 0 0.35rem;
  background: rgba(255, 255, 255, 0.14);
}
.result-card:hover .result-card__rank {
  opacity: 1;
}
.result-card__caption {
  padding: 0.5rem 0.2rem 0.15rem;
  color: rgba(128, 128, 128, 0.98);
  font-size: 0.8rem;
  line-height: 1.35;
}
.gallery-card__meta {
  display: grid;
  gap: 0.12rem;
}
.gallery-card__meta-row {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.75rem;
  padding: 0;
  font-size: 0.74rem;
  line-height: 1.35;
}
.gallery-card__meta-label {
  color: rgba(196, 200, 208, 0.88);
  font-weight: 700;
}
.gallery-card__meta-value {
  color: rgba(255, 255, 255, 0.96);
  text-align: right;
}
.result-card--placeholder {
  padding: 0;
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
[class*="st-key-uploaded_rename_submit_"] button,
[class*="st-key-detail_close_action_"] button,
[class*="st-key-uploaded_delete_prompt_"] button,
[class*="st-key-uploaded_delete_cancel_"] button,
[class*="st-key-uploaded_delete_confirm_action_"] button {
  min-height: 2.35rem;
  border-radius: 0.85rem;
}
[class*="st-key-uploaded_delete_prompt_"] button,
[class*="st-key-uploaded_delete_confirm_action_"] button {
  border: 1px solid rgba(239, 68, 68, 0.42) !important;
  background: linear-gradient(
    135deg,
    rgba(220, 38, 38, 0.96),
    rgba(185, 28, 28, 0.92)
  ) !important;
  color: #ffffff !important;
}
[class*="st-key-uploaded_delete_prompt_"] button:hover,
[class*="st-key-uploaded_delete_confirm_action_"] button:hover {
  border-color: rgba(248, 113, 113, 0.55) !important;
  background: linear-gradient(
    135deg,
    rgba(239, 68, 68, 0.96),
    rgba(185, 28, 28, 0.94)
  ) !important;
}
</style>
"""
