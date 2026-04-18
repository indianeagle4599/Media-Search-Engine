"""
config.py

Shared UI constants and CSS for the Streamlit AfterSight interface.
"""

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
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@700;800&display=swap');

.block-container {
  max-width: 1200px;
  padding-top: 0.9rem;
  padding-bottom: 3rem;
}
.app-header {
  margin: 0 auto 0.62rem;
  padding: 2.04rem 1.28rem 0.02rem;
  border: 1px solid rgba(128, 128, 128, 0.18);
  border-radius: 1.2rem;
  background:
    radial-gradient(circle at top left, rgba(99, 102, 241, 0.18), transparent 32rem),
    linear-gradient(135deg, rgba(255, 255, 255, 0.045), rgba(255, 255, 255, 0.01));
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
}
.brandbar {
  display: inline-flex;
  align-items: center;
  gap: 0.1rem;
}
.brandbar__logo {
  width: clamp(3.6rem, 5.6vw, 4.8rem);
  height: auto;
  flex: 0 0 auto;
  display: block;
}
.brandmark {
  margin: 0;
  display: inline-block;
  gap: 0;
  font-family: "Poppins", "Avenir Next", "Segoe UI", sans-serif;
  font-size: clamp(2.36rem, 3.84vw, 3.36rem);
  font-weight: 800;
  letter-spacing: -0.052em;
  line-height: 1.1;
  padding-top: 0.08em;
  padding-bottom: 0.1em;
  overflow: visible;
  text-rendering: geometricPrecision;
}
.brandmark__after {
  color: rgba(248, 248, 251, 0.98);
}
.brandmark__sight {
  background: linear-gradient(
    138deg,
    #1f4fff 0%,
    #315dff 16%,
    #4b68ff 30%,
    #6a66ff 46%,
    #8b5cf6 62%,
    #9851ff 78%,
    #b65cff 100%
  );
  background-size: 480% 480%;
  background-repeat: no-repeat;
  -webkit-background-clip: text;
  background-clip: text;
  -webkit-text-fill-color: transparent;
  text-shadow: 0 0 22px rgba(124, 58, 237, 0.12);
  animation: aftersight-sight-glow 6.8s linear infinite;
}
@keyframes aftersight-sight-glow {
  0% {
    background-position: 82% 16%;
    filter: brightness(0.985) saturate(0.985);
  }
  32% {
    background-position: 64% 32%;
    filter: brightness(1) saturate(1);
  }
  50% {
    background-position: 48% 48%;
    filter: brightness(1.045) saturate(1.045);
  }
  68% {
    background-position: 30% 66%;
    filter: brightness(1.005) saturate(1.005);
  }
  100% {
    background-position: 82% 16%;
    filter: brightness(0.985) saturate(0.985);
  }
}
.muted {
  color: rgba(128, 128, 128, 0.98);
}
.search-toolbar-status {
  margin: 0;
  display: flex;
  justify-content: flex-start;
  align-items: center;
  min-height: 1.98rem;
  padding-left: 0.84rem;
}
.search-status {
  display: inline-flex;
  align-items: center;
  gap: 0.36rem;
  font-size: 0.77rem;
  line-height: 1;
  color: rgba(143, 149, 162, 0.96);
  white-space: nowrap;
}
.search-status__dot {
  width: 0.42rem;
  height: 0.42rem;
  border-radius: 999px;
  box-shadow: 0 0 0 0.12rem rgba(255, 255, 255, 0.06);
}
.search-status--loaded .search-status__dot {
  background: #34c759;
}
.search-status--offloaded .search-status__dot {
  background: #9ca3af;
}
.search-status--unavailable .search-status__dot {
  background: #ef4444;
}
div[data-testid="stForm"] {
  border: 1px solid rgba(128, 128, 128, 0.18);
  border-radius: 1.08rem;
  margin-top: -0.18rem;
  padding: 0.68rem 0.92rem 0.64rem;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.02), rgba(255, 255, 255, 0.01)),
    rgba(128, 128, 128, 0.045);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
}
div[data-testid="stForm"] div[data-testid="stTextInput"] {
  margin-bottom: 0.1rem;
}
div[data-testid="stForm"] div[data-testid="stTextInput"] input {
  border: none;
  background: transparent;
  font-size: 1.05rem;
  padding: 0.58rem 0.3rem 0.56rem 0.88rem;
}
div[data-testid="stForm"] div[data-testid="stTextInput"] > div {
  border: 1px solid rgba(128, 128, 128, 0.1);
  border-radius: 0.88rem;
  background: rgba(255, 255, 255, 0.045);
  box-shadow: none;
}
div[data-testid="stForm"] div[data-testid="stHorizontalBlock"] {
  align-items: center;
  gap: 0.12rem;
  margin-top: 0;
  padding-top: 0.04rem;
  border-top: none;
}
.st-key-search_submit,
.st-key-search_clear,
.st-key-search_history,
.st-key-search_configure {
  align-self: center;
}
.st-key-search_submit,
.st-key-search_clear,
.st-key-search_history,
.st-key-search_configure {
  margin-left: 0 !important;
  margin-right: 0 !important;
}
.st-key-search_submit button,
.st-key-search_clear button,
.st-key-search_history button,
.st-key-search_configure button {
  min-height: 2.08rem;
  height: 2.08rem;
  border-radius: 0.78rem;
  padding: 0 0.6rem;
  font-size: 0.88rem;
  line-height: 1;
  box-shadow: none;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  transition:
    border-color 140ms ease,
    background 140ms ease,
    transform 140ms ease;
}
.st-key-search_submit button p,
.st-key-search_clear button p,
.st-key-search_history button p,
.st-key-search_configure button p {
  margin: 0 !important;
  line-height: 1 !important;
}
.st-key-search_clear button,
.st-key-search_history button,
.st-key-search_configure button {
  width: 2.08rem;
  border: 1px solid rgba(128, 128, 128, 0.2) !important;
  background: rgba(128, 128, 128, 0.08) !important;
}
.st-key-search_submit button {
  min-width: 2.32rem;
  border: 1px solid rgba(99, 102, 241, 0.28) !important;
  background: linear-gradient(
    135deg,
    rgba(99, 102, 241, 0.96),
    rgba(79, 70, 229, 0.88)
  ) !important;
  color: #ffffff !important;
}
.st-key-search_submit button:hover,
.st-key-search_clear button:hover,
.st-key-search_history button:hover,
.st-key-search_configure button:hover {
  transform: translateY(-1px);
}
.empty-state {
  margin: 0.12rem 0 0;
  text-align: center;
  font-size: 0.88rem;
}
.st-key-page {
  margin-bottom: -0.08rem;
}
.results-summary {
  margin: 0;
  color: rgba(231, 233, 237, 0.95);
  font-size: 0.9rem;
  line-height: 1.25;
  min-height: 2rem;
  display: flex;
  align-items: center;
}
.results-view-label {
  margin: 0;
  color: rgba(231, 233, 237, 0.95);
  font-size: 0.9rem;
  line-height: 1;
  min-height: 2rem;
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding-right: 0.08rem;
}
.st-key-search_results_view {
  margin-top: 0;
  margin-bottom: 0.08rem;
}
.st-key-search_results_view [data-testid="stRadio"] {
  margin: 0;
}
.st-key-search_results_view div[role="radiogroup"] {
  min-height: 2rem;
  align-items: center;
  justify-content: flex-start;
  flex-wrap: nowrap;
  gap: 0.48rem;
}
.st-key-search_results_view label {
  white-space: nowrap;
  margin-bottom: 0 !important;
}
.result-list-divider {
  height: 1px;
  margin: 0.42rem 0 0.62rem;
  background: rgba(255, 255, 255, 0.12);
}
@media (max-width: 1180px) {
  .results-view-label {
    justify-content: flex-start;
    min-height: auto;
    padding-top: 0.08rem;
  }
  .st-key-search_results_view div[role="radiogroup"] {
    flex-direction: column;
    align-items: flex-start;
    justify-content: flex-start;
    gap: 0.32rem;
  }
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
.result-card__overlay-meta {
  font-size: 0.76rem;
  line-height: 1.35;
  color: rgba(255, 255, 255, 0.9);
}
.result-card:hover .result-card__rank {
  opacity: 1;
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
