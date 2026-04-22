<h1 style="display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.2rem;">
  <img src="assets/AfterSight%20Logo.png" alt="AfterSight logo" width="64" />
  <span>AfterSight</span>
</h1>

<p style="margin-top: 0; margin-bottom: 0.6rem;">
  <strong>Image-first media search with metadata-aware retrieval.</strong>
</p>

<p style="margin-top: 0;">
  AfterSight turns a folder of local media into a structured search surface.
</p>

AfterSight fingerprints files, reconciles on-disk + EXIF timestamps, asks Google Gemini for scene-level JSON descriptions for supported images, and loads both metadata and generated text into Mongo and Chroma.

By splitting narrative, lexical, OCR, and absolute fields into their own indexes—and fusing results with Reciprocal Rank Fusion—the project gives agentic or RAG systems a way to ask semantically nuanced questions (“show me candid, wide‑angle beach shots captured near sunrise from last winter”) and actually get grounded answers.

## File System
- **Indexer (`utils/io.py`)** — Walk `images_root/`, hash each asset, normalize paths, extract whitelisted EXIF/HEIF tags, and emit a `dates` block with reliability flags.
- **Captioner (`utils/prompt.py`)** — Assemble the batch prompt from the manifest-driven `/prompts/common`, `/prompts/batch`, and `/prompts/describe_image` prompt assets, send Gemini requests for supported images, and enforce the `content/context` schema.
- **Persistence (`utils/mongo.py`)** — Store each `<file_hash>_<model_hash>` entry with metadata + description, dedupe by probing Mongo first, and upsert only what’s missing.
- **Vector Store (`utils/chroma.py`)** — Split descriptions into narrative, lexical, OCR, and absolute fields; feed each into its own Chroma collection with field-appropriate embeddings (MiniLM vs. Ollama `mxbai-embed-large`), keeping prior vectors unless `overwrite=True`.
- **Retrieval (`query_all_collections`)** — Clean queries, reuse cached embeddings, query every populated collection, and fuse results with Reciprocal Rank Fusion (weighted toward narrative fields).
- **Ingestion (`utils/ingest.py`)** — Reusable orchestration for folder and uploaded-file ingestion, covering metadata sync, missing descriptions, and Chroma population.
- **Evaluation hook (`eval_retrieval.py`)** — Stub for retrieval evaluation once you have gold sets.

## Configuration
Create a `.env` with:

```
GEM_API_KEY=<Google AI Studio key>
MONGO_URL=mongodb://<user>:<pass>@host:port
MONGO_DB_NAME=<db>
MONGO_COLLECTION_NAME=<collection>
CHROMA_URL=<path to persistent chroma store>
REPO_ROOT=<absolute repo path>  # prompt assembly
```

Optional overrides:

```
MEDIA_INDEX_ROOT=images_root
MEDIA_API_NAME=gemini
MEDIA_MODEL_NAME=gemini-2.5-flash-lite
MEDIA_UPLOAD_ROOT=images_root/uploads
MEDIA_UPLOAD_COLLECTION=media_uploads
CHROMA_EMBEDDING_PROCESSES=4
CHROMA_EMBEDDING_MIN_DOCS=24
CHROMA_EMBEDDING_BATCH_SIZE=32
```

## Run the Pipeline
1. **Install deps** – `pip install -r requirements.txt` (Ollama must expose `mxbai-embed-large` locally if you use the default Chroma config).
2. **Run ingestion** – `python main.py` runs the sync flow defined in `main.py` (default `images_root_test`), then backfills missing descriptions and Chroma entries.
3. **Query** – use the Streamlit UI or call `query_all_collections` directly from `utils.chroma`.

## Run the Streamlit Query UI
After MongoDB and the persistent Chroma store are populated, launch:

```
streamlit run streamlit_app.py
```

Enter a free-text query, choose the number of results to return, and the app will render ranked media previews from the stored `metadata.file_path` values. The UI includes Search, Upload, Gallery, and ChromaDB pages. Uploads are a 3-step flow: store files, describe stored images, then index described entries. Stored uploads go under `MEDIA_UPLOAD_ROOT/<YYYYMMDD>/`. The Gallery page shows deduplicated uploaded images with sorting and random sampling.

## Retrieval Overview
- Queries can be strings or token lists; they’re lowercased/de-duped before embedding.
- Embeddings are cached per model to avoid recomputation during the run.
- Every collection returns its own ranked list; scores are fused with RRF using per-collection weights (narrative > lexical > OCR > metadata).
- Top hits are mapped back to Mongo entries so you can display the Gemini summaries or any other metadata you’ve stored.
