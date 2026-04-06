# Media Search Engine

Media Search Engine turns a folder of photos/videos into a structured search surface. It fingerprints every file, reconciles on-disk + EXIF timestamps, asks Google Gemini for scene-level JSON descriptions, and loads both metadata and generated text into Mongo and Chroma.

By splitting narrative, lexical, OCR, and absolute fields into their own indexes—and fusing results with Reciprocal Rank Fusion—the project gives agentic or RAG systems a way to ask semantically nuanced questions (“show me candid, wide‑angle beach shots captured near sunrise from last winter”) and actually get grounded answers.

## File System
- **Indexer (`utils/io.py`)** — Walk `images_root/`, hash each asset, normalize paths, extract whitelisted EXIF/HEIF tags, and emit a `dates` block with reliability flags.
- **Captioner (`utils/prompt.py`)** — Assemble `/prompts/describe_image` into Gemini requests for every compatible asset (JPEG/PNG/WebP/HEIC + major video codecs) and enforce the `content/context` schema.
- **Persistence (`utils/mongo.py`)** — Store each `<file_hash>_<model_hash>` entry with metadata + description, dedupe by probing Mongo first, and upsert only what’s missing.
- **Vector Store (`utils/chroma.py`)** — Split descriptions into narrative, lexical, OCR, and absolute fields; feed each into its own Chroma collection with field-appropriate embeddings (MiniLM vs. Ollama `mxbai-embed-large`), keeping prior vectors unless `overwrite=True`.
- **Retrieval (`query_all_collections`)** — Clean queries, reuse cached embeddings, query every populated collection, and fuse results with Reciprocal Rank Fusion (weighted toward narrative fields). `main.py` prints top‑k summaries per query for quick smoke tests.
- **Evaluation hook (`eval_retrieval.py`)** — Loads scenarios from `json_outs/eval_retrieval.json`; fill `get_retrieval`/`evaluate_chroma` to track latency vs. accuracy once you have gold sets.

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

Override defaults (e.g., `images_root`, Gemini model, sample queries) inside `main.py` before running.

## Run the Pipeline
1. **Install deps** – `pip install -r requirements.txt` (Ollama must expose `mxbai-embed-large` locally if you use the default Chroma config).
2. **Index media** – `index_folder("images_root")` fingerprints every asset and resolves timestamps.
3. **Sync Mongo** – `fetch_existing` + `update_metadata` pull prior entries, ensure metadata matches the latest schema, and tell you what still needs descriptions.
4. **Describe gaps** – `populate_missing` streams the remaining assets through Gemini and upserts each batch immediately.
5. **Populate Chroma** – `populate_db` scatters the structured content into per-field collections without clobbering existing vectors.
6. **Query** – adjust `query_texts` in `main.py` and inspect the printed top‑k summaries to validate relevance.

## Run the Streamlit Query UI
After MongoDB and the persistent Chroma store are populated, launch:

```
streamlit run streamlit_app.py
```

Enter a free-text query, choose the number of results to return, and the app will render ranked media previews from the stored `metadata.file_path` values.

## Retrieval Model in One Breath
- Queries can be strings or token lists; they’re lowercased/de-duped before embedding.
- Embeddings are cached per model to avoid recomputation during the run.
- Every collection returns its own ranked list; scores are fused with RRF using per-collection weights (narrative > lexical > OCR > metadata).
- Top hits are mapped back to Mongo entries so you can display the Gemini summaries or any other metadata you’ve stored.

