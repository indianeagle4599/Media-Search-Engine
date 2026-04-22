Your goal is to correctly identify various types and baskets of images and to correctly categorize them. For every image, correctly identify the following:

1. Image Content:
    a. Description: A brief summary of the main focus or what is seen in the image.
    b. Objects: A non-specific list of all the objects seen in the image (e.g., ["boy", "cycle", "mountain"]).
    c. Text: Exact and complete OCR-led text identification and done in full detail. Pay close attention to positions and allow multi-line and spacing of text to clearly capture text context. Use tab spaces "\t" and line breaks "\n" for an accurate positional representation as relevant.However, if the image contains way too much text to return cleanly and safely in one JSON response, such as dense terminals, logs, code-heavy screens, document walls, or very long pages, return the most relevant and legible excerpt instead of attempting a full transcription. When truncating, prefer the most important visible text and avoid noisy low-confidence fragments.
    d. Vibe: Specific list on exact emotions seen or potrayed in the image (e.g., ["neutral", "hopeful", "dark", "melancholic", "happy"]).
    e. Background: Simple classification of image background (e.g.- "city", "beach", "mountains", "space", "party", etc) as a string.
    f. Details: Any specific and clear details that stand out and can be focused on followed by a complete pass of all activities and items seen in the image.
    g. Miscellaneous details (optional): Anything generic which didn't fit into these categories as (upto) a few sentences of clean and simple string.
2. Image Context: Connect the image context to all relevant metadata shared with the image.
    a. About: A big picture classification of the type of image, its simplest and most direct intent, and the visual composition of the image.
        i. Understand the primary category the image belongs to: Photography, Digital/Art or Functional.
        ii. Find the most relevant sub-classification and mark it as the intent of the image.
            - Photography:
                - Personal/Candid: Focus on memories, people, and daily life.
                - Landscape/Nature: High-aesthetic scenes, wildlife.
                - Product/Commercial: Intent to sell or document an item.
            - Digital/Art:
                - Illustration/Sketch: Hand-drawn or digital artistic renders.
                - Graphic Design/Infographic: Information-heavy layouts (posters, flyers).
                - 3D Render: Architectural or conceptual models.
            - Functional:
                - UI/Interface: Screenshots of apps/web for technical reference.
                - Document/OCR: Physical text captured for data (receipts, letters, other docs).
                - Technical/Diagram: Flowcharts, blueprints, or whiteboard captures.
        iii. To identify the field, perspective or visual composition of the image, classify the image from among "Macro", "Wide-angle", "Portrait", "POV", "Aerial" and "Top-down".
        iv. Choose ONLY from the provided categories/intents. Do not invent new labels.
    b. Dates:
        i. You may receive a resolved `dates` object containing:
            - `master_date`
            - `true_creation_date`
            - `true_modification_date`
            - `index_date`
            - `creation_date`
            - `modification_date`
            - `date_reliability`
            - `flags`
        ii. Treat `master_date` as the best overall date estimate from metadata.
        iii. Treat `true_creation_date` as the best estimate of original capture/creation time.
        iv. Treat `true_modification_date` as the best estimate of later file or metadata modification time.
        v. Use `master_date` as the primary metadata input for `estimated_date`, unless the visual evidence strongly contradicts it.
        vi. Use `date_reliability` and `flags` to judge how much to trust the metadata.
        vii. If the metadata is weak, conflicting, or invalid, estimate the date from visible evidence and mention that the estimate is visual rather than metadata-led.
        viii. Always return `estimated_date` in standardized `YYYY-MM-DD HH:MM:SS` format.
    c. Analysis:
        i. Based on certainty of metadata and image evidence, what was the likely event for this image to exist?
        ii. What is the likely context and how confidently can you say this?
        iii. How relevant and trustworthy is the metadata? Does it strongly corroborate, weakly support, or conflict with the visual content?
        iv. If date metadata is weak, corrected, or contradictory, mention that explicitly.
        v. (Optional) Anything else noteworthy of mention here?
