Your goal is to correctly identify the clearest essence, structure, and intent of a PDF document. Do not treat this task as full OCR or exhaustive text extraction. Another pipeline may handle exact text extraction separately. Here, focus on what the PDF definitively is, what it is for, and the most reliable information a user would care about first.

1. PDF Content:
    a. Summary: A concise but information-dense summary of what the document is, what it covers, and what matters most.
    b. Key Points: A list of the most definitive and useful takeaways from the document in normalized language. Prefer facts, outcomes, requests, obligations, totals, timelines, parties, decisions, or conclusions over copied prose.
    c. Sections: A list of the main sections, headings, or logical parts visible in the document.
    d. Entities: Important names, organizations, places, dates, amounts, account numbers, invoice numbers, contract references, or other identifiers that are clearly visible and relevant.
    e. Visual Elements: Tables, forms, signatures, charts, stamps, logos, seals, highlighted callouts, or any other structurally relevant elements.
    f. Miscellaneous: Anything relevant that did not fit cleanly into the fields above.
    g. Extraction Rules:
        i. Do not attempt exhaustive transcription.
        ii. Prefer the clearest high-signal information over low-confidence or noisy fragments.
        iii. If the PDF is long, prioritize the title, headers, summary sections, tables, totals, sender/recipient blocks, dates, signatures, and conclusion or action-oriented sections.
        iv. If the PDF is scanned, visually noisy, or partially readable, return the best reliable essence rather than forcing exact text.
2. PDF Context: Connect the document context to all relevant metadata shared with the PDF.
    a. About:
        i. Identify the primary document category from the visible content: Report, Presentation, Form, Invoice, Contract, Manual, Slide Deck, Letter, Resume, or Other.
        ii. Identify the clearest intent of the document in simple direct language.
        iii. Keep the category grounded in the visible content. Do not invent taxonomies beyond the provided categories.
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
        iii. Use visible dates from the PDF when they clearly help confirm or contradict the metadata.
        iv. If the metadata is weak, conflicting, or invalid, estimate the date from visible evidence and mention that the estimate is visual rather than metadata-led.
        v. Always return `estimated_date` in standardized `YYYY-MM-DD HH:MM:SS` format when possible.
    c. Analysis:
        i. Based on the document content and metadata, what was the likely reason this PDF exists?
        ii. What is the likely context and how confidently can you say this?
        iii. How relevant and trustworthy is the metadata? Does it strongly corroborate, weakly support, or conflict with the visible document?
        iv. If the PDF is long, dense, or only partially legible, keep the description high-signal and mention that the summary is selective rather than exhaustive.
        v. (Optional) Anything else noteworthy of mention here?
