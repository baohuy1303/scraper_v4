# scraper_v4 — Plan

## Goal

Take a website URL and return a structured brand dossier useful for downstream image/video ad generation. Build on `scraper_v3.ipynb` (homepage-only, ~10–13s media+text harvest). Add a structured LLM extraction step. Keep speed close to v3.

Optimizing for **time-to-ship**. The "best-quality" architecture (multi-page crawl, citations, media classification, eval framework, etc.) is documented separately and intentionally deferred.

## Deliverable

`scraper_v4.ipynb` — new file. `scraper_v3.ipynb` stays frozen as the baseline so we can A/B compare.

## Bug fixes (only blocking / data-loss)

1. **Response body eviction** (`on_response`) — `asyncio.create_task(...)` fire-and-forget means in-flight response handlers can lose access to their bodies when the page/context closes. Observed losing 3 images on the Medium test.

   Fix: collect the spawned tasks into a `pending_responses: set[asyncio.Task]`, and `await asyncio.gather(*pending_responses, return_exceptions=True)` before the `async with AsyncCamoufox` block exits.

2. **"Target page closed" warning** — same root cause, same fix.

Nothing else is touched. The 5-worker queue, batch flush, adaptive scroll, skip-patterns, extension mapping, and extractors all stay as-is.

## New features

### Text mode flag

```python
text_mode: Literal["raw", "chunked"] = "raw"
```

- **`"raw"`** (default) — current behavior, `body.all_inner_texts()`. Result JSON has `text: list[str]`.
- **`"chunked"`** — single `page.evaluate` that walks the DOM, strips `<nav> <header> <footer> <script> <style>`, and splits on `<h1>/<h2>/<h3>` into `[{heading, level, text}, ...]`. Result JSON has `text_chunks` populated; `text` still gets a flattened representation for compatibility.

Both modes feed the LLM extractor downstream, so you can A/B which produces a better dossier at the end.

### LLM extraction (text → structured brand data)

```python
llm_extract: bool = True
```

- **Model:** `gpt-5.4-mini` (constant at top of cell — verify exact ID before first run).
- **Method:** OpenAI structured outputs (response_format=json_schema) with a strict schema. More reliable than free-form JSON.
- **Input:** the extracted `text` (raw or flattened chunks) + `metadata` (title, description, og tags).
- **Schema fields** (every field nullable so "missing" is explicit):
  - `company_name`
  - `what_they_do` — 1–2 sentence description
  - `products_services: list[{name, description, pricing?}]`
  - `pricing_summary` — free-text overview if available
  - `target_customer`
  - `value_prop`
  - `tone_voice` — descriptors (e.g. "playful, technical, premium")
  - `testimonials: list[{quote, attribution?}]`
  - `faqs: list[{question, answer}]`
  - `common_phrases: list[str]` — recurring marketing language
  - `competitors_mentioned: list[str]`
  - `calls_to_action: list[str]`
- **Concurrency:** kicked off as soon as text+metadata are ready, runs in parallel with media download drain. Effectively free latency-wise on most pages.
- **Result JSON gains:** `brand_data: {...}` plus `brand_data_meta: {model, latency_ms, input_tokens, output_tokens}` for cost tracking.

### Multimodal visual analysis flag

```python
multimodal: bool = False
```

- **Model:** `gpt-5.4-mini` (same as text path — one SDK, one key).
- **Input:** the full-page screenshot already taken at `session_dir/screenshot.png`.
- **Output schema:**
  - `visual_style_notes` — narrative description of the brand's visual aesthetic
  - `color_palette: list[str]` — hex codes the model observes as dominant
  - `tone_descriptors_visual: list[str]` — playful, corporate, minimal, etc.
  - `layout_style` — e.g. "modern SaaS landing", "editorial blog", "high-density e-com"
- **Concurrency:** runs in parallel with the text LLM call.
- **Result JSON gains:** `visual_data: {...}` and `visual_data_meta: {...}`. Both `null` when flag is off.

## Function signature

```python
async def scrape(
    url: str,
    timeout: int = 20000,
    headless: bool | Literal["virtual"] = True,
    text_mode: Literal["raw", "chunked"] = "raw",
    llm_extract: bool = True,
    multimodal: bool = False,
    camoufox_options: dict[str, Any] | None = None,
) -> dict[str, Any]
```

## Output JSON shape

```jsonc
{
  "url": "...",
  "timestamp": "...",
  "session_dir": "...",
  "images": [ ... ],          // unchanged from v3
  "videos": [ ... ],          // unchanged from v3
  "images_count": 7,
  "videos_count": 0,
  "total_count": 7,
  "text": [ ... ],            // raw list (raw mode) or flattened chunks (chunked mode)
  "text_chunks": [ ... ],     // populated only in chunked mode
  "metadata": { ... },
  "screenshot": "...",
  "brand_data": { ... },          // null if llm_extract=False
  "brand_data_meta": { ... },     // null if llm_extract=False
  "visual_data": { ... },         // null if multimodal=False
  "visual_data_meta": { ... }     // null if multimodal=False
}
```

## API keys (Colab style)

```python
import os
from google.colab import userdata  # Colab
os.environ["OPENAI_API_KEY"] = userdata.get("OPENAI_API_KEY")
```

Locally, set `OPENAI_API_KEY` in env / `.env`.

## Speed budget (target)

| Phase | Time |
|---|---|
| Browser nav + scroll + extract | ~10–13s (unchanged) |
| LLM text extraction (gpt-5.4-mini) | 2–4s, parallel with media-download drain |
| Multimodal visual (gpt-5.4-mini, optional) | 3–5s, parallel with text LLM |
| **Total wall time** | **~13–17s with `llm_extract=True`**, ~10–13s if both flags off |

Both LLM calls run concurrently with media drain via `asyncio.gather`, so they should not add full latency on top of the browser phase.

## Final notebook cell — quick benchmark

Loop over 3–4 representative URLs (Magic Hour, a Shopify-style store, a SaaS landing page, a content-heavy Medium-style page). Print a table: wall time, image/video counts, brand-data field-fill rate, raw-vs-chunked diff. Lets you decide which `text_mode` wins before further work.

## Explicitly out of scope (for v4)

- Multi-page crawl (about/pricing/products/etc.) — biggest quality lever, deferred.
- Citation/grounding per field.
- Media classification & ad-suitability ranking — humans will review the captured media for now.
- External signals (reviews sites, social, Wayback, Crunchbase).
- Eval framework with hand-labeled brands.
- HTTP-first fast path with browser fallback.
- Brand-level caching.
- Site-type routing with specialized schemas.

These are tracked in chat history as the "quality-optimized" architecture if/when we want to expand.

## Things to confirm before first run

1. **API key present** in Colab secrets (`OPENAI_API_KEY`).
