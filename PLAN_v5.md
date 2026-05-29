# scraper_v5 — Plan

## Goal

Layer **multi-page crawl**, **multimodal-default**, and a **video-gen-shaped output schema** on top of v4. Match (and exceed) what Creatify/Higgsfield extract for downstream ad/video generation.

v4 is the baseline — frozen. v5 keeps the same skeleton (Camoufox + response-listener media capture + LLM extraction) and adds the three big quality levers.

## Deliverable

`scraper_v5.ipynb` — new file. v3 and v4 stay frozen as baselines.

## Small bug carry-over from v4

`text_mode` accepts `"raw"` or `"chunked"`. Python doesn't enforce `Literal` at runtime, so a typo like `"chunk"` silently fell through to raw on a recent run. Add a 2-line runtime validator at the top of `scrape()` that raises `ValueError` on bad values.

## 1. Multi-page crawl (homepage + 4 pages)

### Discovery
1. **Try `sitemap.xml`** first — fast, often complete. Fetch via `httpx` outside the browser (parallel to homepage nav). Parse for `<loc>` URLs on the same domain.
2. **Fall back to nav-link extraction** — single `page.evaluate` on the homepage that returns `[{href, text}]` from anchors inside `<nav>`, `<header>`, top-level menus, and the footer.
3. **LLM rank+pick** — cheap `gpt-5.4-mini` call with the candidate URL list and their anchor text, prompt: *"pick the top 4 most likely to contain pricing, products, testimonials, about, FAQ, or company info."* Returns ordered list.

### Crawling
- Open 4 secondary pages in parallel using `browser.new_page()` inside the same Camoufox context (shares cookies/fingerprint).
- Each page (homepage + 4): `goto` + adaptive scroll + text extraction + **full-page screenshot**. All 5 screenshots are saved under `session_dir/screenshots/<slug>.png`.
- Response listener is attached per-page but shares the same `download_queue`, `seen_urls`, and `pending_responses` set, so media captured across all pages goes into one bucket.
- **No per-page timeout cap.** We use the same `timeout` param as the homepage call (default 20s for `domcontentloaded`), but we don't kill slow pages early — better to wait than miss content.

### Merging
- All page texts get concatenated with a `## SOURCE: <url>\n` header per page so the LLM knows which content came from where.
- One single brand-data LLM call with the merged text (cheaper than 5 calls; LLM does cross-page synthesis itself).
- The per-page raw text is preserved in JSON under `pages: [{url, text, text_chunks?}]` so the dossier is auditable.

### Failure handling
- If a secondary page fails (timeout, 404, navigation error), log a warning, drop it, continue with the others. Homepage failure aborts the run.

## 2. Multimodal default = ON

- Flip `multimodal: bool = True` default. Flag stays for opt-out.
- Multimodal pass uses **all 5 screenshots in a single API call** — OpenAI Chat API accepts multiple `image_url` blocks per message. One consolidated `visual_data` output across the whole brand.
- Runs in parallel with the brand-data LLM call (after all 5 screenshots are ready).
- Cost note: ~5× a single-image call, but one merged analysis (richer signal: e.g. pricing page often uses accent colors that don't show on homepage hero).

## 3. Lightweight asset surfacing (no classification)

User confirmed they want to classify assets themselves. So we **don't** run a vision pass over images. But we can surface likely candidates using cheap heuristics — surfacing means giving the downstream consumer a head start, not making decisions for them.

In the result JSON under `asset_candidates: {...}`:
- `logo_candidates` — URLs from: filenames containing `logo`, `brand`, favicon `<link rel="icon">`, apple-touch-icon, the og:image.
- `hero_image_candidates` — URLs from: filenames containing `hero`, `banner`, `cover`, plus the largest image above the fold.
- `favicon` — `<link rel="icon">` or fallback `/favicon.ico`.

All from cheap DOM/heuristic work — no vision call.

## 4. Extended schema — additions for video/ad generation

### Brand data (additions)

```python
# already present in v4 — keep
company_name, what_they_do, products_services, pricing_summary,
target_customer, value_prop, tone_voice, testimonials, faqs,
common_phrases, competitors_mentioned, calls_to_action

# NEW
tagline                  # the one-line slogan/hook (often the H1 or hero subhead)
hero_copy                # full hero-section text — the headline they lead with
industry                 # e.g. "AI video tools", "DTC fashion", "B2B SaaS analytics"
trust_signals            # e.g. ["3M+ creators", "Y Combinator backed", "99.9% uptime SLA"]
key_features             # short benefit/feature bullets, separate from products
brand_origin             # founding story / company history if mentioned
mood_descriptors         # ["energetic", "calm", "professional", "playful"] — for video pacing
promotional_info: {
    original_price,      # e.g. "$59"
    promo_price,         # e.g. "$29"
    discount_text,       # e.g. "50% off"
    urgency_text,        # e.g. "Limited time", "Ends Friday"
    valid_until          # explicit date if present
}
```

`promotional_info` fields are all nullable and always attempted — the LLM returns null on non-promo sites without us having to gate by site type.

### Visual data (additions)

```python
# already present in v4 — keep
visual_style_notes, color_palette, tone_descriptors_visual, layout_style

# NEW
primary_colors           # top 3 brand-defining hex codes
accent_colors            # remaining palette
font_style               # narrative — "modern sans-serif", "elegant serif", "playful display"
imagery_style            # "photo-heavy", "illustration-heavy", "3D-rendered", "screenshot-heavy"
energy_level             # "calm" | "balanced" | "energetic" — affects video pacing/music choice
```

### Cheap DOM extras (no LLM)

A single `page.evaluate` on the homepage pulls these directly — fast, exact, no model error:

```python
dom_extras: {
    font_families,       # getComputedStyle on body, h1, h2 — actual CSS font-family strings
    viewport_meta,
    canonical_url,
    language,            # <html lang="...">
    favicon_url,
    apple_touch_icon,
    og_data: { title, description, image, type, site_name, ... },
    twitter_data: { card, title, description, image },
    structured_data,     # JSON-LD blocks — often has Organization, Product, Offer with real pricing
}
```

The `structured_data` (JSON-LD) is the underrated gem — many e-com sites publish exact product names, prices, ratings, availability in machine-readable form. We grab it and pass it to the LLM as context.

## Output JSON shape (v5)

```jsonc
{
  "url": "...",
  "timestamp": "...",
  "session_dir": "...",
  "config": { ... },

  // Pages crawled
  "pages": [
    {
      "url": "...",
      "is_homepage": true,
      "text": [...],
      "text_chunks": [...] | null,
      "screenshot": "screenshots/homepage.png",
      "status": "ok",
      "load_ms": 4321
    },
    {
      "url": "...",
      "is_homepage": false,
      "text": [...],
      "text_chunks": [...] | null,
      "screenshot": "screenshots/pricing.png",
      "status": "ok",
      "load_ms": 6789
    },
    { "url": "...", "is_homepage": false, "status": "failed", "error": "navigation error" }
  ],
  "crawl_meta": {
    "discovery_method": "sitemap" | "nav" | "mixed",
    "candidates_considered": 23,
    "pages_picked": ["..."],
    "llm_pick_latency_ms": 1234
  },

  // Media (captured across all pages)
  "images": [...],          // unchanged shape
  "videos": [...],          // unchanged shape
  "images_count": 22,
  "videos_count": 1,
  "total_count": 23,

  // Heuristic surfacing — no vision call
  "asset_candidates": {
    "logo_candidates": ["..."],
    "hero_image_candidates": ["..."],
    "favicon": "..."
  },

  // All screenshots (also referenced per-page in `pages[]`)
  "screenshots_dir": "scraped_data/.../screenshots/",

  // Cheap DOM signals
  "dom_extras": {
    "font_families": ["Inter", "Söhne", ...],
    "language": "en",
    "favicon_url": "...",
    "og_data": { ... },
    "twitter_data": { ... },
    "structured_data": [ /* JSON-LD blocks */ ]
  },

  // LLM-extracted
  "brand_data": { /* v4 fields + new fields above */ },
  "brand_data_meta": { "model": "gpt-5.4-mini", "latency_ms": ..., "input_tokens": ..., "output_tokens": ... },

  "visual_data": { /* v4 fields + new fields above */ },
  "visual_data_meta": { ... },

  // Per-phase wall-clock timings (ms) — recorded so we know what actually took how long
  "timings": {
    "total_ms": 24123,
    "homepage_load_ms": 4321,
    "homepage_extract_ms": 800,
    "discovery_ms": 1200,            // sitemap fetch + nav extract + LLM rank
    "secondary_pages_ms": 8400,      // wall time waiting for all 4 in parallel
    "per_page_load_ms": {            // individual page nav durations
      "https://...": 4321,
      "https://.../pricing": 6789,
      "...": 5210
    },
    "media_drain_ms": 1100,
    "brand_llm_ms": 4500,
    "visual_llm_ms": 5200
  }
}
```

The `pages` array preserves per-page text for auditability. Top-level `text` is dropped — it would be misleading to have one "text" field after multi-page.

## Function signature

```python
async def scrape(
    url: str,
    timeout: int = 20000,                    # applies to every page nav, homepage and secondary
    headless: bool | Literal["virtual"] = True,
    text_mode: Literal["raw", "chunked"] = "raw",
    llm_extract: bool = True,
    multimodal: bool = True,                 # NEW DEFAULT
    max_pages: int = 5,                      # 1 = old single-page v4 behavior
    camoufox_options: dict[str, Any] | None = None,
) -> dict[str, Any]
```

Setting `max_pages=1` reproduces v4-style single-page behavior — useful for quick smoke tests.
No separate per-page timeout — all pages get the same `timeout`. We don't want to silently drop slow pages just because they're slow.

## Speed budget (target — actual numbers recorded in `timings` block)

| Phase | Estimated time |
|---|---|
| Homepage navigation + scroll + extract + screenshot | ~10–13s |
| Sitemap.xml fetch + nav-link extract | ~1–2s, parallel with homepage extract |
| LLM page-rank call (gpt-5.4-mini, cheap) | ~1–2s |
| 4 secondary pages in parallel (slowest wins, no cap) | ~10–18s |
| LLM brand extraction (over merged text) | ~3–5s, in parallel with media drain |
| LLM visual analysis (5 images in one call) | ~5–8s, parallel with brand LLM |
| **Total wall time (estimated)** | **~22–32s** |

Budget is roughly 2× v4 (was ~13–17s). Tradeoff: significantly richer dossier for 2× wall time, ~5× LLM cost. **Actual values logged in `timings` block of result JSON** so we can see what really happened.

## Explicitly still out of scope

Deferred from earlier "best-way" discussion — keeping these out so v5 stays shippable:

- Asset classification (user will classify themselves)
- Citation/grounding per LLM-extracted field
- External signals (Trustpilot, Crunchbase, LinkedIn, Wayback)
- Eval framework with hand-labeled brands
- HTTP-first fast path with browser fallback
- Brand-level caching across runs
- Script/ad-copy generation downstream
- Performance scoring / competitor ad analysis

## Implementation order (so we can stage and test)

1. Schema additions to `BRAND_DATA_SCHEMA` and `VISUAL_DATA_SCHEMA` — verifiable in isolation by re-running v4 logic with new schema on the existing Magic Hour text.
2. `dom_extras` + `asset_candidates` heuristics — pure DOM work, single `page.evaluate`.
3. Multimodal default flipped to True.
4. Multi-page crawl — biggest change, last so the rest is already tested.
5. Update result JSON shape + drop top-level `text` in favor of `pages`.
6. Re-run on Magic Hour homepage to compare against `magic_hour_scraped_3pm.json`.

## Decisions locked in

1. Top-level `text` field dropped — replaced by `pages[].text`. Per-page text is preserved for auditability.
2. Screenshots on **all** crawled pages (saved under `screenshots/`), all 5 fed to a single multimodal LLM call.
3. JSON-LD structured data extracted and passed to the brand-data LLM as additional context.
4. `max_pages=5` is the default. `max_pages=1` reproduces v4 single-page behavior.
5. **No per-page timeout cap.** All pages share the same `timeout` (default 20s on `goto`); slow pages are not killed early. We'd rather wait than miss content.
6. **Actual wall-clock timings recorded** in a `timings` block of the result JSON so we know exactly where time was spent.
