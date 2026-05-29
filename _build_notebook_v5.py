"""Build scraper_v5.ipynb from inline Python sources."""
import json
from pathlib import Path

NB_PATH = Path(__file__).parent / "scraper_v5.ipynb"


def code_cell(src: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": src.splitlines(keepends=True),
    }


def md_cell(src: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": src.splitlines(keepends=True),
    }


CELL_BADGE = (
    '<a href="https://colab.research.google.com/github/baohuy1303/scraper_v4/blob/main/scraper_v5.ipynb" '
    'target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>'
)

CELL_INSTALL = """!pip install camoufox rich aiohttp openai aiofiles httpx
!camoufox fetch
"""

CELL_CONFIG = '''from pathlib import Path
import os

# OUTPUT_DIR = Path("/content/drive/MyDrive/Magic_Hour_Scraper/scraped_data")
OUTPUT_DIR = Path("scraped_data")
NUM_WORKERS = 5
BATCH_SIZE = 50
MIN_FILE_SIZE = 1500

PAGE_LOAD_WAIT = 2000
PAGE_TIMEOUT_CAP_MS = 40000  # per-page hard cap; timed_out flag set if hit

LLM_MODEL = "gpt-5.4-mini"

# --- API key (Colab) ---
# from google.colab import userdata
# os.environ["OPENAI_API_KEY"] = userdata.get("OPENAI_API_KEY")


def get_scroll_config(total_media: int) -> tuple[float, int]:
    if total_media < 10:
        return 0.10, 50
    elif total_media < 50:
        return 0.05, 75
    else:
        return 0.02, 150
'''

CELL_MAIN = r'''# @title
import asyncio
import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import aiofiles
import httpx
from camoufox.async_api import AsyncCamoufox
from openai import AsyncOpenAI
from playwright.async_api import Browser, Page, Response
from rich.console import Console

console = Console()

SKIP_URL_PATTERNS = [
    "favicon.ico",
    "google-analytics",
    "googletagmanager",
    "doubleclick.net",
    "facebook.com/tr",
    "analytics.js",
    "gtag/js",
    "pixel",
    "tracker",
    "1x1",
    "beacon",
    "telemetry",
    "metrics",
]

DRAIN_BUDGET_S = 5.0  # max time we wait on in-flight response handlers per page


# ----------------------------- Data model ----------------------------- #


@dataclass
class DownloadTask:
    url: str
    filepath: Path
    body: bytes
    resource_type: str
    filename: str


@dataclass
class PageResult:
    url: str
    is_homepage: bool = False
    status: str = "ok"
    error: str | None = None
    text_raw: list[str] = field(default_factory=list)
    text_chunks: list[dict[str, Any]] | None = None
    text_for_json: list[str] = field(default_factory=list)
    text_for_llm: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    screenshot_bytes: bytes | None = None
    screenshot_path: Path | None = None
    load_ms: int | None = None
    timed_out: bool = False
    # Homepage-only extras
    dom_extras: dict[str, Any] | None = None
    asset_candidates: dict[str, Any] | None = None
    nav_links: list[dict[str, str]] | None = None
    pricing_cards: list[dict[str, Any]] | None = None  # populated on pricing pages


# ----------------------------- Media capture ----------------------------- #


async def download_worker(
    queue: asyncio.Queue,
    captured_files: dict[str, list[dict[str, Any]]],
):
    write_buffer: list[tuple[DownloadTask, bytes]] = []

    while True:
        task: DownloadTask | None = await queue.get()
        if task is None:
            queue.task_done()
            if write_buffer:
                await _flush_buffer(write_buffer, captured_files)
            break

        try:
            write_buffer.append((task, task.body))
            queue.task_done()

            if len(write_buffer) >= BATCH_SIZE:
                await _flush_buffer(write_buffer, captured_files)
                write_buffer = []
        except Exception:
            queue.task_done()


async def _flush_buffer(
    buffer: list[tuple[DownloadTask, bytes]],
    captured_files: dict[str, list[dict[str, Any]]],
):
    if not buffer:
        return

    await asyncio.gather(
        *[_write_file(task, body, captured_files) for task, body in buffer]
    )

    console.print(
        f"[dim green]Flushed buffer:[/dim green] {len(buffer)} files written to disk"
    )


async def _write_file(
    task: DownloadTask,
    body: bytes,
    captured_files: dict[str, list[dict[str, Any]]],
):
    try:
        async with aiofiles.open(task.filepath, "wb") as f:
            await f.write(body)

        file_info = {
            "url": task.url,
            "filename": task.filename,
            "saved_path": str(task.filepath),
            "size_bytes": len(body),
        }
        captured_files["images" if task.resource_type == "image" else "videos"].append(
            file_info
        )
    except Exception as e:
        console.print(f"[dim red]Failed to write {task.filename}:[/dim red] {e}")


def get_extension_from_content_type(content_type: str | None, resource_type: str) -> str:
    if not content_type:
        return "png" if resource_type == "image" else "mp4"

    mime_to_ext = {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
        "image/svg+xml": "svg",
        "image/avif": "avif",
        "image/bmp": "bmp",
        "image/x-icon": "ico",
        "video/mp4": "mp4",
        "video/webm": "webm",
        "video/ogg": "ogv",
        "video/quicktime": "mov",
        "video/x-msvideo": "avi",
        "video/x-matroska": "mkv",
    }
    main_type = content_type.split(";")[0].strip().lower()
    return mime_to_ext.get(main_type, "png" if resource_type == "image" else "mp4")


async def on_response(
    response: Response,
    download_queue: asyncio.Queue,
    seen_urls: set[str],
    session_dir: Path,
):
    try:
        if not response.ok or response.url in seen_urls:
            return

        resource_type = response.request.resource_type
        if resource_type not in ("image", "media"):
            return

        url = response.url
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in SKIP_URL_PATTERNS):
            return

        seen_urls.add(url)

        content_length = response.headers.get("content-length")
        if content_length and int(content_length) < MIN_FILE_SIZE:
            return

        content_type = response.headers.get("content-type")
        ext = get_extension_from_content_type(content_type, resource_type)

        url_filename = url.split("/")[-1].split("?")[0]
        if url_filename and "." in url_filename:
            base_filename = url_filename.rsplit(".", 1)[0]
        else:
            base_filename = url_filename or f"{resource_type}_{hash(url)}"

        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{base_filename}_{url_hash}.{ext}"

        output_dir = session_dir / ("images" if resource_type == "image" else "videos")
        filepath = output_dir / filename

        body = await response.body()

        if len(body) < MIN_FILE_SIZE:
            return

        download_queue.put_nowait(
            DownloadTask(url, filepath, body, resource_type, filename)
        )
    except Exception as e:
        console.print(f"[dim red]Failed to capture {response.url}:[/dim red] {e}")


# ----------------------------- DOM extraction ----------------------------- #


async def extract_text_raw(page: Page) -> list[str]:
    all_texts = await page.locator("body").all_inner_texts()
    return [t.strip() for t in all_texts if t.strip()]


async def extract_text_chunked(page: Page) -> list[dict[str, Any]]:
    return await page.evaluate(
        r"""
        () => {
            const body = document.body.cloneNode(true);
            body.querySelectorAll('nav, header, footer, script, style, noscript, aside').forEach(el => el.remove());

            const blocks = body.querySelectorAll('h1, h2, h3, p, li, blockquote, dt, dd');
            const chunks = [];
            let cur = { heading: null, level: 0, text: [] };

            const flush = () => {
                const text = cur.text.join('\n').trim();
                if (text || cur.heading) {
                    chunks.push({ heading: cur.heading, level: cur.level, text });
                }
            };

            for (const el of blocks) {
                const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
                if (!t) continue;
                if (/^H[1-3]$/.test(el.tagName)) {
                    flush();
                    cur = { heading: t, level: parseInt(el.tagName[1]), text: [] };
                } else {
                    cur.text.push(t);
                }
            }
            flush();
            return chunks;
        }
        """
    )


async def extract_metadata(page: Page) -> dict[str, Any]:
    return await page.evaluate(
        """
        () => {
            const title = document.title;
            const description = document.querySelector('meta[name="description"]')?.content || null;
            const ogImages = Array.from(document.querySelectorAll('meta[property="og:image"]'))
                .map(el => el.content)
                .filter(Boolean);
            return { title, description, og_images: ogImages, og_image: ogImages[0] || null };
        }
        """
    )


async def extract_dom_extras(page: Page) -> dict[str, Any]:
    """Cheap DOM signals that don't need an LLM."""
    return await page.evaluate(
        """
        () => {
            const result = {};
            result.language = document.documentElement.lang || null;
            result.canonical_url = document.querySelector('link[rel="canonical"]')?.href || null;
            result.viewport_meta = document.querySelector('meta[name="viewport"]')?.content || null;

            const iconLinks = Array.from(document.querySelectorAll('link[rel]'));
            const matchIcon = (sub) => iconLinks.find(l => (l.rel || '').toLowerCase().includes(sub))?.href || null;
            result.favicon_url = matchIcon('icon');
            result.apple_touch_icon = matchIcon('apple-touch-icon');

            const getFont = (sel) => {
                const el = document.querySelector(sel);
                if (!el) return null;
                return getComputedStyle(el).fontFamily;
            };
            result.font_families = {
                body: getFont('body'),
                h1: getFont('h1'),
                h2: getFont('h2'),
                p: getFont('p'),
            };

            const og = {};
            document.querySelectorAll('meta[property^="og:"]').forEach(m => {
                const key = (m.getAttribute('property') || '').replace('og:', '');
                og[key] = m.content;
            });
            result.og_data = og;

            const tw = {};
            document.querySelectorAll('meta[name^="twitter:"]').forEach(m => {
                const key = (m.getAttribute('name') || '').replace('twitter:', '');
                tw[key] = m.content;
            });
            result.twitter_data = tw;

            const ld = [];
            document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
                try { ld.push(JSON.parse(s.textContent)); } catch(e) {}
            });
            result.structured_data = ld;

            return result;
        }
        """
    )


async def extract_asset_candidates(page: Page) -> dict[str, Any]:
    """Heuristic surfacing of likely logo, hero, favicon URLs — no LLM."""
    return await page.evaluate(
        r"""
        () => {
            const iconLinks = Array.from(document.querySelectorAll('link[rel]'));
            const matchIcon = (sub) => iconLinks.find(l => (l.rel || '').toLowerCase().includes(sub))?.href || null;
            const favicon = matchIcon('icon') && !matchIcon('icon').includes('apple') ? matchIcon('icon') : null;
            const appleTouch = matchIcon('apple-touch-icon');
            const ogImage = document.querySelector('meta[property="og:image"]')?.content || null;
            const twImage = document.querySelector('meta[name="twitter:image"]')?.content || null;

            const viewportHeight = window.innerHeight;
            const aboveFold = [];
            document.querySelectorAll('img').forEach(img => {
                const rect = img.getBoundingClientRect();
                if (rect.top < viewportHeight && rect.bottom > 0 && rect.width > 50 && rect.height > 50) {
                    const src = img.currentSrc || img.src;
                    if (src) aboveFold.push({ src, area: rect.width * rect.height });
                }
            });
            aboveFold.sort((a, b) => b.area - a.area);

            const allImgSrcs = new Set();
            document.querySelectorAll('img').forEach(img => {
                const src = img.currentSrc || img.src;
                if (src) allImgSrcs.add(src);
            });

            const logoMatches = [];
            const heroMatches = [];
            for (const src of allImgSrcs) {
                const low = src.toLowerCase();
                if (/logo|brand|wordmark/.test(low)) logoMatches.push(src);
                if (/hero|banner|cover|header-image|masthead/.test(low)) heroMatches.push(src);
            }

            return {
                favicon: favicon,
                apple_touch_icon: appleTouch,
                og_image: ogImage,
                twitter_image: twImage,
                above_fold_top: aboveFold.slice(0, 5).map(i => i.src),
                logo_url_matches: logoMatches,
                hero_url_matches: heroMatches,
            };
        }
        """
    )


async def extract_pricing_cards(page: Page) -> list[dict[str, Any]]:
    """DOM-based pricing plan extractor. Finds pricing card elements and reads
    plan name, prices, and feature bullets directly — avoids the flat-text
    table-mangling problem. Returns [] if no recognisable cards found."""
    return await page.evaluate(
        r"""
        () => {
            // Heuristic: find containers that have both a price-like token and a CTA button.
            // Covers most SaaS pricing grids (Stripe Checkout style, Tailwind card grids, etc.).
            const priceRe = /[\$\€\£\¥][\d,]+(\.\d+)?|[\d,]+(\.\d+)?\s*(USD|EUR|GBP)/i;
            const allDivs = Array.from(document.querySelectorAll('div, section, article, li'));

            const cards = [];
            for (const el of allDivs) {
                // Must be reasonably small (a card, not the whole page)
                if (el.children.length > 30) continue;
                const text = (el.innerText || '').trim();
                if (text.length < 20 || text.length > 2000) continue;
                // Must contain a price token
                if (!priceRe.test(text)) continue;
                // Must contain a button or CTA-like link
                const hasAction = el.querySelector('button, a[href*="sign"], a[href*="get"], a[href*="start"], a[href*="upgrade"]');
                if (!hasAction) continue;

                // Extract lines, filtering empty ones
                const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

                // Try to find plan name (first non-price, non-empty short line)
                let name = null;
                for (const l of lines) {
                    if (l.length < 50 && !priceRe.test(l) && !/free|month|year|billed|save|credit|per|most|best|popular|value/i.test(l)) {
                        name = l;
                        break;
                    }
                }
                if (!name) continue;

                // Collect price tokens
                const priceTokens = lines.filter(l => priceRe.test(l));

                // Collect feature bullets (lines that aren't prices, names, or CTAs)
                const features = lines.filter(l =>
                    !priceRe.test(l) &&
                    l !== name &&
                    l.length > 3 && l.length < 120 &&
                    !/^(get|sign|start|upgrade|buy|subscribe)/i.test(l) &&
                    !/^(free forever|most popular|best value|save)/i.test(l)
                ).slice(0, 10);

                cards.push({ name, price_tokens: priceTokens, features });
            }

            // Deduplicate by name
            const seen = new Set();
            return cards.filter(c => {
                if (seen.has(c.name)) return false;
                seen.add(c.name);
                return true;
            });
        }
        """
    )


async def extract_nav_links(page: Page) -> list[dict[str, str]]:
    """Extract anchor links from nav/header/footer/role=navigation."""
    return await page.evaluate(
        """
        () => {
            const containers = Array.from(document.querySelectorAll(
                'nav, header, footer, [role="navigation"]'
            ));
            const seen = new Set();
            const out = [];
            for (const c of containers) {
                for (const a of c.querySelectorAll('a[href]')) {
                    const href = a.href;
                    if (!href) continue;
                    if (href.startsWith('javascript:') || href.startsWith('mailto:') || href.startsWith('tel:')) continue;
                    if (seen.has(href)) continue;
                    seen.add(href);
                    const text = ((a.innerText || a.textContent) || '').trim().replace(/\\s+/g, ' ');
                    out.push({ href, text });
                }
            }
            return out;
        }
        """
    )


async def adaptive_scroll(page: Page) -> None:
    img_count = await page.locator("img").count()
    video_count = await page.locator("video").count()
    total_media = img_count + video_count

    scroll_percent, delay = get_scroll_config(total_media)

    await page.evaluate(
        f"""
        async () => {{
            const totalHeight = document.scrollingElement.scrollHeight;
            const viewportHeight = window.innerHeight;
            const scrollableHeight = totalHeight - viewportHeight;
            const scrollStep = Math.min(Math.max(scrollableHeight * {scroll_percent}, 100), 500);
            const delay = {delay};

            while (document.scrollingElement.scrollTop + viewportHeight < totalHeight) {{
                document.scrollingElement.scrollBy(0, scrollStep);
                await new Promise(resolve => setTimeout(resolve, delay));
            }}
        }}
        """
    )


# ----------------------------- Page discovery ----------------------------- #


_SITEMAP_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)


async def fetch_sitemap_urls(base_url: str, http: httpx.AsyncClient) -> list[str]:
    """Try /sitemap.xml. If it's a sitemap index, follow one level. Best-effort."""
    parsed = urlparse(base_url)
    same_netloc = parsed.netloc
    candidates = [
        f"{parsed.scheme}://{parsed.netloc}/sitemap.xml",
        f"{parsed.scheme}://{parsed.netloc}/sitemap_index.xml",
    ]
    for sm_url in candidates:
        try:
            r = await http.get(sm_url, timeout=5.0, follow_redirects=True)
            if r.status_code != 200 or "xml" not in (r.headers.get("content-type", "") + r.text[:100].lower()):
                continue
            locs = [m.strip() for m in _SITEMAP_LOC_RE.findall(r.text)]
            # If the locs themselves look like sitemaps, fetch one and merge
            if locs and all(l.endswith(".xml") for l in locs[:3]):
                expanded: list[str] = []
                for child_sm in locs[:3]:  # cap to avoid runaway
                    try:
                        cr = await http.get(child_sm, timeout=5.0, follow_redirects=True)
                        if cr.status_code == 200:
                            expanded.extend(m.strip() for m in _SITEMAP_LOC_RE.findall(cr.text))
                    except Exception:
                        pass
                locs = expanded
            # Same-domain filter
            same = []
            seen: set[str] = set()
            for u in locs:
                if urlparse(u).netloc != same_netloc:
                    continue
                if u in seen:
                    continue
                seen.add(u)
                same.append(u)
            if same:
                return same
        except Exception:
            continue
    return []


async def llm_rank_pages(
    homepage_url: str,
    candidates: list[dict[str, str]],
    n_pick: int,
    client: AsyncOpenAI,
) -> tuple[list[str], int]:
    """LLM picks top-N URLs from candidates. Returns (urls, latency_ms)."""
    start = time.time()

    prompt = (
        f"You are helping select pages from a website to scrape for brand/product data "
        f"useful for generating video ads and marketing creative. The homepage is {homepage_url}.\n\n"
        f"From the candidate URLs below, pick the {n_pick} URLs most likely to contain useful "
        f"information. Prioritize: pricing, products, features, use-cases, customers/testimonials, "
        f"about, FAQ, brand story. Avoid: individual blog posts, legal pages, login/signup, "
        f"contact-only pages, status pages, deeply-nested article URLs.\n\n"
        f"Candidates (some have anchor text from the nav):\n"
        f"{json.dumps(candidates, indent=2)}\n\n"
        f"Return exactly {n_pick} URLs from the candidates list, in priority order."
    )

    schema = {
        "type": "object",
        "properties": {
            "urls": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["urls"],
        "additionalProperties": False,
    }

    response = await client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "ranked_urls", "schema": schema, "strict": True},
        },
    )
    elapsed_ms = int((time.time() - start) * 1000)
    parsed = json.loads(response.choices[0].message.content)
    return parsed.get("urls", [])[:n_pick], elapsed_ms


# ----------------------------- LLM extraction ----------------------------- #

BRAND_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "company_name": {"type": ["string", "null"]},
        "what_they_do": {"type": ["string", "null"]},
        "tagline": {"type": ["string", "null"], "description": "Short slogan/hook (often the H1 or hero subhead)."},
        "hero_copy": {"type": ["string", "null"], "description": "Full hero-section text — the headline they lead with on the homepage."},
        "industry": {"type": ["string", "null"], "description": "E.g. 'AI video tools', 'DTC fashion', 'B2B SaaS analytics'."},
        "products_services": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": ["string", "null"]},
                    "pricing": {"type": ["string", "null"]},
                },
                "required": ["name", "description", "pricing"],
                "additionalProperties": False,
            },
        },
        "key_features": {"type": "array", "items": {"type": "string"}, "description": "Short benefit/feature bullets, distinct from products_services."},
        "pricing_summary": {"type": ["string", "null"], "description": "1-3 sentence human-readable overview of the pricing model."},
        "pricing_tiers": {
            "type": "array",
            "description": "One entry per plan/tier. Extract exactly what the page shows — do not convert currencies.",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Plan name, e.g. 'Basic', 'Creator', 'Pro', 'Business'."},
                    "monthly_price": {"type": ["string", "null"], "description": "Price when billed monthly, e.g. '$39/mo'. Null if not shown."},
                    "annual_price_per_month": {"type": ["string", "null"], "description": "Effective monthly price on annual billing, e.g. '$25/mo'. Null if not shown."},
                    "annual_total": {"type": ["string", "null"], "description": "Total charged annually, e.g. '$300/year'. Null if not shown."},
                    "credits": {"type": ["string", "null"], "description": "Credits included, e.g. '300,000 / year' or '400'."},
                    "highlights": {"type": "array", "items": {"type": "string"}, "description": "Key differentiating features for this tier."},
                },
                "required": ["name", "monthly_price", "annual_price_per_month", "annual_total", "credits", "highlights"],
                "additionalProperties": False,
            },
        },
        "pricing_currency": {"type": ["string", "null"], "description": "Currency symbol or code detected on the pricing page, e.g. 'USD', '$', '€'. Helps flag geo-IP currency mismatches."},
        "target_customer": {"type": ["string", "null"]},
        "value_prop": {"type": ["string", "null"]},
        "tone_voice": {"type": ["string", "null"], "description": "Tone descriptors, e.g. 'playful, technical, premium'."},
        "mood_descriptors": {"type": "array", "items": {"type": "string"}, "description": "E.g. 'energetic', 'calm', 'professional', 'playful' — for video pacing."},
        "brand_origin": {"type": ["string", "null"], "description": "Founding story / company history if mentioned."},
        "trust_signals": {"type": "array", "items": {"type": "string"}, "description": "E.g. '3M+ creators', 'Y Combinator backed', '99.9% uptime SLA'."},
        "testimonials": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "quote": {"type": "string"},
                    "attribution": {"type": ["string", "null"]},
                },
                "required": ["quote", "attribution"],
                "additionalProperties": False,
            },
        },
        "faqs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "answer": {"type": "string"},
                },
                "required": ["question", "answer"],
                "additionalProperties": False,
            },
        },
        "common_phrases": {"type": "array", "items": {"type": "string"}},
        "competitors_mentioned": {"type": "array", "items": {"type": "string"}},
        "calls_to_action": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "company_name", "what_they_do", "tagline", "hero_copy", "industry",
        "products_services", "key_features", "pricing_summary", "pricing_tiers",
        "pricing_currency", "target_customer", "value_prop", "tone_voice",
        "mood_descriptors", "brand_origin", "trust_signals", "testimonials",
        "faqs", "common_phrases", "competitors_mentioned", "calls_to_action",
    ],
    "additionalProperties": False,
}

VISUAL_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "visual_style_notes": {"type": ["string", "null"]},
        "color_palette": {"type": "array", "items": {"type": "string"}, "description": "Dominant hex codes across the brand."},
        "primary_colors": {"type": "array", "items": {"type": "string"}, "description": "Top 3 brand-defining hex codes."},
        "accent_colors": {"type": "array", "items": {"type": "string"}, "description": "Remaining palette."},
        "tone_descriptors_visual": {"type": "array", "items": {"type": "string"}},
        "layout_style": {"type": ["string", "null"]},
        "font_style": {"type": ["string", "null"], "description": "Narrative — 'modern sans-serif', 'elegant serif', 'playful display'."},
        "imagery_style": {"type": ["string", "null"], "description": "E.g. 'photo-heavy', 'illustration-heavy', '3D-rendered', 'screenshot-heavy'."},
        "energy_level": {"type": ["string", "null"], "description": "'calm', 'balanced', or 'energetic' — affects video pacing/music."},
    },
    "required": [
        "visual_style_notes", "color_palette", "primary_colors", "accent_colors",
        "tone_descriptors_visual", "layout_style", "font_style", "imagery_style", "energy_level",
    ],
    "additionalProperties": False,
}


async def llm_extract_brand_data(
    text_for_llm: str,
    homepage_metadata: dict[str, Any],
    dom_extras: dict[str, Any],
    pricing_cards: list[dict[str, Any]],
    client: AsyncOpenAI,
) -> dict[str, Any]:
    start = time.time()

    # Trim structured data so we don't bloat the prompt
    structured = dom_extras.get("structured_data") or []
    structured_str = json.dumps(structured, indent=2)
    if len(structured_str) > 8000:
        structured_str = structured_str[:8000] + "\n...[truncated]"

    pricing_section = ""
    if pricing_cards:
        pricing_section = (
            f"\n\n## Pricing cards (DOM-extracted — use this as the authoritative source for "
            f"pricing_tiers and pricing_currency; the page text may have garbled table data)\n"
            f"{json.dumps(pricing_cards, indent=2)}"
        )

    prompt = (
        "Extract structured brand data from the following content. The content comes from multiple "
        "pages of the same website — each page is preceded by a '## SOURCE: <url>' header. "
        "Synthesize across pages; do not invent details. Use null for unknown string fields and [] for unknown list fields.\n\n"
        "For pricing_tiers: use the '## Pricing cards' section if present — it is DOM-extracted and "
        "more accurate than the flat page text. Capture prices exactly as shown (do not convert currencies). "
        "Set pricing_currency to the currency symbol or code you see (e.g. '$', '€', 'USD').\n\n"
        f"## Homepage metadata\n{json.dumps(homepage_metadata, indent=2)}\n\n"
        f"## Structured data (JSON-LD from homepage)\n{structured_str}"
        f"{pricing_section}\n\n"
        f"## Page content\n{text_for_llm}"
    )

    response = await client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "brand_data", "schema": BRAND_DATA_SCHEMA, "strict": True},
        },
    )

    elapsed_ms = int((time.time() - start) * 1000)
    data = json.loads(response.choices[0].message.content)
    usage = response.usage

    return {
        "data": data,
        "meta": {
            "model": LLM_MODEL,
            "latency_ms": elapsed_ms,
            "input_tokens": getattr(usage, "prompt_tokens", None),
            "output_tokens": getattr(usage, "completion_tokens", None),
        },
    }


async def llm_visual_analysis(
    screenshots: list[tuple[str, bytes]],  # list of (label, bytes)
    client: AsyncOpenAI,
) -> dict[str, Any]:
    start = time.time()

    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"Analyze {len(screenshots)} screenshots from different pages of the same brand's website. "
                "Synthesize ONE visual analysis describing the brand's overall visual identity. "
                "Extract the dominant color palette as hex codes (primary + accent), describe the visual "
                "aesthetic, font style, imagery style, energy level, and layout patterns. "
                "Treat the screenshots as one brand — do not analyze each page separately."
            ),
        }
    ]
    for label, b in screenshots:
        img_b64 = base64.b64encode(b).decode()
        content.append({"type": "text", "text": f"### Page: {label}"})
        content.append(
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}}
        )

    response = await client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": content}],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "visual_data", "schema": VISUAL_DATA_SCHEMA, "strict": True},
        },
    )

    elapsed_ms = int((time.time() - start) * 1000)
    data = json.loads(response.choices[0].message.content)
    usage = response.usage

    return {
        "data": data,
        "meta": {
            "model": LLM_MODEL,
            "latency_ms": elapsed_ms,
            "input_tokens": getattr(usage, "prompt_tokens", None),
            "output_tokens": getattr(usage, "completion_tokens", None),
            "screenshots_used": len(screenshots),
        },
    }


# ----------------------------- Single-page scraper ----------------------------- #


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/") or "homepage"
    slug = re.sub(r"[^\w\-]+", "_", path)[:60]
    h = hashlib.md5(url.encode()).hexdigest()[:6]
    return f"{slug}_{h}"


async def scrape_single_page(
    browser: Browser,
    url: str,
    *,
    is_homepage: bool,
    timeout: int,
    text_mode: Literal["raw", "chunked"],
    download_queue: asyncio.Queue,
    seen_urls: set[str],
    session_dir: Path,
    screenshots_dir: Path,
) -> PageResult:
    """Scrape one page. Returns a PageResult. Never raises — failures go in `error`."""
    label = "homepage" if is_homepage else _slug_from_url(url)
    console.print(f"[cyan]→ [{label}][/cyan] {url}")

    result = PageResult(url=url, is_homepage=is_homepage)
    page: Page | None = None
    page_pending: set[asyncio.Task] = set()

    def handle_response(r: Response) -> None:
        t = asyncio.create_task(on_response(r, download_queue, seen_urls, session_dir))
        page_pending.add(t)
        t.add_done_callback(page_pending.discard)

    try:
        page = await browser.new_page()
        page.on("response", handle_response)

        load_start = time.time()

        async def _load_and_extract() -> None:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await page.wait_for_timeout(PAGE_LOAD_WAIT)

            # Capture above-fold assets BEFORE scrolling so viewport is at top
            if is_homepage:
                result.asset_candidates = await extract_asset_candidates(page)

            await adaptive_scroll(page)
            try:
                await page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass

            # Run remaining extractors in parallel
            tasks: dict[str, asyncio.Task] = {
                "metadata": asyncio.create_task(extract_metadata(page)),
                "screenshot": asyncio.create_task(page.screenshot(full_page=False)),
            }
            if text_mode == "chunked":
                tasks["text_chunks"] = asyncio.create_task(extract_text_chunked(page))
            else:
                tasks["text_raw"] = asyncio.create_task(extract_text_raw(page))
            if is_homepage:
                tasks["dom_extras"] = asyncio.create_task(extract_dom_extras(page))
                tasks["nav_links"] = asyncio.create_task(extract_nav_links(page))
            if "pricing" in url.lower():
                tasks["pricing_cards"] = asyncio.create_task(extract_pricing_cards(page))

            gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)
            keyed = dict(zip(tasks.keys(), gathered))

            # Metadata
            md = keyed.get("metadata")
            result.metadata = md if isinstance(md, dict) else {}

            # Screenshot bytes
            sb = keyed.get("screenshot")
            if isinstance(sb, (bytes, bytearray)):
                result.screenshot_bytes = bytes(sb)

            # Text
            if text_mode == "chunked":
                chunks = keyed.get("text_chunks")
                if isinstance(chunks, list):
                    result.text_chunks = chunks
                    parts: list[str] = []
                    for c in chunks:
                        if not isinstance(c, dict):
                            continue
                        if c.get("heading"):
                            parts.append(f"## {c['heading']}")
                        if c.get("text"):
                            parts.append(c["text"])
                    result.text_for_json = parts
                    result.text_for_llm = "\n\n".join(parts)
            else:
                raw = keyed.get("text_raw")
                if isinstance(raw, list):
                    result.text_raw = raw
                    result.text_for_json = raw
                    result.text_for_llm = "\n\n".join(raw)

            if is_homepage:
                de = keyed.get("dom_extras")
                result.dom_extras = de if isinstance(de, dict) else {}
                nl = keyed.get("nav_links")
                result.nav_links = nl if isinstance(nl, list) else []
            pc = keyed.get("pricing_cards")
            if isinstance(pc, list):
                result.pricing_cards = pc

        try:
            await asyncio.wait_for(
                _load_and_extract(),
                timeout=PAGE_TIMEOUT_CAP_MS / 1000,
            )
        except asyncio.TimeoutError:
            result.timed_out = True
            console.print(
                f"[yellow]⚠ [{label}] hit {PAGE_TIMEOUT_CAP_MS/1000:.0f}s cap — "
                f"saving whatever was captured[/yellow]"
            )

        # Save screenshot if we got one (even on timeout, partial data is kept)
        if result.screenshot_bytes is not None:
            screenshots_dir.mkdir(parents=True, exist_ok=True)
            sp = screenshots_dir / f"{label}.png"
            async with aiofiles.open(sp, "wb") as f:
                await f.write(result.screenshot_bytes)
            result.screenshot_path = sp

        result.load_ms = int((time.time() - load_start) * 1000)
        flag = " [TIMED OUT]" if result.timed_out else ""
        console.print(f"[dim green]✓ [{label}] {result.load_ms}ms{flag}[/dim green]")

    except Exception as e:
        result.status = "failed"
        result.error = str(e)
        console.print(f"[red]✗ [{label}] failed: {e}[/red]")

    finally:
        # Detach listener and drain THIS page's response handlers BEFORE closing the page.
        if page is not None:
            try:
                page.remove_listener("response", handle_response)
            except Exception:
                pass

            drain_start = time.time()
            while page_pending:
                if time.time() - drain_start > DRAIN_BUDGET_S:
                    console.print(
                        f"[dim yellow]Drain budget hit on [{label}]; "
                        f"{len(page_pending)} handler(s) still pending[/dim yellow]"
                    )
                    break
                await asyncio.gather(*list(page_pending), return_exceptions=True)
                await asyncio.sleep(0.05)

            try:
                await page.close()
            except Exception:
                pass

    return result


# ----------------------------- Main scrape entrypoint ----------------------------- #


async def scrape(
    url: str,
    timeout: int = 20000,
    headless: bool | Literal["virtual"] = True,
    text_mode: Literal["raw", "chunked"] = "raw",
    llm_extract: bool = True,
    multimodal: bool = True,
    max_pages: int = 5,
    camoufox_options: dict[str, Any] | None = None,
) -> dict[str, Any]:

    # Runtime Literal validation (v4 bug: typos silently fell through)
    if text_mode not in ("raw", "chunked"):
        raise ValueError(f"text_mode must be 'raw' or 'chunked', got {text_mode!r}")
    if max_pages < 1:
        raise ValueError(f"max_pages must be >= 1, got {max_pages}")

    overall_start = time.time()
    timings: dict[str, Any] = {}

    captured_files: dict[str, list[dict[str, Any]]] = {"images": [], "videos": []}
    camoufox_options = camoufox_options or {}

    seen_urls: set[str] = set()

    parsed_url = urlparse(url)
    domain = parsed_url.netloc or parsed_url.path
    domain = re.sub(r"[^\w\-.]", "_", domain)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_dir = OUTPUT_DIR / f"{domain}_{timestamp}"
    screenshots_dir = session_dir / "screenshots"

    (session_dir / "images").mkdir(parents=True, exist_ok=True)
    (session_dir / "videos").mkdir(parents=True, exist_ok=True)
    screenshots_dir.mkdir(parents=True, exist_ok=True)

    download_queue: asyncio.Queue = asyncio.Queue()
    workers = [
        asyncio.create_task(download_worker(download_queue, captured_files))
        for _ in range(NUM_WORKERS)
    ]

    openai_client: AsyncOpenAI | None = None
    if llm_extract or multimodal:
        openai_client = AsyncOpenAI()

    homepage_result: PageResult | None = None
    secondary_results: list[PageResult] = []
    crawl_meta: dict[str, Any] | None = None

    async with AsyncCamoufox(headless=headless, **camoufox_options) as browser:
        # Phase 1: homepage + (parallel) sitemap fetch
        sitemap_task = asyncio.create_task(_fetch_sitemap_wrapper(url))
        homepage_start = time.time()
        homepage_result = await scrape_single_page(
            browser,
            url,
            is_homepage=True,
            timeout=timeout,
            text_mode=text_mode,
            download_queue=download_queue,
            seen_urls=seen_urls,
            session_dir=session_dir,
            screenshots_dir=screenshots_dir,
        )
        timings["homepage_total_ms"] = int((time.time() - homepage_start) * 1000)

        sitemap_urls = await sitemap_task

        # Phase 2: discover candidates + LLM rank
        if max_pages > 1 and homepage_result.status == "ok":
            disc_start = time.time()
            same_netloc = urlparse(url).netloc
            nav_links = homepage_result.nav_links or []

            seen_cands: set[str] = {url}
            candidates: list[dict[str, str]] = []

            # Sitemap first (already filtered by domain)
            for u in sitemap_urls:
                if u in seen_cands:
                    continue
                seen_cands.add(u)
                candidates.append({"href": u, "text": "", "source": "sitemap"})

            # Nav next (filter by same domain)
            for link in nav_links:
                href = link.get("href") or ""
                if not href or href in seen_cands:
                    continue
                if urlparse(href).netloc != same_netloc:
                    continue
                seen_cands.add(href)
                candidates.append({"href": href, "text": link.get("text", ""), "source": "nav"})

            if sitemap_urls and nav_links:
                discovery_method = "mixed"
            elif sitemap_urls:
                discovery_method = "sitemap"
            elif nav_links:
                discovery_method = "nav"
            else:
                discovery_method = "none"

            n_pick = max_pages - 1
            picked_urls: list[str] = []
            llm_pick_ms: int | None = None
            if candidates and openai_client is not None and n_pick > 0:
                try:
                    picked_urls, llm_pick_ms = await llm_rank_pages(
                        url, candidates, n_pick, openai_client
                    )
                except Exception as e:
                    console.print(f"[red]llm_rank_pages failed: {e}[/red]")
                    picked_urls = [c["href"] for c in candidates[:n_pick]]
            else:
                picked_urls = [c["href"] for c in candidates[:n_pick]]

            timings["discovery_ms"] = int((time.time() - disc_start) * 1000)

            crawl_meta = {
                "discovery_method": discovery_method,
                "candidates_considered": len(candidates),
                "pages_picked": picked_urls,
                "llm_pick_latency_ms": llm_pick_ms,
                "sitemap_count": len(sitemap_urls),
                "nav_link_count": len(nav_links),
            }

            # Phase 3: secondary pages in parallel
            if picked_urls:
                console.print(f"[cyan]Crawling {len(picked_urls)} secondary pages in parallel...[/cyan]")
                sec_start = time.time()
                secondary_tasks = [
                    scrape_single_page(
                        browser,
                        u,
                        is_homepage=False,
                        timeout=timeout,
                        text_mode=text_mode,
                        download_queue=download_queue,
                        seen_urls=seen_urls,
                        session_dir=session_dir,
                        screenshots_dir=screenshots_dir,
                    )
                    for u in picked_urls
                ]
                gathered = await asyncio.gather(*secondary_tasks, return_exceptions=True)
                for r, picked in zip(gathered, picked_urls):
                    if isinstance(r, Exception):
                        secondary_results.append(
                            PageResult(url=picked, is_homepage=False, status="failed", error=str(r))
                        )
                    else:
                        secondary_results.append(r)
                timings["secondary_pages_ms"] = int((time.time() - sec_start) * 1000)

        # Phase 4: build merged text and kick off LLM tasks (run in parallel with media drain)
        all_pages: list[PageResult] = [homepage_result] + secondary_results
        successful = [p for p in all_pages if p.status == "ok"]

        merged_parts: list[str] = []
        for p in successful:
            if not p.text_for_llm:
                continue
            merged_parts.append(f"## SOURCE: {p.url}\n\n{p.text_for_llm}")
        text_for_llm = "\n\n---\n\n".join(merged_parts)

        homepage_metadata = homepage_result.metadata or {}
        homepage_dom_extras = homepage_result.dom_extras or {}

        # Collect pricing cards from any pricing page crawled
        all_pricing_cards: list[dict[str, Any]] = []
        for p in successful:
            if p.pricing_cards:
                all_pricing_cards.extend(p.pricing_cards)

        llm_tasks: dict[str, asyncio.Task] = {}
        if llm_extract and openai_client is not None and text_for_llm:
            llm_tasks["brand"] = asyncio.create_task(
                llm_extract_brand_data(
                    text_for_llm, homepage_metadata, homepage_dom_extras,
                    all_pricing_cards, openai_client
                )
            )
        if multimodal and openai_client is not None:
            shots = [
                ("homepage" if p.is_homepage else _slug_from_url(p.url), p.screenshot_bytes)
                for p in successful
                if p.screenshot_bytes
            ]
            if shots:
                llm_tasks["visual"] = asyncio.create_task(
                    llm_visual_analysis(shots, openai_client)
                )

        # Drain media queue (parallel with LLM tasks)
        drain_start = time.time()
        console.print("[cyan]Draining media download queue...[/cyan]")
        await download_queue.join()
        timings["media_drain_ms"] = int((time.time() - drain_start) * 1000)

        for _ in range(NUM_WORKERS):
            await download_queue.put(None)
        await asyncio.gather(*workers)

    # Browser closed. Await LLM tasks.
    brand_data = None
    brand_data_meta = None
    visual_data = None
    visual_data_meta = None

    if llm_tasks:
        console.print(f"[cyan]Awaiting {len(llm_tasks)} LLM task(s)...[/cyan]")
        for key, task in llm_tasks.items():
            try:
                res = await task
                if key == "brand":
                    brand_data = res["data"]
                    brand_data_meta = res["meta"]
                    timings["brand_llm_ms"] = res["meta"]["latency_ms"]
                    console.print(
                        f"[dim green]✓ brand_data in {res['meta']['latency_ms']}ms[/dim green]"
                    )
                elif key == "visual":
                    visual_data = res["data"]
                    visual_data_meta = res["meta"]
                    timings["visual_llm_ms"] = res["meta"]["latency_ms"]
                    console.print(
                        f"[dim green]✓ visual_data in {res['meta']['latency_ms']}ms[/dim green]"
                    )
            except Exception as e:
                console.print(f"[red]LLM task '{key}' failed: {e}[/red]")
                if key == "brand":
                    brand_data_meta = {"error": str(e)}
                else:
                    visual_data_meta = {"error": str(e)}

    timings["total_ms"] = int((time.time() - overall_start) * 1000)
    timings["per_page_load_ms"] = {p.url: p.load_ms for p in all_pages if p.load_ms is not None}

    # Build result JSON
    pages_json = []
    timed_out_pages = []
    for p in all_pages:
        entry = {
            "url": p.url,
            "is_homepage": p.is_homepage,
            "status": p.status,
            "load_ms": p.load_ms,
            "timed_out": p.timed_out,
        }
        if p.status == "ok":
            entry["text"] = p.text_for_json
            entry["text_chunks"] = p.text_chunks
            entry["metadata"] = p.metadata
            entry["screenshot"] = str(p.screenshot_path) if p.screenshot_path else None
        if p.error:
            entry["error"] = p.error
        pages_json.append(entry)
        if p.timed_out:
            timed_out_pages.append(p.url)

    result_data = {
        "url": url,
        "timestamp": timestamp,
        "session_dir": str(session_dir),
        "config": {
            "text_mode": text_mode,
            "llm_extract": llm_extract,
            "multimodal": multimodal,
            "max_pages": max_pages,
            "timeout": timeout,
        },
        "pages": pages_json,
        "any_timed_out": len(timed_out_pages) > 0,
        "timed_out_pages": timed_out_pages,
        "crawl_meta": crawl_meta,
        "images": captured_files["images"],
        "videos": captured_files["videos"],
        "images_count": len(captured_files["images"]),
        "videos_count": len(captured_files["videos"]),
        "total_count": len(captured_files["images"]) + len(captured_files["videos"]),
        "asset_candidates": homepage_result.asset_candidates if homepage_result else None,
        "screenshots_dir": str(screenshots_dir),
        "dom_extras": homepage_dom_extras,
        "brand_data": brand_data,
        "brand_data_meta": brand_data_meta,
        "visual_data": visual_data,
        "visual_data_meta": visual_data_meta,
        "timings": timings,
    }

    result_json = session_dir / "scrape_result.json"
    async with aiofiles.open(result_json, "w") as f:
        await f.write(json.dumps(result_data, indent=2, default=str))

    console.print(
        f"\n[bold green]✓ Done in {timings['total_ms']}ms[/bold green] "
        f"| pages: {len(successful)}/{len(all_pages)} | "
        f"media: {result_data['total_count']}"
    )
    console.print(f"[cyan]Saved to:[/cyan] {result_json}")

    return result_data


async def _fetch_sitemap_wrapper(url: str) -> list[str]:
    """Convenience: open a short-lived httpx client to fetch sitemap URLs."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as http:
            return await fetch_sitemap_urls(url, http)
    except Exception as e:
        console.print(f"[dim yellow]Sitemap fetch failed: {e}[/dim yellow]")
        return []
'''

CELL_EXAMPLE = '''# Don't need these 2 lines when on local python
import nest_asyncio
nest_asyncio.apply()
import asyncio

url = "https://magichour.ai"

async def main():
    # Full v5 run: 5 pages crawled, brand + visual LLM, all screenshots
    res = await scrape(
        url,
        text_mode="raw",       # "raw" or "chunked"
        llm_extract=True,
        multimodal=True,       # ON by default in v5
        max_pages=5,           # 1 = single-page v4-style behavior
    )

    # Quick look at timings:
    print(res["timings"])

asyncio.run(main())
'''

CELL_BENCHMARK = '''# @title Benchmark — compare across test URLs
import time
from rich.table import Table

TEST_URLS = [
    ("magichour.ai", "https://magichour.ai"),
    ("medium", "https://medium.com/@amit25173/scrapy-vs-beautifulsoup-vs-selenium-579bce149262"),
    # ("higgsfield", "https://higgsfield.ai"),
    # ("creatify", "https://creatify.ai"),
]


def field_fill_rate(brand: dict | None) -> str:
    if not brand:
        return "n/a"
    filled, total = 0, 0
    for v in brand.values():
        total += 1
        if v is None:
            continue
        if isinstance(v, list) and len(v) == 0:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, dict) and not any(vv for vv in v.values()):
            continue
        filled += 1
    return f"{filled}/{total}"


async def benchmark():
    rows = []
    for label, url in TEST_URLS:
        for max_pages in (1, 5):
            start = time.time()
            try:
                res = await scrape(
                    url,
                    text_mode="raw",
                    llm_extract=True,
                    multimodal=True,
                    max_pages=max_pages,
                )
                rows.append({
                    "site": label,
                    "pages": max_pages,
                    "total_ms": res["timings"].get("total_ms"),
                    "imgs": res["images_count"],
                    "vids": res["videos_count"],
                    "brand_filled": field_fill_rate(res.get("brand_data")),
                    "visual": "yes" if res.get("visual_data") else "no",
                    "err": "",
                })
            except Exception as e:
                rows.append({
                    "site": label, "pages": max_pages, "total_ms": "-",
                    "imgs": "-", "vids": "-", "brand_filled": "-",
                    "visual": "-", "err": str(e)[:60],
                })

    table = Table(title="scraper_v5 benchmark")
    for col in ("site", "pages", "total_ms", "imgs", "vids", "brand_filled", "visual", "err"):
        table.add_column(col)
    for r in rows:
        table.add_row(
            r["site"], str(r["pages"]), str(r["total_ms"]), str(r["imgs"]),
            str(r["vids"]), r["brand_filled"], r["visual"], r["err"],
        )
    console.print(table)


# await benchmark()
'''


def main() -> None:
    notebook = {
        "cells": [
            md_cell(CELL_BADGE),
            code_cell(CELL_INSTALL),
            code_cell(CELL_CONFIG),
            code_cell(CELL_MAIN),
            code_cell(CELL_EXAMPLE),
            code_cell(CELL_BENCHMARK),
        ],
        "metadata": {
            "colab": {"provenance": [], "include_colab_link": True},
            "kernelspec": {"name": "python3", "display_name": "Python 3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 0,
    }
    NB_PATH.write_text(json.dumps(notebook, indent=1), encoding="utf-8")
    print(f"wrote {NB_PATH}")


if __name__ == "__main__":
    main()
