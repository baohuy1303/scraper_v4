"""Build scraper_v4.ipynb from inline Python sources."""
import json
from pathlib import Path

NB_PATH = Path(__file__).parent / "scraper_v4.ipynb"


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
    '<a href="https://colab.research.google.com/github/baohuy1303/scraper_v4/blob/main/scraper_v4.ipynb" '
    'target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>'
)

CELL_INSTALL = """!pip install camoufox rich aiohttp openai aiofiles
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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import aiofiles
from camoufox.async_api import AsyncCamoufox
from openai import AsyncOpenAI
from playwright.async_api import Page, Response
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


@dataclass
class DownloadTask:
    url: str
    filepath: Path
    body: bytes
    resource_type: str
    filename: str


async def download_worker(
    queue: asyncio.Queue,
    captured_files: dict[str, list[dict[str, Any]]],
    session_dir: Path,
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


async def extract_text(page: Page) -> dict[str, Any]:
    """Raw text extraction — current v3 behavior, unchanged."""
    console.print("[cyan]Extracting text (raw)...[/cyan]")
    all_texts = await page.locator("body").all_inner_texts()
    cleaned_texts = [text.strip() for text in all_texts if text.strip()]
    return {"name": "extract_text", "data": cleaned_texts, "count": len(cleaned_texts)}


async def extract_text_chunked(page: Page) -> dict[str, Any]:
    """DOM-aware chunked extraction: split on h1/h2/h3, strip nav/header/footer/script/style."""
    console.print("[cyan]Extracting text (chunked)...[/cyan]")
    chunks = await page.evaluate(
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
    return {"name": "extract_text_chunked", "data": chunks, "count": len(chunks)}


async def extract_metadata(page: Page) -> dict[str, Any]:
    console.print("[cyan]Extracting metadata...[/cyan]")
    metadata = await page.evaluate(
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
    return {"name": "extract_metadata", "data": metadata}


async def adaptive_scroll(page: Page) -> None:
    img_count = await page.locator("img").count()
    video_count = await page.locator("video").count()
    total_media = img_count + video_count

    console.print(
        f"[dim]Found {img_count} images and {video_count} videos on initial page load[/dim]"
    )

    scroll_percent, delay = get_scroll_config(total_media)

    console.print(
        f"[dim]Using {scroll_percent*100}% scroll step with {delay}ms delay[/dim]"
    )

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


# ----------------------------- LLM extraction ----------------------------- #

BRAND_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "company_name": {"type": ["string", "null"]},
        "what_they_do": {
            "type": ["string", "null"],
            "description": "1-2 sentence description of what the company does or sells.",
        },
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
        "pricing_summary": {"type": ["string", "null"]},
        "target_customer": {"type": ["string", "null"]},
        "value_prop": {"type": ["string", "null"]},
        "tone_voice": {
            "type": ["string", "null"],
            "description": "Descriptors of the brand's tone, e.g. 'playful, technical, premium'.",
        },
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
        "company_name",
        "what_they_do",
        "products_services",
        "pricing_summary",
        "target_customer",
        "value_prop",
        "tone_voice",
        "testimonials",
        "faqs",
        "common_phrases",
        "competitors_mentioned",
        "calls_to_action",
    ],
    "additionalProperties": False,
}

VISUAL_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "visual_style_notes": {
            "type": ["string", "null"],
            "description": "Narrative description of the brand's visual aesthetic.",
        },
        "color_palette": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Dominant colors as hex codes, e.g. '#FF5733'.",
        },
        "tone_descriptors_visual": {"type": "array", "items": {"type": "string"}},
        "layout_style": {
            "type": ["string", "null"],
            "description": "E.g. 'modern SaaS landing', 'editorial blog', 'high-density e-com'.",
        },
    },
    "required": [
        "visual_style_notes",
        "color_palette",
        "tone_descriptors_visual",
        "layout_style",
    ],
    "additionalProperties": False,
}


async def llm_extract_brand_data(
    text_for_llm: str,
    metadata: dict[str, Any],
    client: AsyncOpenAI,
) -> dict[str, Any]:
    start = time.time()

    prompt = (
        "Extract structured brand data from the following website text and metadata. "
        "Be faithful to the source — do not invent details. If a field cannot be determined, return null "
        "(or an empty array for list fields).\n\n"
        f"## Page metadata\n{json.dumps(metadata, indent=2)}\n\n"
        f"## Page text\n{text_for_llm}"
    )

    response = await client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "brand_data",
                "schema": BRAND_DATA_SCHEMA,
                "strict": True,
            },
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
    screenshot_bytes: bytes,
    client: AsyncOpenAI,
) -> dict[str, Any]:
    start = time.time()

    image_b64 = base64.b64encode(screenshot_bytes).decode()
    data_url = f"data:image/png;base64,{image_b64}"

    response = await client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Analyze the visual style of this website screenshot. "
                            "Extract the dominant color palette as hex codes, describe the visual "
                            "aesthetic, tone descriptors, and layout style."
                        ),
                    },
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "visual_data",
                "schema": VISUAL_DATA_SCHEMA,
                "strict": True,
            },
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


# ----------------------------- Main scrape entrypoint ----------------------------- #


async def scrape(
    url: str,
    timeout: int = 20000,
    headless: bool | Literal["virtual"] = True,
    text_mode: Literal["raw", "chunked"] = "raw",
    llm_extract: bool = True,
    multimodal: bool = False,
    camoufox_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    captured_files: dict[str, list[dict[str, Any]]] = {"images": [], "videos": []}
    camoufox_options = camoufox_options or {}

    seen_urls: set[str] = set()
    pending_responses: set[asyncio.Task] = set()  # fix: track in-flight body reads

    parsed_url = urlparse(url)
    domain = parsed_url.netloc or parsed_url.path
    domain = re.sub(r"[^\w\-.]", "_", domain)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_dir = OUTPUT_DIR / f"{domain}_{timestamp}"

    (session_dir / "images").mkdir(parents=True, exist_ok=True)
    (session_dir / "videos").mkdir(parents=True, exist_ok=True)

    download_queue: asyncio.Queue = asyncio.Queue()

    workers = [
        asyncio.create_task(
            download_worker(download_queue, captured_files, session_dir)
        )
        for _ in range(NUM_WORKERS)
    ]

    openai_client: AsyncOpenAI | None = None
    if llm_extract or multimodal:
        openai_client = AsyncOpenAI()

    text_data: dict[str, Any] = {"name": "extract_text", "data": [], "count": 0}
    text_chunks: list[dict[str, Any]] | None = None
    metadata: dict[str, Any] = {"name": "extract_metadata", "data": {}}
    screenshot_path = session_dir / "screenshot.png"
    screenshot_bytes: bytes | None = None

    llm_tasks: dict[str, asyncio.Task] = {}

    async with AsyncCamoufox(headless=headless, **camoufox_options) as browser:
        page = await browser.new_page()

        def handle_response(r: Response) -> None:
            task = asyncio.create_task(
                on_response(r, download_queue, seen_urls, session_dir)
            )
            pending_responses.add(task)
            task.add_done_callback(pending_responses.discard)

        page.on("response", handle_response)

        try:
            console.print(f"[cyan]Starting navigation (timeout={timeout}ms)...[/cyan]")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await page.wait_for_timeout(PAGE_LOAD_WAIT)

            console.print("[cyan]Scrolling to load lazy content...[/cyan]")
            await adaptive_scroll(page)
            console.print("[dim green]✓ Scroll complete[/dim green]")

            try:
                await page.wait_for_load_state("networkidle", timeout=2000)
                console.print("[dim green]✓ Network idle reached[/dim green]")
            except Exception:
                console.print(
                    "[dim yellow]⚠ Network still active, continuing anyway[/dim yellow]"
                )

            console.print("[cyan]Extracting page data...[/cyan]")

            text_extractor = (
                extract_text_chunked(page) if text_mode == "chunked" else extract_text(page)
            )

            text_data, metadata, screenshot_bytes = await asyncio.gather(
                text_extractor,
                extract_metadata(page),
                page.screenshot(full_page=False),
            )
            console.print("[dim green]✓ Page data extracted[/dim green]")

            # Write screenshot to disk in the background — non-blocking
            async def _save_screenshot(b: bytes, p: Path) -> None:
                async with aiofiles.open(p, "wb") as f:
                    await f.write(b)

            screenshot_save_task = asyncio.create_task(
                _save_screenshot(screenshot_bytes, screenshot_path)
            )

            # Prepare text for LLM
            if text_mode == "chunked":
                text_chunks = text_data.get("data", [])
                parts: list[str] = []
                for c in text_chunks:
                    if c.get("heading"):
                        parts.append(f"## {c['heading']}")
                    if c.get("text"):
                        parts.append(c["text"])
                text_for_llm = "\n\n".join(parts)
                text_for_json: list[str] = parts  # flat list for `text` field
            else:
                text_for_llm = "\n\n".join(text_data.get("data", []))
                text_for_json = text_data.get("data", [])

            # Kick off LLM calls in parallel with media-download drain
            if llm_extract and openai_client is not None:
                llm_tasks["brand"] = asyncio.create_task(
                    llm_extract_brand_data(text_for_llm, metadata.get("data", {}), openai_client)
                )
            if multimodal and openai_client is not None and screenshot_bytes:
                llm_tasks["visual"] = asyncio.create_task(
                    llm_visual_analysis(screenshot_bytes, openai_client)
                )

        except Exception as e:
            console.print(f"[yellow]Warning during scraping: {e}[/yellow]")
            console.print("[yellow]Continuing to save captured media...[/yellow]")
            text_data = {
                "name": "extract_text",
                "data": [],
                "count": 0,
                "error": str(e),
            }
            metadata = {"name": "extract_metadata", "data": {}, "error": str(e)}
            text_for_json = []
            screenshot_save_task = None
        finally:
            # Drain in-flight response handlers BEFORE the browser context exits
            # so response bodies are not evicted (v3 bug).
            if pending_responses:
                console.print(
                    f"[cyan]Waiting for {len(pending_responses)} in-flight response handlers...[/cyan]"
                )
                await asyncio.gather(*list(pending_responses), return_exceptions=True)

            console.print("[cyan]Waiting for all downloads to complete...[/cyan]")
            await download_queue.join()

            if "screenshot_save_task" in locals() and screenshot_save_task:
                try:
                    await screenshot_save_task
                    console.print("[dim green]✓ Screenshot saved[/dim green]")
                except Exception as e:
                    console.print(f"[dim yellow]Screenshot save failed: {e}[/dim yellow]")

        for _ in range(NUM_WORKERS):
            await download_queue.put(None)
        await asyncio.gather(*workers)

    # ------- Outside browser context: collect LLM results (still running in parallel) -------
    brand_data = None
    brand_data_meta = None
    visual_data = None
    visual_data_meta = None

    if llm_tasks:
        console.print(
            f"[cyan]Awaiting {len(llm_tasks)} LLM task(s)...[/cyan]"
        )
        for key, task in llm_tasks.items():
            try:
                res = await task
                if key == "brand":
                    brand_data = res["data"]
                    brand_data_meta = res["meta"]
                    console.print(
                        f"[dim green]✓ Brand data extracted in {res['meta']['latency_ms']}ms[/dim green]"
                    )
                elif key == "visual":
                    visual_data = res["data"]
                    visual_data_meta = res["meta"]
                    console.print(
                        f"[dim green]✓ Visual data extracted in {res['meta']['latency_ms']}ms[/dim green]"
                    )
            except Exception as e:
                console.print(f"[red]LLM task '{key}' failed: {e}[/red]")
                if key == "brand":
                    brand_data_meta = {"error": str(e)}
                else:
                    visual_data_meta = {"error": str(e)}

    total_count = len(captured_files["images"]) + len(captured_files["videos"])
    console.print(f"\n[bold green]✓ Done![/bold green]")
    console.print(f"[cyan]Total media files:[/cyan] {total_count}")
    console.print(f"[cyan]Images:[/cyan] {len(captured_files['images'])}")
    console.print(f"[cyan]Videos:[/cyan] {len(captured_files['videos'])}")
    console.print(f"[cyan]Saved to:[/cyan] {session_dir}")

    result_data = {
        "url": url,
        "timestamp": timestamp,
        "session_dir": str(session_dir),
        "images": captured_files["images"],
        "videos": captured_files["videos"],
        "images_count": len(captured_files["images"]),
        "videos_count": len(captured_files["videos"]),
        "total_count": total_count,
        "text": text_for_json if "text_for_json" in locals() else [],
        "text_chunks": text_chunks,
        "metadata": metadata.get("data", {}),
        "screenshot": str(screenshot_path) if screenshot_path.exists() else None,
        "brand_data": brand_data,
        "brand_data_meta": brand_data_meta,
        "visual_data": visual_data,
        "visual_data_meta": visual_data_meta,
        "config": {
            "text_mode": text_mode,
            "llm_extract": llm_extract,
            "multimodal": multimodal,
        },
    }

    result_json = session_dir / "scrape_result.json"
    async with aiofiles.open(result_json, "w") as f:
        await f.write(json.dumps(result_data, indent=2))

    console.print(f"[cyan]Results saved to:[/cyan] {result_json}")

    return result_data
'''

CELL_EXAMPLE = '''# Don't need these 2 lines when on local python
import nest_asyncio
nest_asyncio.apply()

# url = "https://magichour.ai"
# url = "https://www.amazon.ca/OCOOPA-Rechargeable-Magnetic-Handwarmers-Certified/dp/B0CC189314/"
url = "https://medium.com/@amit25173/scrapy-vs-beautifulsoup-vs-selenium-579bce149262"
# url = "https://www.cbc.ca/news/canada/british-columbia/islamophobia-in-b-c-1.6576808"

# use with asyncio.run() instead on local python
res = await scrape(
    url,
    text_mode="raw",       # "raw" or "chunked"
    llm_extract=True,      # gpt-5.4-mini text -> brand_data
    multimodal=False,      # gpt-5.4-mini vision over screenshot -> visual_data
)
'''

CELL_BENCHMARK = '''# @title Benchmark — compare raw vs chunked text modes
import time
from rich.table import Table

TEST_URLS = [
    ("magichour.ai", "https://magichour.ai"),
    ("medium article", "https://medium.com/@amit25173/scrapy-vs-beautifulsoup-vs-selenium-579bce149262"),
    # Add a SaaS landing page and an e-com page here:
    # ("higgsfield", "https://higgsfield.ai"),
    # ("creatify", "https://creatify.ai"),
]


def field_fill_rate(brand: dict | None) -> str:
    if not brand:
        return "n/a"
    filled = 0
    total = 0
    for v in brand.values():
        total += 1
        if v is None:
            continue
        if isinstance(v, list) and len(v) == 0:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        filled += 1
    return f"{filled}/{total}"


async def benchmark():
    rows = []
    for label, url in TEST_URLS:
        for mode in ("raw", "chunked"):
            start = time.time()
            try:
                res = await scrape(
                    url,
                    text_mode=mode,
                    llm_extract=True,
                    multimodal=False,
                )
                elapsed = time.time() - start
                rows.append({
                    "site": label,
                    "mode": mode,
                    "time_s": f"{elapsed:.1f}",
                    "imgs": res["images_count"],
                    "vids": res["videos_count"],
                    "filled": field_fill_rate(res.get("brand_data")),
                    "err": "",
                })
            except Exception as e:
                rows.append({
                    "site": label,
                    "mode": mode,
                    "time_s": "-",
                    "imgs": "-",
                    "vids": "-",
                    "filled": "-",
                    "err": str(e)[:60],
                })

    table = Table(title="scraper_v4 benchmark")
    for col in ("site", "mode", "time_s", "imgs", "vids", "filled", "err"):
        table.add_column(col)
    for r in rows:
        table.add_row(
            r["site"], r["mode"], r["time_s"], str(r["imgs"]),
            str(r["vids"]), r["filled"], r["err"],
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
