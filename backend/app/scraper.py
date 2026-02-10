from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://www.finn.no/recommerce/forsale/search"
KNOWN_CATEGORIES = [("Torget", BASE_URL)]

TORGET_SUBCATEGORIES: List[tuple[str, str]] = [
    ("Antikviteter og kunst", "https://www.finn.no/recommerce/forsale/search?category=0.76"),
    ("Dyr og utstyr", "https://www.finn.no/recommerce/forsale/search?category=0.77"),
    ("Elektronikk og hvitevarer", "https://www.finn.no/recommerce/forsale/search?category=0.93"),
    ("Foreldre og barn", "https://www.finn.no/recommerce/forsale/search?category=0.68"),
    ("Fritid, hobby og underholdning", "https://www.finn.no/recommerce/forsale/search?category=0.86"),
    ("Hage, oppussing og hus", "https://www.finn.no/recommerce/forsale/search?category=0.67"),
    ("Klær, kosmetikk og tilbehør", "https://www.finn.no/recommerce/forsale/search?category=0.71"),
    ("Møbler og interiør", "https://www.finn.no/recommerce/forsale/search?category=0.78"),
    ("Næringsvirksomhet", "https://www.finn.no/recommerce/forsale/search?category=0.91"),
    ("Sport og friluftsliv", "https://www.finn.no/recommerce/forsale/search?category=0.69"),
    ("Utstyr til bil, båt og MC", "https://www.finn.no/recommerce/forsale/search?category=0.90"),
]


@dataclass
class Listing:
    category: str
    subcategory: str
    url: str
    title: str
    image: str
    price: str
    published: str
    status: str


@dataclass
class CategoryItem:
    name: str
    url: str


@dataclass
class ParseFilters:
    fiks_ferdig: bool = False
    price_from: Optional[int] = None
    price_to: Optional[int] = None
    published_today: bool = False


@dataclass
class RecheckItem:
    category: str
    title: str
    url: str
    likes: int
    date: str
    status: str
    photo: str


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
ITEM_URL_RE = re.compile(r"/recommerce/forsale/item/\d+")
PRICE_RE = re.compile(r"\d[\d\s\u00a0]*\s*kr", re.IGNORECASE)
LIKES_RE = re.compile(r"^\d+$")
LAST_UPDATED_RE = re.compile(r"Sist\s+endret\s*:\s*([^・·]+)", re.IGNORECASE)
ProgressCallback = Optional[Callable[[dict], Any]]


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_url(base_url: str, href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return urljoin(base_url, href)


def _dedupe_dict_rows(rows: Iterable[dict], key: str) -> List[dict]:
    seen = set()
    out: List[dict] = []
    for row in rows:
        row_key = row.get(key)
        if not row_key or row_key in seen:
            continue
        seen.add(row_key)
        out.append(row)
    return out


async def _emit_progress(progress_cb: ProgressCallback, payload: dict) -> None:
    if not progress_cb:
        return
    try:
        result = progress_cb(payload)
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        return


async def _fetch_httpx_response(url: str, timeout_ms: int = 30000) -> tuple[str, int]:
    timeout = httpx.Timeout(timeout_ms / 1000)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    ) as client:
        response = await client.get(url)
        return response.text, response.status_code


async def _fetch_playwright_html(url: str, timeout_ms: int = 30000) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--headless=new"],
            ignore_default_args=["--headless=old"],
        )
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        # Let the search grid hydrate.
        await page.wait_for_timeout(1200)
        html = await page.content()
        await browser.close()
        return html


async def fetch_html(url: str, wait_for: Optional[str] = None, timeout_ms: int = 30000) -> str:
    del wait_for
    try:
        html, _ = await _fetch_httpx_response(url, timeout_ms=timeout_ms)
        return html
    except Exception:
        return await _fetch_playwright_html(url, timeout_ms=timeout_ms)


def extract_categories(html: str, base_url: str = BASE_URL) -> List[CategoryItem]:
    del html, base_url
    return [CategoryItem(name=name, url=url) for name, url in KNOWN_CATEGORIES]


def extract_subcategories(html: str, category_url: str) -> List[CategoryItem]:
    del html, category_url
    return [CategoryItem(name=name, url=url) for name, url in TORGET_SUBCATEGORIES]


def build_search_url(
    category_url: str,
    filters: ParseFilters,
    page: int,
) -> str:
    parsed = urlparse(category_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if filters.fiks_ferdig:
        params["shipping_types"] = "0"
    else:
        params.pop("shipping_types", None)

    if filters.price_from is not None:
        params["price_from"] = str(filters.price_from)
    else:
        params.pop("price_from", None)

    if filters.price_to is not None:
        params["price_to"] = str(filters.price_to)
    else:
        params.pop("price_to", None)

    if filters.published_today:
        params["published"] = "1"
    else:
        params.pop("published", None)

    if page > 1:
        params["page"] = str(page)
    else:
        params.pop("page", None)

    rebuilt = parsed._replace(query=urlencode(params, doseq=True))
    return urlunparse(rebuilt)


def _extract_title(container: Optional[BeautifulSoup], anchor: BeautifulSoup) -> str:
    if container:
        h1 = container.find(attrs={"data-testid": "object-title"})
        if h1:
            title = _clean(h1.get_text(" "))
            if title:
                return title
        for tag_name in ["h1", "h2", "h3"]:
            heading = container.find(tag_name)
            if heading:
                title = _clean(heading.get_text(" "))
                if title:
                    return title

    aria = _clean(anchor.get("aria-label"))
    if aria:
        return aria

    return _clean(anchor.get_text(" "))


def _extract_price(container: Optional[BeautifulSoup]) -> str:
    if not container:
        return ""

    for node in container.find_all(string=True):
        text = _clean(node)
        if PRICE_RE.search(text):
            match = PRICE_RE.search(text)
            if match:
                return _clean(match.group(0))
        if text.lower() == "til salgs":
            return "Til salgs"
    return ""


def _extract_image(container: Optional[BeautifulSoup], base_url: str) -> str:
    if not container:
        return ""
    img = container.find("img")
    if not img:
        return ""
    src = img.get("src") or img.get("data-src") or ""
    return _normalize_url(base_url, _clean(src))


def _extract_relative_time(container: Optional[BeautifulSoup]) -> str:
    if not container:
        return ""

    spans = container.select("span.whitespace-nowrap")
    for span in reversed(spans):
        text = _clean(span.get_text(" "))
        if re.search(r"^(nå|\d+\s*(min|t|dg))\.?$", text, flags=re.IGNORECASE):
            return text
    return ""


def _walk_cards_from_next_data(data: Any, base_url: str) -> List[dict]:
    rows: List[dict] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            url = ""
            for key in ["url", "href", "link"]:
                value = node.get(key)
                if isinstance(value, str) and ITEM_URL_RE.search(value):
                    url = _normalize_url(base_url, value)
                    break

            title = ""
            for key in ["title", "name", "heading"]:
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    title = _clean(value)
                    break

            if url and title:
                price = ""
                for key in ["price", "priceLabel", "formattedPrice", "priceText"]:
                    value = node.get(key)
                    if value is not None:
                        price = _clean(value)
                        if price:
                            break

                image = ""
                for key in ["image", "imageUrl", "thumbnailUrl"]:
                    value = node.get(key)
                    if isinstance(value, str) and value.strip():
                        image = _normalize_url(base_url, value)
                        break
                    if isinstance(value, list) and value:
                        image = _normalize_url(base_url, _clean(value[0]))
                        break

                rows.append(
                    {
                        "url": url,
                        "title": title,
                        "price": price,
                        "image": image,
                        "published": "",
                        "status": "Aktiv",
                    }
                )

            for value in node.values():
                walk(value)

        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return rows


def _extract_cards_from_html(html: str, base_url: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[dict] = []

    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag and next_data_tag.string:
        try:
            next_data = json.loads(next_data_tag.string)
            rows.extend(_walk_cards_from_next_data(next_data, base_url))
        except Exception:
            pass

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "")
        if not ITEM_URL_RE.search(href):
            continue

        url = _normalize_url(base_url, href)

        container = anchor
        for _ in range(7):
            parent = container.parent if hasattr(container, "parent") else None
            if not parent:
                break
            container = parent
            classes = " ".join(container.get("class", [])) if hasattr(container, "get") else ""
            if container.name == "article" or "sf-search-ad" in classes or "relative" in classes:
                break

        title = _extract_title(container, anchor)
        if not title:
            continue

        rows.append(
            {
                "url": url,
                "title": title,
                "price": _extract_price(container),
                "image": _extract_image(container, base_url),
                "published": _extract_relative_time(container),
                "status": "Aktiv",
            }
        )

    return _dedupe_dict_rows(rows, "url")


async def scrape_search_pages(
    category_name: str,
    category_url: str,
    filters: ParseFilters,
    max_items: int = 200,
    max_pages: int = 40,
    progress_cb: ProgressCallback = None,
) -> List[Listing]:
    listings: List[Listing] = []
    seen_urls: set[str] = set()

    for page in range(1, max_pages + 1):
        page_url = build_search_url(category_url, filters, page)
        try:
            html, _ = await _fetch_httpx_response(page_url)
            rows = _extract_cards_from_html(html, page_url)
            # Fallback to browser rendering when response is mostly JS shell.
            if not rows:
                rendered = await _fetch_playwright_html(page_url)
                rows = _extract_cards_from_html(rendered, page_url)
        except Exception:
            try:
                rendered = await _fetch_playwright_html(page_url)
                rows = _extract_cards_from_html(rendered, page_url)
            except Exception:
                rows = []

        if not rows:
            await _emit_progress(
                progress_cb,
                {
                    "category": category_name,
                    "page": page,
                    "added": 0,
                    "total": len(listings),
                    "done": True,
                    "reason": "empty",
                },
            )
            break

        added_this_page = 0
        for row in rows:
            url = row.get("url", "")
            if not url or url in seen_urls:
                continue

            seen_urls.add(url)
            added_this_page += 1
            listings.append(
                Listing(
                    category="Torget",
                    subcategory=category_name,
                    url=url,
                    title=row.get("title", ""),
                    image=row.get("image", ""),
                    price=row.get("price", ""),
                    published=row.get("published", ""),
                    status="Aktiv",
                )
            )

            if len(listings) >= max_items:
                await _emit_progress(
                    progress_cb,
                    {
                        "category": category_name,
                        "page": page,
                        "added": added_this_page,
                        "total": len(listings),
                        "done": True,
                        "reason": "max_items",
                    },
                )
                return listings

        await _emit_progress(
            progress_cb,
            {
                "category": category_name,
                "page": page,
                "added": added_this_page,
                "total": len(listings),
                "done": False,
                "reason": "page",
            },
        )

        # Page param ignored or end reached.
        if added_this_page == 0:
            await _emit_progress(
                progress_cb,
                {
                    "category": category_name,
                    "page": page,
                    "added": added_this_page,
                    "total": len(listings),
                    "done": True,
                    "reason": "no_new",
                },
            )
            break

    await _emit_progress(
        progress_cb,
        {
            "category": category_name,
            "page": 0,
            "added": 0,
            "total": len(listings),
            "done": True,
            "reason": "finished",
        },
    )
    return listings


def _extract_status(soup: BeautifulSoup, status_code: int, raw_html: str = "") -> str:
    if status_code == 404:
        return "404"

    h1 = soup.find("h1")
    if h1 and _clean(h1.get_text(" ")) == "404":
        return "404"

    for badge in soup.find_all("div"):
        classes = " ".join(badge.get("class", []))
        if "badge--negative" in classes:
            text = _clean(badge.get_text(" ")).lower()
            if "inaktiv" in text:
                return "Inaktiv"
        if "badge--warning" in classes:
            text = _clean(badge.get_text(" ")).lower()
            if "solgt" in text:
                return "Solgt"

    text = _clean(soup.get_text(" ")).lower()
    if "finn finner du alt" in text and "404" in text:
        return "404"

    raw_low = (raw_html or "").lower()
    if "badge--negative" in raw_low and ">inaktiv<" in raw_low:
        return "Inaktiv"
    if "badge--warning" in raw_low and ">solgt<" in raw_low:
        return "Solgt"
    if "<h1>404</h1>" in raw_low:
        return "404"

    # IMPORTANT: do not infer Solgt/Inaktiv from title/body text (e.g. \"vurderes solgt\").
    return "Aktiv"


def _extract_likes(soup: BeautifulSoup, raw_html: str = "") -> int:
    like_span = soup.select_one("button[aria-haspopup='dialog'] span.pl-4")
    if like_span:
        text = _clean(like_span.get_text(" "))
        if LIKES_RE.match(text):
            return int(text)

    like_span = soup.select_one("button span.pl-4")
    if like_span:
        text = _clean(like_span.get_text(" "))
        if LIKES_RE.match(text):
            return int(text)

    like_span = soup.select_one("span.pl-4")
    if like_span:
        text = _clean(like_span.get_text(" "))
        if LIKES_RE.match(text):
            return int(text)

    for span in soup.find_all("span"):
        text = _clean(span.get_text(" "))
        if LIKES_RE.match(text):
            parent_text = _clean((span.parent.get_text(" ") if span.parent else "")).lower()
            if "favoritt" in parent_text or "hjerte" in parent_text:
                return int(text)

    raw_match = re.search(
        r"<button[^>]*aria-haspopup=[\"']dialog[\"'][^>]*>.*?<span[^>]*class=[\"'][^\"']*pl-4[^\"']*[\"'][^>]*>\s*(\d+)\s*</span>",
        raw_html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if raw_match:
        try:
            return int(raw_match.group(1))
        except Exception:
            pass

    raw_match = re.search(
        r"class=[\"'][^\"']*pl-4[^\"']*[\"'][^>]*>\s*(\d+)\s*<",
        raw_html or "",
        flags=re.IGNORECASE,
    )
    if raw_match:
        try:
            return int(raw_match.group(1))
        except Exception:
            pass

    return 0


def _extract_last_updated(soup: BeautifulSoup, raw_html: str = "") -> str:
    for p in soup.find_all("p"):
        text = _clean(p.get_text(" "))
        match = LAST_UPDATED_RE.search(text)
        if match:
            return _clean(match.group(1))

    raw_clean = _clean(re.sub(r"<!--.*?-->", " ", raw_html or "", flags=re.DOTALL))
    match = LAST_UPDATED_RE.search(raw_clean)
    if match:
        return _clean(match.group(1))

    return ""


def _extract_photo_url(soup: BeautifulSoup, base_url: str) -> str:
    meta = soup.find("meta", attrs={"property": "og:image"})
    if meta and meta.get("content"):
        return _normalize_url(base_url, _clean(meta.get("content")))

    main_img = soup.find("img", src=re.compile(r"images\.finncdn\.no", re.IGNORECASE))
    if main_img:
        src = main_img.get("src") or main_img.get("data-src") or ""
        return _normalize_url(base_url, _clean(src))

    return ""


def _extract_title_detail(soup: BeautifulSoup) -> str:
    title = soup.find(attrs={"data-testid": "object-title"})
    if title:
        return _clean(title.get_text(" "))

    h1 = soup.find("h1")
    if h1:
        return _clean(h1.get_text(" "))

    if soup.title:
        return _clean(soup.title.get_text(" "))

    return ""


async def fetch_listing_detail(url: str) -> dict:
    def _parse_detail(html: str, status_code: int) -> dict:
        soup = BeautifulSoup(html, "lxml")
        return {
            "url": url,
            "title": _extract_title_detail(soup),
            "likes": _extract_likes(soup, html),
            "date": _extract_last_updated(soup, html),
            "status": _extract_status(soup, status_code, html),
            "photo": _extract_photo_url(soup, url),
        }

    try:
        html, status_code = await _fetch_httpx_response(url)
    except Exception:
        try:
            html = await _fetch_playwright_html(url)
            status_code = 200
        except Exception:
            return {
                "url": url,
                "title": "",
                "likes": 0,
                "date": "",
                "status": "404",
                "photo": "",
            }

    details = _parse_detail(html, status_code)

    # Some pages return a thin HTML shell through plain HTTP; retry with browser render.
    weak_details = (
        details.get("status") == "Aktiv"
        and int(details.get("likes") or 0) == 0
        and not details.get("date")
        and not details.get("title")
    )
    if weak_details:
        try:
            rendered_html = await _fetch_playwright_html(url)
            details = _parse_detail(rendered_html, status_code)
        except Exception:
            pass

    return details


async def recheck_rows(
    rows: List[dict],
    concurrency: int = 5,
    include_active: bool = False,
    progress_cb: ProgressCallback = None,
) -> List[RecheckItem]:
    semaphore = asyncio.Semaphore(concurrency)

    async def _task(idx: int, row: dict) -> tuple[int, Optional[RecheckItem], str, str]:
        async with semaphore:
            url = _clean(row.get("url") or row.get("ссылка") or row.get("link"))
            if not url:
                return idx, None, "skip", ""

            details = await fetch_listing_detail(url)
            status = details.get("status", "Aktiv")

            title = _clean(row.get("title") or row.get("название")) or _clean(details.get("title"))
            category = _clean(row.get("category") or row.get("категория"))

            item = RecheckItem(
                category=category,
                title=title,
                url=url,
                likes=int(details.get("likes") or 0),
                date=_clean(details.get("date")),
                status=status,
                photo=_clean(details.get("photo")),
            )
            return idx, item, status, url

    tasks = [asyncio.create_task(_task(idx, row)) for idx, row in enumerate(rows, start=1)]
    total = len(tasks)
    done = 0
    collected: List[tuple[int, RecheckItem]] = []

    for task in asyncio.as_completed(tasks):
        idx, item, status, url = await task
        done += 1
        await _emit_progress(
            progress_cb,
            {
                "done": done,
                "total": total,
                "status": status,
                "url": url,
            },
        )
        if item is None:
            continue
        if not include_active and item.status == "Aktiv":
            continue
        collected.append((idx, item))

    collected.sort(key=lambda pair: pair[0])
    return [item for _, item in collected]


async def scrape_listings(
    category_name: str,
    subcategory_name: str,
    subcategory_url: str,
    max_items: int = 50,
) -> List[Listing]:
    del category_name
    filters = ParseFilters()
    return await scrape_search_pages(
        category_name=subcategory_name or "Torget",
        category_url=subcategory_url,
        filters=filters,
        max_items=max_items,
    )


def detect_changes(old_rows: List[dict], new_rows: List[dict]) -> List[dict]:
    old_by_url = {
        _clean(row.get("url") or row.get("ссылка") or row.get("link")): row
        for row in old_rows
        if _clean(row.get("url") or row.get("ссылка") or row.get("link"))
    }
    new_by_url = {
        _clean(row.get("url") or row.get("ссылка") or row.get("link")): row
        for row in new_rows
        if _clean(row.get("url") or row.get("ссылка") or row.get("link"))
    }

    changed: List[dict] = []
    for url, old in old_by_url.items():
        new = new_by_url.get(url)
        if not new:
            continue

        old_price = _clean(old.get("price") or old.get("цена"))
        new_price = _clean(new.get("price") or new.get("цена"))
        old_status = _clean(old.get("status") or old.get("статус"))
        new_status = _clean(new.get("status") or new.get("статус"))

        if old_price != new_price or old_status != new_status:
            changed.append(
                {
                    "url": url,
                    "title": _clean(new.get("title") or new.get("название") or old.get("title") or old.get("название")),
                    "old_price": old_price,
                    "new_price": new_price,
                    "old_status": old_status,
                    "new_status": new_status,
                }
            )

    return changed
