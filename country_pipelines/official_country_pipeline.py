from __future__ import annotations

import base64
import re
import sys
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import pandas as pd
import pdfplumber
import requests
import urllib3
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.email_utils import (
    canonical_email,
    extract_emails,
    extract_name,
    extract_rank,
    infer_email_type,
    make_snippet,
    normalize_whitespace,
    slugify_name,
)
from shared.export_utils import write_excel_workbook
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 20
SEARCH_MIN_INTERVAL_SECONDS = 2.0
DUCKDUCKGO_SEARCH_CACHE: dict[str, list["SearchResult"]] = {}
BRAVE_SEARCH_CACHE: dict[str, list["SearchResult"]] = {}
BING_SEARCH_CACHE: dict[str, list["SearchResult"]] = {}
SEED_PAGE_CACHE: dict[str, list[str]] = {}
SEED_INSTITUTION_CACHE: dict[str, list[dict]] = {}
LAST_SEARCH_TIMESTAMPS: dict[str, float] = {}


def country_slug(country_name: str) -> str:
    return slugify_name(country_name).replace(" ", "_")


def build_output_paths(country_name: str, output_dir: Path | str | None = None) -> dict[str, Path]:
    slug = country_slug(country_name)
    resolved_output_dir = Path(output_dir) if output_dir else ROOT / f"{slug}_profs"
    return {
        "output_dir": resolved_output_dir,
        "csv": resolved_output_dir / f"{slug}_professor_emails.csv",
        "xlsx": resolved_output_dir / f"{slug}_professor_emails.xlsx",
    }


KEYWORD_PATTERNS = (
    "staff",
    "faculty",
    "academic",
    "teaching",
    "directory",
    "people",
    "profile",
    "professor",
    "professors",
    "associate-professor",
    "department",
    "departments",
    "school",
    "schools",
    "college",
    "colleges",
    "contact",
)
NAME_TITLE_PREFIXES = ("prof", "dr", "eng", "mr", "mrs", "ms")
PROFILE_LISTING_HINTS = (
    "staff profiles",
    "all staff",
    "faculty staff",
    "faculty profiles",
    "academic staff",
    "our staff",
    "websites",
    "search for professor",
    "search for associate professor",
)
NON_PROFESSOR_HINTS = (
    "lecturer",
    "tutorial fellow",
    "assistant lecturer",
    "senior lecturer",
    "lecturer i",
    "lecturer ii",
    "secretary",
    "admin",
    "administrator",
    "clerk",
    "director",
    "vice chancellor",
    "deputy vice",
    "dean",
    "chairman",
    "chairperson",
    "messenger",
    "cleaner",
)
BLOCKED_LINK_PATTERNS = (
    "login",
    "mail.",
    "/mail",
    "microsoftonline",
    "office.com",
    "payment",
    "paying-for-college",
    "finance",
    "donate",
    "calendar",
    "catalog",
    "fee-schedule",
    "fees-and-charges",
    "admissions",
)

SOURCE_DISCOVERY_QUERIES = (
    '"{country}" accredited universities official',
    '"{country}" status of universities official',
    '"{country}" higher education institutions official',
    '"{country}" ministry of education universities official',
    '"{country}" public universities private universities official',
)

OFFICIAL_SOURCE_HOST_HINTS = (
    ".gov",
    ".go.",
    ".or.",
    ".org",
    ".edu",
    ".ac.",
    "education",
    "ministry",
    "commission",
    "council",
    "accredit",
    "regulat",
)

OFFICIAL_SOURCE_URL_HINTS = (
    "accredited",
    "recognized",
    "universit",
    "college",
    "polytechnic",
    "tvet",
    "institution",
    "higher-education",
    "higher_education",
    "tertiary",
    "education",
)

OFFICIAL_SOURCE_RESULT_HINTS = OFFICIAL_SOURCE_URL_HINTS + (
    "commission for university education",
    "status of universities",
    "public universities",
    "private universities",
    "accreditation",
    "authorised universities",
    "authorized universities",
    "programmes offered in universities",
)

SEED_SOURCE_LINK_HINTS = (
    "accredited",
    "status of universities",
    "authorized",
    "authorised",
    "accredited universities",
    "public universities",
    "private universities",
    "constituent college",
    "university colleges",
)

SEED_SOURCE_BLOCK_HINTS = (
    "layout=edit",
    "press release",
    "news updates",
    "honorary",
    "guidelines",
    "fees",
    "manual",
    "partner",
    "launch",
    "downloads",
    "statistics report",
    "codebook",
    "classification",
    "report.pdf",
)

BLOCKED_SOURCE_HOSTS = (
    "wikipedia.org",
    "facebook.com",
    "linkedin.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "tiktok.com",
    "instagram.com",
    "medium.com",
    "reddit.com",
)

INSTITUTION_NAME_HINTS = (
    "university",
    "polytechnic",
    "college",
    "institute",
    "tvet",
)

GENERIC_INSTITUTION_NAME_BLOCKLIST = (
    "ministry of",
    "commission for",
    "council for",
    "list of",
    "accredited institutions",
    "recognized institutions",
    "higher education",
    "tertiary education",
    "official website",
    "contact us",
    "apply now",
    "institutions colleges universities",
    "public university constituent colleges",
    "private university constituent colleges",
    "universities with letters of interim",
    "which is the",
    "courses offered at",
    "professional courses",
    "their requirements",
    "updated",
    "central placement service",
    "authority",
    "statistics",
    "standards",
    "guidelines",
    "foreign university",
)

AFRICA_COUNTRY_ALPHA2 = {
    "algeria": "dz",
    "angola": "ao",
    "benin": "bj",
    "botswana": "bw",
    "burkina faso": "bf",
    "burundi": "bi",
    "cabo verde": "cv",
    "cameroon": "cm",
    "central african republic": "cf",
    "chad": "td",
    "comoros": "km",
    "democratic republic of the congo": "cd",
    "republic of the congo": "cg",
    "cote d'ivoire": "ci",
    "djibouti": "dj",
    "egypt": "eg",
    "equatorial guinea": "gq",
    "eritrea": "er",
    "eswatini": "sz",
    "ethiopia": "et",
    "gabon": "ga",
    "gambia": "gm",
    "ghana": "gh",
    "guinea": "gn",
    "guinea-bissau": "gw",
    "kenya": "ke",
    "lesotho": "ls",
    "liberia": "lr",
    "libya": "ly",
    "madagascar": "mg",
    "malawi": "mw",
    "mali": "ml",
    "mauritania": "mr",
    "mauritius": "mu",
    "morocco": "ma",
    "mozambique": "mz",
    "namibia": "na",
    "niger": "ne",
    "nigeria": "ng",
    "rwanda": "rw",
    "sao tome and principe": "st",
    "senegal": "sn",
    "seychelles": "sc",
    "sierra leone": "sl",
    "somalia": "so",
    "south africa": "za",
    "south sudan": "ss",
    "sudan": "sd",
    "tanzania": "tz",
    "togo": "tg",
    "tunisia": "tn",
    "uganda": "ug",
    "zambia": "zm",
    "zimbabwe": "zw",
}

COUNTRY_NAME_ALIASES = {
    "cape verde": "cabo verde",
    "ivory coast": "cote d'ivoire",
    "cote divoire": "cote d'ivoire",
    "dr congo": "democratic republic of the congo",
    "drc": "democratic republic of the congo",
    "congo kinshasa": "democratic republic of the congo",
    "congo brazzaville": "republic of the congo",
    "republic of congo": "republic of the congo",
    "the gambia": "gambia",
    "swaziland": "eswatini",
    "são tomé and príncipe": "sao tome and principe",
    "sao tome & principe": "sao tome and principe",
}

OFFICIAL_SOURCE_HOST_LABELS = (
    "education",
    "ministryofeducation",
    "educationministry",
    "moe",
    "highereducation",
    "universityeducation",
    "tertiaryeducation",
    "tvet",
    "cue",
    "che",
    "nche",
    "nuc",
    "tec",
    "hec",
    "ugc",
)

PROFESSOR_OUTPUT_COLUMNS = [
    "country",
    "university",
    "university_type",
    "official_domain",
    "faculty_or_school",
    "department",
    "full_name",
    "normalized_name",
    "rank",
    "normalized_rank",
    "email",
    "email_type",
    "title_line",
    "profile_url",
    "source_url",
    "source_page_title",
    "evidence_snippet",
    "extraction_method",
    "confidence_score",
    "duplicate_group_id",
    "source_priority",
    "notes",
    "date_collected",
]


CanonicalNameFn = Callable[[str], str]
LoadInstitutionsFn = Callable[[], list[dict]]
ProgressCallbackFn = Callable[[dict], None]


@dataclass(frozen=True)
class CountryPipelineConfig:
    country_name: str
    seed_source_name: str
    load_seed_institutions: LoadInstitutionsFn
    canonicalize_institution_name: CanonicalNameFn
    prioritized_institutions: tuple[str, ...] = ()
    institution_seed_urls: dict[str, list[str]] = field(default_factory=dict)
    manual_domain_hints: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SearchResult:
    url: str
    title: str
    snippet: str


def infer_institution_type(institution_name: str) -> str:
    lowered = institution_name.lower()
    if "polytechnic" in lowered:
        return "Polytechnic"
    if "technical and vocational college" in lowered or "tvet" in lowered:
        return "TVET"
    if "university college" in lowered:
        return "University College"
    if "college" in lowered:
        return "College"
    if "institute" in lowered:
        return "Institute"
    return "University"


def canonical_institution_name(name: str) -> str:
    key = slugify_name(name)
    key = key.replace(" univ ", " university ")
    key = key.replace(" univ", " university")
    key = key.replace(" inst ", " institute ")
    key = key.replace(" inst", " institute")
    key = key.replace(" poly ", " polytechnic ")
    return normalize_whitespace(key)


def institution_name_tokens(name: str) -> list[str]:
    stopwords = {
        "university",
        "college",
        "institute",
        "polytechnic",
        "school",
        "of",
        "the",
        "and",
        "for",
        "technology",
        "science",
        "sciences",
    }
    return [
        token
        for token in slugify_name(name).split()
        if len(token) > 2 and token not in stopwords
    ]


def institution_acronym_candidates(name: str) -> set[str]:
    tokens = [token for token in slugify_name(name).split() if token not in {"of", "the", "and", "for"}]
    candidates: set[str] = set()
    if tokens:
        candidates.add("".join(token[0] for token in tokens))
    significant = institution_name_tokens(name)
    if significant:
        candidates.add("".join(token[0] for token in significant))
    return {candidate for candidate in candidates if len(candidate) >= 2}


def resolve_country_alpha2(country_name: str) -> str:
    normalized = slugify_name(country_name)
    normalized = COUNTRY_NAME_ALIASES.get(normalized, normalized)
    return AFRICA_COUNTRY_ALPHA2.get(normalized, "")


def generate_official_source_url_candidates(country_name: str) -> list[str]:
    country_code = resolve_country_alpha2(country_name)
    if not country_code:
        return []
    urls: list[str] = []
    seen: set[str] = set()
    government_labels = ("education", "highereducation")
    regulator_labels = ("cue", "nche", "che", "nuc", "tec")
    government_patterns = (
        "{label}.go.{cc}",
        "www.{label}.go.{cc}",
        "{label}.gov.{cc}",
        "www.{label}.gov.{cc}",
    )
    regulator_patterns = (
        "{label}.or.{cc}",
        "www.{label}.or.{cc}",
        "{label}.edu.{cc}",
        "www.{label}.edu.{cc}",
        "{label}.ac.{cc}",
        "www.{label}.ac.{cc}",
        "{label}.go.{cc}",
        "www.{label}.go.{cc}",
        "{label}.gov.{cc}",
        "www.{label}.gov.{cc}",
    )
    for label in government_labels:
        for pattern in government_patterns:
            url = f"https://{pattern.format(label=label, cc=country_code)}/"
            if url not in seen:
                seen.add(url)
                urls.append(url)
    for label in regulator_labels:
        for pattern in regulator_patterns:
            url = f"https://{pattern.format(label=label, cc=country_code)}/"
            if url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def throttle_search_backend(backend: str) -> None:
    now = time.time()
    last = LAST_SEARCH_TIMESTAMPS.get(backend, 0.0)
    wait_for = SEARCH_MIN_INTERVAL_SECONDS - (now - last)
    if wait_for > 0:
        time.sleep(wait_for)
    LAST_SEARCH_TIMESTAMPS[backend] = time.time()


def reduce_host_domain(host: str) -> str:
    host = host.lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) >= 3 and len(parts[-1]) == 2 and parts[-2] in {"ac", "go", "or", "co"}:
        return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def fetch_url(url: str, *, allow_insecure: bool = True) -> requests.Response:
    last_error: Exception | None = None
    session = requests.Session()
    session.headers.update(HEADERS)
    for attempt in range(3):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.SSLError:
            if not allow_insecure:
                raise
            response = session.get(url, timeout=REQUEST_TIMEOUT, verify=False)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Unable to fetch {url}")


def looks_like_official_source_host(host: str) -> bool:
    lowered = host.lower()
    if any(blocked in lowered for blocked in BLOCKED_SOURCE_HOSTS):
        return False
    return any(token in lowered for token in OFFICIAL_SOURCE_HOST_HINTS)


def looks_like_official_source_url(url: str) -> bool:
    lowered = url.lower()
    return any(token in lowered for token in OFFICIAL_SOURCE_URL_HINTS)


def looks_like_official_source_result(result: SearchResult, country_name: str) -> bool:
    host = urlparse(result.url).netloc.lower()
    if any(blocked in host for blocked in BLOCKED_SOURCE_HOSTS):
        return False
    if not looks_like_official_source_host(host):
        return False
    combined = slugify_name(f"{result.title} {result.snippet} {result.url}")
    country_key = slugify_name(country_name)
    if country_key and country_key not in combined:
        return False
    return looks_like_official_source_url(result.url) or any(
        token in combined for token in OFFICIAL_SOURCE_RESULT_HINTS
    )


def clean_institution_name(text: str) -> str:
    cleaned = normalize_whitespace(text)
    cleaned = re.sub(r"^\d+[\.\)]\s*", "", cleaned)
    cleaned = re.sub(r"\s*\|\s*website.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+-\s+official website.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+\(official.*$", "", cleaned, flags=re.I)
    cleaned = cleaned.strip(" -,:;|")
    return normalize_whitespace(cleaned)


def looks_like_institution_name(text: str) -> bool:
    cleaned = clean_institution_name(text)
    lowered = cleaned.lower()
    if not 6 <= len(cleaned) <= 140:
        return False
    if re.search(r"\d{2,}", cleaned):
        return False
    if any(blocked in lowered for blocked in GENERIC_INSTITUTION_NAME_BLOCKLIST):
        return False
    if not any(token in lowered for token in INSTITUTION_NAME_HINTS):
        return False
    if cleaned.count("@") or "http" in lowered:
        return False
    return True


def extract_institution_names_from_html(html: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    try:
        tables = pd.read_html(StringIO(html))
    except (ValueError, ImportError):
        tables = []

    for table in tables:
        headers = " ".join(str(col) for col in table.columns).lower()
        if not any(token in headers for token in ("institution", "university", "college", "polytechnic", "name")):
            continue
        for value in table.astype(str).fillna("").values.flatten():
            cleaned = clean_institution_name(value)
            if looks_like_institution_name(cleaned):
                key = canonical_institution_name(cleaned)
                if key not in seen:
                    seen.add(key)
                    candidates.append(cleaned)

    soup = BeautifulSoup(html, "html.parser")
    for element in soup.find_all(["a", "li", "td", "option", "p", "h1", "h2", "h3", "h4"]):
        cleaned = clean_institution_name(element.get_text(" ", strip=True))
        if looks_like_institution_name(cleaned):
            key = canonical_institution_name(cleaned)
            if key not in seen:
                seen.add(key)
                candidates.append(cleaned)
    return candidates


def extract_institution_names_from_text(text: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    for raw_line in text.splitlines():
        line = normalize_whitespace(raw_line)
        if not line:
            continue
        fragments = [line]
        fragments.extend(
            piece
            for piece in re.split(r"\s{2,}|\t|\s+\|\s+|;\s+", line)
            if normalize_whitespace(piece) and piece != line
        )
        for fragment in fragments:
            cleaned = clean_institution_name(fragment)
            cleaned = re.sub(r"^\d+[\.\)]\s*", "", cleaned)
            if looks_like_institution_name(cleaned):
                key = canonical_institution_name(cleaned)
                if key not in seen:
                    seen.add(key)
                    candidates.append(cleaned)
    return candidates


def looks_like_institution_list_source(source_url: str, source_text: str) -> bool:
    combined = normalize_whitespace(f"{source_url} {source_text[:1500]}").lower()
    if any(token in combined for token in SEED_SOURCE_BLOCK_HINTS):
        return False
    return any(token in combined for token in SEED_SOURCE_LINK_HINTS)


def search_brave_results(query: str, max_results: int = 10) -> list[SearchResult]:
    if query in BRAVE_SEARCH_CACHE:
        return BRAVE_SEARCH_CACHE[query][:max_results]
    search_url = f"https://search.brave.com/search?q={quote_plus(query)}&source=web"
    last_error: Exception | None = None
    response = None
    for attempt in range(2):
        try:
            response = fetch_url(search_url)
            break
        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 429 and attempt == 0:
                time.sleep(4)
                continue
            raise
    if response is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Unable to fetch Brave search results for query: {query}")
    soup = BeautifulSoup(response.text, "html.parser")
    results: list[SearchResult] = []
    seen: set[str] = set()
    for container in soup.select('div[data-type="web"]'):
        anchor = container.select_one('a.l1[href^="http"]') or container.select_one('a[href^="http"]')
        if anchor is None:
            continue
        link = normalize_whitespace(anchor.get("href") or "")
        if not link.startswith("http"):
            continue
        if "search.brave.com" in link or link in seen:
            continue
        seen.add(link)
        title = normalize_whitespace(anchor.get_text(" ", strip=True))
        snippet = normalize_whitespace(container.get_text(" ", strip=True))
        results.append(SearchResult(url=link, title=title, snippet=snippet))
        if len(results) >= max_results:
            break
    BRAVE_SEARCH_CACHE[query] = results
    return results[:max_results]


def decode_duckduckgo_result_url(link: str) -> str:
    if link.startswith("//"):
        link = f"https:{link}"
    parsed = urlparse(link)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        params = parse_qs(parsed.query)
        encoded = params.get("uddg", [""])[0]
        if encoded:
            return unquote(encoded)
    return link


def search_duckduckgo_lite_results(query: str, max_results: int = 10) -> list[SearchResult]:
    if query in DUCKDUCKGO_SEARCH_CACHE:
        return DUCKDUCKGO_SEARCH_CACHE[query][:max_results]
    throttle_search_backend("duckduckgo")
    search_url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    response = requests.get(search_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    results: list[SearchResult] = []
    seen: set[str] = set()
    for anchor in soup.select('a[href*="uddg="]'):
        link = decode_duckduckgo_result_url(normalize_whitespace(anchor.get("href") or ""))
        if not link.startswith("http"):
            continue
        if link in seen:
            continue
        seen.add(link)
        title = normalize_whitespace(anchor.get_text(" ", strip=True))
        snippet = normalize_whitespace(anchor.find_parent("tr").get_text(" ", strip=True))
        results.append(SearchResult(url=link, title=title, snippet=snippet))
        if len(results) >= max_results:
            break
    DUCKDUCKGO_SEARCH_CACHE[query] = results
    return results[:max_results]


def decode_bing_result_url(link: str) -> str:
    if link.startswith("https://www.bing.com/ck/a?"):
        parsed = urlparse(link)
        params = dict(
            piece.split("=", 1)
            for piece in parsed.query.split("&")
            if "=" in piece
        )
        encoded = params.get("u", "")
        if encoded.startswith("a1"):
            payload = encoded[2:]
            padding = "=" * ((4 - len(payload) % 4) % 4)
            try:
                return base64.b64decode(f"{payload}{padding}").decode("utf-8")
            except Exception:
                return link
    return link


def search_bing_results(query: str, max_results: int = 10) -> list[SearchResult]:
    if query in BING_SEARCH_CACHE:
        return BING_SEARCH_CACHE[query][:max_results]
    throttle_search_backend("bing")
    search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
    response = requests.get(search_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    results: list[SearchResult] = []
    seen: set[str] = set()
    for container in soup.select("li.b_algo"):
        anchor = container.select_one("h2 a[href]")
        if anchor is None:
            continue
        link = decode_bing_result_url(normalize_whitespace(anchor.get("href") or ""))
        if not link.startswith("http"):
            continue
        if link in seen:
            continue
        seen.add(link)
        title = normalize_whitespace(anchor.get_text(" ", strip=True))
        snippet = normalize_whitespace(container.get_text(" ", strip=True))
        results.append(SearchResult(url=link, title=title, snippet=snippet))
        if len(results) >= max_results:
            break
    BING_SEARCH_CACHE[query] = results
    return results[:max_results]


def discover_additional_source_links(page_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    parsed_root = urlparse(page_url)
    base_host = reduce_host_domain(parsed_root.netloc)
    links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        absolute = urljoin(page_url, anchor["href"].strip())
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if reduce_host_domain(parsed.netloc) != base_host:
            continue
        combined = f"{absolute} {anchor.get_text(' ', strip=True)}".lower()
        if any(token in combined for token in SEED_SOURCE_LINK_HINTS) and not any(
            token in combined for token in SEED_SOURCE_BLOCK_HINTS
        ):
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)
        if len(links) >= 10:
            break
    return links


def discover_official_seed_pages(country_name: str) -> list[str]:
    if country_name in SEED_PAGE_CACHE:
        return SEED_PAGE_CACHE[country_name][:]
    pages: list[str] = []
    seen: set[str] = set()
    country_key = slugify_name(country_name)

    for candidate_url in generate_official_source_url_candidates(country_name):
        try:
            page = requests.get(
                candidate_url,
                headers=HEADERS,
                timeout=3,
                verify=False,
            )
            page.raise_for_status()
        except Exception:
            continue
        content_type = page.headers.get("content-type", "").lower()
        if "text/html" not in content_type:
            continue
        page_text = slugify_name(page.text[:7000])
        if country_key and country_key not in page_text and "university" not in page_text:
            continue
        if not any(token in page_text for token in ("university", "college", "polytechnic", "higher education", "tvet")):
            continue
        final_url = page.url
        if final_url not in seen:
            seen.add(final_url)
            pages.append(final_url)
        reduced_host = reduce_host_domain(urlparse(final_url).netloc)
        if reduced_host.startswith("cue."):
            portal_url = f"https://imis.{reduced_host}/RecognitionAndEquationforQualifications/AccreditedUniversities"
            if portal_url not in seen:
                seen.add(portal_url)
                pages.append(portal_url)
        for extra in discover_additional_source_links(final_url, page.text):
            if extra not in seen:
                seen.add(extra)
                pages.append(extra)
        if len(pages) >= 12:
            SEED_PAGE_CACHE[country_name] = pages[:12]
            return pages[:12]

    if pages:
        SEED_PAGE_CACHE[country_name] = pages[:12]
        return pages[:12]

    for template in SOURCE_DISCOVERY_QUERIES:
        query = template.format(country=country_name)
        try:
            results = search_duckduckgo_lite_results(query, max_results=10)
        except Exception:
            try:
                results = search_brave_results(query, max_results=10)
            except Exception:
                continue
        for result in results:
            if not looks_like_official_source_result(result, country_name):
                continue
            link = result.url
            if not link.startswith("http"):
                continue
            if link in seen:
                continue
            try:
                page = fetch_url(link)
            except Exception:
                continue
            content_type = page.headers.get("content-type", "").lower()
            if "pdf" in content_type or page.url.lower().endswith(".pdf"):
                seen.add(page.url)
                pages.append(page.url)
                if len(pages) >= 12:
                    SEED_PAGE_CACHE[country_name] = pages[:12]
                    return pages[:12]
                continue
            if "text/html" not in content_type:
                continue
            page_text = slugify_name(page.text[:5000])
            if country_key and country_key not in page_text and country_key not in slugify_name(result.snippet):
                continue
            if not any(token in page_text for token in ("university", "college", "polytechnic", "institution", "higher education")):
                continue
            seen.add(page.url)
            pages.append(page.url)
            for extra in discover_additional_source_links(page.url, page.text):
                if extra not in seen:
                    seen.add(extra)
                    pages.append(extra)
            if len(pages) >= 12:
                SEED_PAGE_CACHE[country_name] = pages[:12]
                return pages[:12]
    SEED_PAGE_CACHE[country_name] = pages[:12]
    return pages[:12]


def auto_load_seed_institutions(country_name: str) -> list[dict]:
    if country_name in SEED_INSTITUTION_CACHE:
        return [row.copy() for row in SEED_INSTITUTION_CACHE[country_name]]
    institutions: list[dict] = []
    seen: set[str] = set()
    for source_url in discover_official_seed_pages(country_name):
        try:
            response = fetch_url(source_url)
        except Exception:
            continue
        content_type = response.headers.get("content-type", "").lower()
        source_names: list[str] = []
        if "pdf" in content_type or response.url.lower().endswith(".pdf"):
            try:
                lowered_url = response.url.lower()
                if not any(token in lowered_url for token in ("accredited", "authori", "status")):
                    continue
                with pdfplumber.open(BytesIO(response.content)) as pdf:
                    pdf_text = "\n".join(page.extract_text() or "" for page in pdf.pages[:40])
                if not looks_like_institution_list_source(response.url, pdf_text):
                    continue
                source_names = extract_institution_names_from_text(pdf_text)
            except Exception:
                continue
        elif "text/html" in content_type:
            if not looks_like_institution_list_source(response.url, response.text):
                continue
            source_names = extract_institution_names_from_html(response.text)
            if len(source_names) < 5:
                soup = BeautifulSoup(response.text, "html.parser")
                source_names.extend(
                    extract_institution_names_from_text(soup.get_text("\n", strip=True))
                )
        else:
            continue
        foreign_indicator_count = sum(
            1
            for name in source_names
            if any(
                token in name.lower()
                for token in (
                    " usa",
                    "united kingdom",
                    "uganda",
                    "holy see",
                    "malaysia",
                    "tanzania",
                )
            )
        )
        if foreign_indicator_count >= 2:
            continue
        for institution_name in source_names:
            lowered_name = institution_name.lower()
            if "," in institution_name:
                if country_name.lower() not in lowered_name:
                    continue
                institution_name = normalize_whitespace(institution_name.split(",", 1)[0])
            key = canonical_institution_name(institution_name)
            if key in seen:
                continue
            seen.add(key)
            institutions.append(
                {
                    "university": institution_name,
                    "university_type": infer_institution_type(institution_name),
                }
            )
    SEED_INSTITUTION_CACHE[country_name] = [row.copy() for row in institutions]
    return institutions


def build_auto_country_config(country_name: str) -> CountryPipelineConfig:
    normalized_country = normalize_whitespace(country_name)
    return CountryPipelineConfig(
        country_name=normalized_country,
        seed_source_name="Auto-discovered official higher-education sources",
        load_seed_institutions=lambda: auto_load_seed_institutions(normalized_country),
        canonicalize_institution_name=canonical_institution_name,
        prioritized_institutions=(),
        institution_seed_urls={},
        manual_domain_hints={},
    )


@dataclass
class InstitutionRecord:
    university: str
    university_type: str
    official_domain: str
    discovery_method: str
    domain_status: str
    discovery_notes: str


class OfficialCountryProfessorCrawler:
    def __init__(
        self,
        config: CountryPipelineConfig,
        max_pages: int = 30,
        second_pass_pages: int = 25,
        workers: int = 6,
        *,
        output_dir: Path | str | None = None,
        progress_callback: ProgressCallbackFn | None = None,
    ):
        self.config = config
        self.max_pages = max_pages
        self.second_pass_pages = second_pass_pages
        self.workers = workers
        self.country_name = config.country_name
        self.output_paths = build_output_paths(config.country_name, output_dir)
        self.progress_callback = progress_callback
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.crawl_log: list[dict] = []
        self.domain_rows: list[dict] = []
        self.coverage_rows: list[dict] = []
        self.excluded_rows: list[dict] = []
        self.method_rows = [
            {
                "step": 1,
                "name": "Official institution seed list",
                "detail": (
                    "Auto-discovers official higher-education regulator or ministry pages for the "
                    "target country, then extracts institution names from those official sources."
                ),
            },
            {
                "step": 2,
                "name": "Official domain discovery",
                "detail": (
                    "Uses search-assisted resolution and only keeps domains whose homepage content "
                    "matches the institution name."
                ),
            },
            {
                "step": 3,
                "name": "Aggressive crawl",
                "detail": (
                    "Crawls official public staff, faculty, department, directory, profile, and "
                    "PDF pages with keyword prioritisation."
                ),
            },
            {
                "step": 4,
                "name": "Second-pass enrichment",
                "detail": (
                    "Low-yield domains get an extra pass over school, faculty, department, "
                    "professor, and PDF targets discovered during the first pass."
                ),
            },
            {
                "step": 5,
                "name": "Filtering and export",
                "detail": (
                    "Keeps only publicly posted professor-rank emails in the final export and "
                    "moves ambiguous or weak rows to Review_Excluded."
                ),
            },
        ]

    def emit_progress(self, phase: str, message: str, **payload: object) -> None:
        if self.progress_callback is None:
            return
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": phase,
            "message": message,
        }
        event.update(payload)
        self.progress_callback(event)

    def fetch_text(
        self,
        url: str,
        *,
        allow_insecure: bool = True,
        timeout: int = REQUEST_TIMEOUT,
        attempts: int = 3,
    ) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.SSLError:
                if not allow_insecure:
                    raise
                response = self.session.get(url, timeout=timeout, verify=False)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt == attempts - 1:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Unable to fetch {url}")

    def load_seed_institutions(self) -> list[dict]:
        universities: list[dict] = []
        seen_keys: set[str] = set()
        for institution in self.config.load_seed_institutions():
            clean_name = normalize_whitespace(str(institution["university"]))
            if not clean_name:
                continue
            key = self.config.canonicalize_institution_name(clean_name)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            universities.append(
                {
                    "university": clean_name,
                    "university_type": institution["university_type"],
                }
            )
        return universities

    @staticmethod
    def reduce_domain(host: str) -> str:
        return reduce_host_domain(host)

    @staticmethod
    def host_matches_institution(host: str, university: str) -> bool:
        reduced = OfficialCountryProfessorCrawler.reduce_domain(host)
        label = reduced.split(".")[0]
        tokens = institution_name_tokens(university)
        acronyms = institution_acronym_candidates(university)
        if any(token in label for token in tokens):
            return True
        if any(acronym in label for acronym in acronyms):
            return True
        return any(token in host for token in (".ac.", ".edu.")) and bool(tokens)

    def homepage_matches(self, university: str, html: str) -> bool:
        text = slugify_name(html)
        name_tokens = [token for token in slugify_name(university).split() if len(token) > 2]
        if not name_tokens:
            return False
        matches = sum(1 for token in name_tokens if token in text)
        return matches >= max(2, min(4, len(name_tokens)))

    def validate_official_domain(
        self,
        university: str,
        url: str,
        *,
        timeout: int = REQUEST_TIMEOUT,
        attempts: int = 3,
    ) -> tuple[bool, str]:
        if not self.host_matches_institution(urlparse(url).netloc, university):
            return False, "host_institution_mismatch"
        try:
            response = self.fetch_text(url, timeout=timeout, attempts=attempts)
        except Exception as exc:  # noqa: BLE001
            return False, f"fetch_failed: {exc}"
        if not self.host_matches_institution(urlparse(response.url).netloc, university):
            return False, "redirected_host_mismatch"
        if "text/html" not in response.headers.get("content-type", ""):
            return False, "not_html"
        if self.homepage_matches(university, response.text):
            return True, response.url
        return False, "homepage_text_mismatch"

    def generate_institution_domain_candidates(self, university: str) -> list[str]:
        country_code = resolve_country_alpha2(self.country_name)
        if not country_code:
            return []
        raw_tokens = [token for token in slugify_name(university).split() if token]
        significant = institution_name_tokens(university)
        ordered_labels: list[str] = []
        seen_labels: set[str] = set()

        def add_label(label: str) -> None:
            cleaned = re.sub(r"[^a-z0-9]", "", label.lower())
            if len(cleaned) < 2 or cleaned in seen_labels:
                return
            seen_labels.add(cleaned)
            ordered_labels.append(cleaned)

        for acronym in sorted(institution_acronym_candidates(university), key=len, reverse=True):
            add_label(acronym)
        if significant:
            add_label("".join(significant[:2]))
            add_label("".join(significant))
        if raw_tokens[:2] == ["university", "of"] and significant:
            main = significant[0]
            add_label(f"uo{main[:3]}")
            add_label(f"uo{main[:4]}")
            add_label(f"uo{main[0]}{main[-2:]}")
        for token in significant:
            add_label(token)
        candidate_tlds = (
            f"ac.{country_code}",
            f"edu.{country_code}",
            "edu",
        )
        urls: list[str] = []
        seen: set[str] = set()
        for label in ordered_labels:
            for tld in candidate_tlds:
                for prefix in ("", "www."):
                    url = f"https://{prefix}{label}.{tld}/"
                    if url not in seen:
                        seen.add(url)
                        urls.append(url)
        return urls

    def resolve_domain_from_patterns(self, university: str) -> tuple[str, str, str]:
        for candidate in self.generate_institution_domain_candidates(university):
            valid, detail = self.validate_official_domain(
                university,
                candidate,
                timeout=6,
                attempts=1,
            )
            if valid:
                resolved = detail if isinstance(detail, str) and detail.startswith("http") else candidate
                return resolved, "pattern_validated", "Validated from generated institution-domain pattern."
        return "", "pattern_unresolved", "No generated institution-domain pattern validated."

    def resolve_domain_from_search(self, university: str) -> tuple[str, str, str]:
        queries = (
            f'"{university}" "{self.country_name}" official university',
            f'"{university}" "{self.country_name}"',
            f'"{university}" official site',
        )
        seen_candidates: set[str] = set()
        last_error = "Search results did not validate to an official homepage."

        for query in queries:
            results: list[SearchResult] = []
            for search_fn in (search_duckduckgo_lite_results, search_bing_results):
                try:
                    results = search_fn(query, max_results=10)
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    continue
                if results:
                    break
            if not results:
                continue

            for result in results:
                link = result.url.strip()
                if not link.startswith("http"):
                    continue
                host = self.reduce_domain(urlparse(link).netloc)
                if host in seen_candidates:
                    continue
                seen_candidates.add(host)
                combined = slugify_name(f"{result.title} {result.snippet} {link}")
                university_key = slugify_name(university)
                if university_key and not any(token in combined for token in university_key.split()[:3]):
                    continue
                if any(
                    blocked in host
                    for blocked in (
                        "wikipedia.org",
                        "facebook.com",
                        "linkedin.com",
                        "x.com",
                        "twitter.com",
                        "youtube.com",
                        "tiktok.com",
                        "instagram.com",
                    )
                ):
                    continue
                if not any(token in host for token in (".ac.", ".edu", ".org", ".gov", ".go.", ".co.", ".or.")):
                    continue
                candidates = [link, f"https://{host}/"]
                for candidate in candidates:
                    valid, detail = self.validate_official_domain(
                        university,
                        candidate,
                        timeout=8,
                        attempts=1,
                    )
                    if valid:
                        resolved = detail if isinstance(detail, str) and detail.startswith("http") else candidate
                        return resolved, "search_validated", "Validated from Bing HTML search result."
                    last_error = str(detail)
        return "", "unresolved", last_error

    def resolve_institution_domain(self, institution: str, institution_type: str) -> InstitutionRecord:
        manual = None
        wanted_key = self.config.canonicalize_institution_name(institution)
        for candidate_name, candidate_url in self.config.manual_domain_hints.items():
            if self.config.canonicalize_institution_name(candidate_name) == wanted_key:
                manual = candidate_url
                break
        if manual:
            try:
                response = self.fetch_text(manual)
                if "text/html" in response.headers.get("content-type", ""):
                    return InstitutionRecord(
                        university=institution,
                        university_type=institution_type,
                        official_domain=response.url,
                        discovery_method="manual_hint_validated",
                        domain_status="resolved",
                        discovery_notes="Accepted from curated official-domain hint after successful fetch.",
                    )
            except Exception:  # noqa: BLE001
                pass
            valid, detail = self.validate_official_domain(institution, manual)
            if valid and detail.startswith("http"):
                return InstitutionRecord(
                    university=institution,
                    university_type=institution_type,
                    official_domain=detail,
                    discovery_method="manual_hint_validated",
                    domain_status="resolved",
                    discovery_notes="Validated against institution homepage text.",
                )

        resolved, method, notes = self.resolve_domain_from_patterns(institution)
        if resolved:
            return InstitutionRecord(
                university=institution,
                university_type=institution_type,
                official_domain=resolved,
                discovery_method=method,
                domain_status="resolved",
                discovery_notes=notes,
            )

        resolved, method, notes = self.resolve_domain_from_search(institution)
        if resolved:
            return InstitutionRecord(
                university=institution,
                university_type=institution_type,
                official_domain=resolved,
                discovery_method=method,
                domain_status="resolved",
                discovery_notes=notes,
            )

        return InstitutionRecord(
            university=institution,
            university_type=institution_type,
            official_domain="",
            discovery_method=method,
            domain_status="unresolved",
            discovery_notes=notes,
        )

    def seed_urls_for_record(self, record: InstitutionRecord) -> list[str]:
        seeds = [record.official_domain] if record.official_domain else []
        for url in self.config.institution_seed_urls.get(record.university, []):
            if url not in seeds:
                seeds.append(url)
        return seeds

    @staticmethod
    def page_is_listing_page(page_url: str, page_title: str, text: str = "") -> bool:
        source = normalize_whitespace(f"{page_title} {page_url} {text[:600]}").lower()
        return any(hint in source for hint in PROFILE_LISTING_HINTS)

    @staticmethod
    def is_name_like(text: str) -> bool:
        clean = normalize_whitespace(text)
        if not clean or len(clean) > 100:
            return False
        lowered = clean.lower()
        if any(
            token in lowered
            for token in (
                "faculty",
                "department",
                "school",
                "university",
                "contact",
                "admission",
                "programme",
                "office",
                "resources",
                "staff",
                "members",
                "team",
                "profile details",
                "studies",
                "sciences",
                "engineering",
                "medicine",
                "pathology",
                "physiology",
                "laboratory",
                "economics",
                "business",
                "complex",
                "embassy",
                "nairobi",
                "kenya",
                "street",
                "road",
                "avenue",
                "downloads",
                "former",
                "deans",
                "curriculum",
                "vitae",
                "google",
                "scholar",
                "learning",
                "teaching",
                "close",
                "dean",
                "chair",
                "leader",
            )
        ):
            return False
        words = re.findall(r"[A-Za-z][A-Za-z\.'-]*", clean)
        if not 2 <= len(words) <= 6:
            return False
        if lowered.startswith(NAME_TITLE_PREFIXES):
            return True
        capitals = sum(1 for word in words if word[0].isupper())
        return capitals >= max(2, len(words) - 1)

    @staticmethod
    def profile_link_priority(url: str, text: str, page_is_listing: bool) -> tuple[int, int]:
        combined = f"{text} {url}".lower()
        if any(token in combined for token in ("professor", "assoc", "prof.", "prof ")):
            return (0, len(url))
        if page_is_listing and OfficialCountryProfessorCrawler.is_name_like(text):
            return (1, len(url))
        if any(token in combined for token in ("faculty-staff", "staff", "profile", "research profile")):
            return (2, len(url))
        return (3, len(url))

    @staticmethod
    def classify_missing_rank(context: str) -> str:
        lowered = context.lower()
        if any(token in lowered for token in NON_PROFESSOR_HINTS):
            return "non_professor_rank"
        return "weak_linkage"

    def discover_links(
        self, page_url: str, soup: BeautifulSoup, official_domain: str, page_title: str
    ) -> tuple[list[str], list[str]]:
        html_links: list[tuple[str, str]] = []
        pdf_links: list[str] = []
        base_host = self.reduce_domain(urlparse(official_domain).netloc)
        seen_urls: set[str] = set()
        page_text = soup.get_text("\n", strip=True)
        listing_page = self.page_is_listing_page(page_url, page_title, page_text)
        elements = soup.find_all(["a", "button", "div", "span"], attrs={"data-href": True}) + soup.find_all("a", href=True)
        for element in elements:
            href = (element.get("href") or element.get("data-href") or "").strip()
            text = normalize_whitespace(element.get_text(" ", strip=True))
            parent_text = normalize_whitespace(
                element.parent.get_text(" ", strip=True) if getattr(element, "parent", None) else text
            )
            if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            absolute = self.resolve_crawl_url(page_url, official_domain, href)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            host = self.reduce_domain(parsed.netloc)
            if host != base_host:
                continue
            if absolute in seen_urls:
                continue
            seen_urls.add(absolute)
            combined = f"{absolute} {text} {parent_text}".lower()
            if any(pattern in combined for pattern in BLOCKED_LINK_PATTERNS):
                continue
            if absolute.lower().endswith(".pdf"):
                if any(keyword in combined for keyword in ("staff", "faculty", "professor", "department", "school")):
                    pdf_links.append(absolute)
                continue
            if any(keyword in combined for keyword in KEYWORD_PATTERNS):
                html_links.append((absolute, f"{text} {parent_text}"))
                continue
            if listing_page and (
                self.is_name_like(text)
                or re.search(r"/[A-Za-z0-9._-]+$", urlparse(absolute).path)
                or "page=" in absolute
            ):
                html_links.append((absolute, f"{text} {parent_text}"))

        ordered_html_links = [
            url
            for url, text in sorted(
                html_links,
                key=lambda item: self.profile_link_priority(item[0], item[1], listing_page),
            )
        ]
        return ordered_html_links, list(dict.fromkeys(pdf_links))

    @staticmethod
    def resolve_crawl_url(page_url: str, official_domain: str, href: str) -> str:
        if href.startswith(("http://", "https://")):
            return href
        if href.startswith(("includes/", "assets/")):
            return urljoin(official_domain, href)
        return urljoin(page_url, href)

    def extract_records_from_text(
        self,
        *,
        university: str,
        university_type: str,
        official_domain: str,
        source_url: str,
        source_page_title: str,
        text: str,
        extraction_method: str,
        profile_url: str = "",
    ) -> list[dict]:
        rows: list[dict] = []
        lines = [normalize_whitespace(line) for line in text.splitlines()]
        lines = [line for line in lines if line]
        if not lines:
            return rows

        for idx, line in enumerate(lines):
            emails = extract_emails(line)
            if not emails:
                continue
            start = max(0, idx - 20)
            end = min(len(lines), idx + 15)
            context_lines = lines[start:end]
            local_email_index = idx - start
            context = " ".join(context_lines)
            immediate_context = " ".join(lines[max(0, idx - 6) : min(len(lines), idx + 7)])
            email_label_context = " ".join(lines[max(0, idx - 1) : min(len(lines), idx + 2)])
            title_line, normalized_rank = extract_rank(immediate_context)
            profile_like_source = self.is_profile_like_source(source_url)
            if not normalized_rank and profile_like_source:
                title_line, normalized_rank = extract_rank(context)
            if not normalized_rank:
                self.excluded_rows.append(
                    {
                        "university": university,
                        "source_url": source_url,
                        "profile_url": profile_url or source_url,
                        "email": "; ".join(emails),
                        "reason": self.classify_missing_rank(context),
                        "evidence_snippet": make_snippet(context, emails[0]),
                    }
                )
                continue

            title_name = self.extract_name_from_title(source_page_title)
            if title_name and not self.is_name_like(title_name):
                title_name = ""
            context_name = self.extract_name_from_context(context_lines, email_index=local_email_index)
            fallback_name = self.clean_name_line(extract_name(context))
            context_has_titled_name = bool(
                re.search(
                    r"(?:^|\s)(?:\d+\.\s*)?(?:prof(?:essor)?|associate professor|assistant professor)\s+[A-Z]",
                    immediate_context,
                    re.IGNORECASE,
                )
            )
            if (
                title_name
                and context_name
                and self.is_name_like(context_name)
                and context_has_titled_name
                and not self.same_person_name(title_name, context_name)
            ):
                full_name = context_name
            else:
                full_name = title_name or context_name or fallback_name
            full_name = self.clean_name_line(full_name)
            if not self.is_name_like(full_name) and self.is_name_like(context_name):
                full_name = self.clean_name_line(context_name)
            if not self.is_name_like(full_name) and self.is_name_like(fallback_name):
                full_name = self.clean_name_line(fallback_name)
            if not self.is_name_like(full_name):
                self.excluded_rows.append(
                    {
                        "university": university,
                        "source_url": source_url,
                        "profile_url": profile_url or source_url,
                        "email": "; ".join(emails),
                        "reason": "parsing_failure",
                        "evidence_snippet": make_snippet(context, emails[0]),
                    }
                )
                continue

            faculty, department = self.infer_faculty_department(text, source_page_title)
            for email in emails:
                email_type = infer_email_type(email, official_domain)
                if email_type == "personal":
                    self.excluded_rows.append(
                        {
                            "university": university,
                            "source_url": source_url,
                            "profile_url": profile_url or source_url,
                            "email": email,
                            "reason": "personal_email",
                            "evidence_snippet": make_snippet(context, email),
                        }
                    )
                    continue
                if email_type == "generic_institutional":
                    self.excluded_rows.append(
                        {
                            "university": university,
                            "source_url": source_url,
                            "profile_url": profile_url or source_url,
                            "email": email,
                            "reason": "generic_inbox",
                            "evidence_snippet": make_snippet(context, email),
                        }
                    )
                    continue
                if email_type == "external" and not re.search(
                    r"\b(?:email|e-mail|contacts?|contact information)\b",
                    email_label_context,
                    re.IGNORECASE,
                ):
                    self.excluded_rows.append(
                        {
                            "university": university,
                            "source_url": source_url,
                            "profile_url": profile_url or source_url,
                            "email": email,
                            "reason": "weak_linkage",
                            "evidence_snippet": make_snippet(context, email),
                        }
                    )
                    continue
                rows.append(
                    {
                        "country": self.country_name,
                        "university": university,
                        "university_type": university_type,
                        "official_domain": official_domain,
                        "faculty_or_school": faculty,
                        "department": department,
                        "full_name": full_name,
                        "normalized_name": slugify_name(full_name),
                        "rank": title_line or normalized_rank,
                        "normalized_rank": normalized_rank,
                        "email": canonical_email(email),
                        "email_type": email_type,
                        "title_line": title_line or normalized_rank,
                        "profile_url": profile_url or source_url,
                        "source_url": source_url,
                        "source_page_title": source_page_title,
                        "evidence_snippet": make_snippet(context, email),
                        "extraction_method": extraction_method,
                        "confidence_score": 0.0,
                        "duplicate_group_id": "",
                        "source_priority": self.source_priority(source_url),
                        "notes": "",
                        "date_collected": datetime.now(timezone.utc).date().isoformat(),
                    }
                )
        return rows

    @staticmethod
    def clean_name_line(line: str) -> str:
        cleaned = normalize_whitespace(line)
        cleaned = re.sub(r"^\d+[\.\)]\s*", "", cleaned)
        bio_match = re.search(
            r"^(?:(?:dr|prof|professor|mr|mrs|ms|eng)\.?\s+)?"
            r"([A-Z][A-Za-z\.'-]+(?:\s+[A-Z][A-Za-z\.'-]+){1,5})\s+is\s+an?\b",
            cleaned,
            re.IGNORECASE,
        )
        if bio_match:
            cleaned = bio_match.group(1)
        comma_parts = [part.strip() for part in cleaned.split(",") if part.strip()]
        if len(comma_parts) >= 2 and not re.match(
            r"^(?:phd|ph\.d\.|msc|m\.sc\.|bsc|b\.sc\.|mba|mpsk|pe|miek|idr|ogw)\b",
            comma_parts[1],
            re.IGNORECASE,
        ):
            surname, given_names = comma_parts[0], comma_parts[1]
            reordered = normalize_whitespace(f"{given_names} {surname}")
            if OfficialCountryProfessorCrawler.is_name_like(reordered):
                cleaned = reordered
        cleaned = re.sub(r"^(?:name|staff name|profile details|contacts?)\s*:\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"^(?:prof|prof\.|dr|eng|mr|mrs|ms)\.?\s+", "", cleaned, flags=re.I)
        cleaned = re.sub(
            r"\s+(?:associate|assistant|full)?\s*professor\b.*$",
            "",
            cleaned,
            flags=re.I,
        )
        cleaned = re.sub(r",?\s*(?:phd|ph\.d\.|msc|m\.sc\.|bsc|b\.sc\.|mba|pe|miek|idr|ogw)\b.*$", "", cleaned, flags=re.I)
        cleaned = normalize_whitespace(cleaned.strip(" -,:;|"))
        if re.match(
            r"^(?:associate|assistant|full)?\s*professor\s+of\b",
            cleaned,
            re.IGNORECASE,
        ):
            return ""
        if re.match(
            r"^(?:assoc(?:iate)?\.?\s*prof(?:essor)?|assistant\s*prof(?:essor)?|prof(?:essor)?)\.?\s+of\b",
            cleaned,
            re.IGNORECASE,
        ):
            return ""
        lowered = cleaned.lower()
        if re.search(r"p\.?\s*o\.?\s*box", lowered):
            return ""
        if re.search(r"\.(?:pdf|docx?)\b", lowered):
            return ""
        if any(
            token in lowered
            for token in (
                "telephone",
                "transformative teaching",
                "criminal justice",
                "biography:",
                "contact address",
                "former deans",
                "downloads",
                "google scholar",
            )
        ):
            return ""
        return cleaned

    @classmethod
    def extract_name_from_title(cls, source_page_title: str) -> str:
        segments = [
            normalize_whitespace(segment)
            for segment in re.split(r"\s+[|–-]\s+", source_page_title or "")
            if normalize_whitespace(segment)
        ]
        preferred = segments + [source_page_title]
        for segment in preferred:
            cleaned = cls.clean_name_line(segment)
            if cls.is_name_like(cleaned):
                return cleaned
            extracted = cls.clean_name_line(extract_name(segment))
            if cls.is_name_like(extracted):
                return extracted
        return ""

    @staticmethod
    def is_profile_like_source(source_url: str) -> bool:
        lowered = source_url.lower()
        if any(token in lowered for token in ("search/node", "all-staff", "/websites")):
            return False
        if any(token in lowered for token in ("profile=", "item=", "/contacts", "/biography")):
            return True
        parsed = urlparse(source_url)
        return bool(re.search(r"/(?:node/\d+|[a-z0-9._-]+)$", parsed.path.lower()))

    @staticmethod
    def same_person_name(left: str, right: str) -> bool:
        left_tokens = {token for token in slugify_name(left).split() if len(token) > 1}
        right_tokens = {token for token in slugify_name(right).split() if len(token) > 1}
        if not left_tokens or not right_tokens:
            return False
        overlap = left_tokens & right_tokens
        return len(overlap) >= min(2, len(left_tokens), len(right_tokens))

    @classmethod
    def extract_name_from_context(cls, lines: list[str], email_index: int) -> str:
        priority_lines = list(reversed(lines[:email_index])) + lines[email_index + 1 :]
        titled_lines = []
        for line in priority_lines:
            if re.match(
                r"^(?:\d+[\.\)]\s*)?(?:prof(?:essor)?|dr|eng|mr|mrs|ms)\.?\s+[A-Z]",
                line,
                re.IGNORECASE,
            ):
                titled_lines.append(line)
                continue
            if re.fullmatch(r"[A-Z][A-Z\s\.'-]{5,}\s+PROFESSOR", line):
                titled_lines.append(line)
        ordered_lines = titled_lines + [line for line in priority_lines if line not in titled_lines]
        for line in ordered_lines:
            lowered = line.lower()
            if "@" in line or len(line.split()) < 2:
                continue
            if any(
                token in lowered
                for token in (
                    "email",
                    "telephone",
                    "contact information",
                    "biography",
                    "school of",
                    "department of",
                    "position:",
                    "contacts:",
                    "office:",
                    "qualifications:",
                    "area of specialization",
                    "research interests",
                    "examination officer",
                    "biomedical sciences",
                    "studies",
                    "sciences",
                    "engineering",
                    "medicine",
                    "physiology",
                    "pathology",
                    "link",
                )
            ):
                continue
            if re.fullmatch(
                r"(?:associate\s+professor|assistant\s+professor|full\s+professor|professor|prof\.?)",
                lowered,
            ):
                continue
            candidate = cls.clean_name_line(line)
            if re.search(r"\bis\s+an?\s+(?:associate|assistant|full)?\s*professor\b", line, re.I):
                sentence_name = cls.clean_name_line(line)
                if cls.is_name_like(sentence_name):
                    return sentence_name
            if lowered.startswith(NAME_TITLE_PREFIXES) and len(candidate.split()) >= 2:
                return candidate
            if not cls.is_name_like(candidate):
                continue
            name = extract_name(candidate) or candidate
            if cls.is_name_like(name):
                return name
        return ""

    @staticmethod
    def infer_faculty_department(text: str, page_title: str) -> tuple[str, str]:
        source = normalize_whitespace(f"{page_title} {text[:1000]}")
        faculty = ""
        department = ""
        school_match = re.search(
            r"\b(?:school|faculty|college)\s+of\s+([A-Z][A-Za-z&,\- ]+)",
            source,
            re.IGNORECASE,
        )
        department_match = re.search(
            r"\bdepartment\s+of\s+([A-Z][A-Za-z&,\- ]+)",
            source,
            re.IGNORECASE,
        )
        if school_match:
            faculty = normalize_whitespace(school_match.group(0))
        if department_match:
            department = normalize_whitespace(department_match.group(0))
        return faculty, department

    @staticmethod
    def source_priority(url: str) -> int:
        lowered = url.lower()
        if lowered.endswith(".pdf"):
            return 2
        if any(token in lowered for token in ("profile", "staff", "faculty", "directory")):
            return 1
        return 3

    def crawl_university(self, record: InstitutionRecord) -> tuple[list[dict], dict]:
        self.emit_progress(
            "crawl_started",
            f"Started crawling {record.university}",
            university=record.university,
            official_domain=record.official_domain,
        )
        if not record.official_domain:
            summary = {
                "university": record.university,
                "official_domain": "",
                "status": "unresolved_domain",
                "pages_crawled": 0,
                "records_found": 0,
                "notes": record.discovery_notes,
            }
            self.emit_progress(
                "crawl_skipped",
                f"Skipped {record.university} because no official domain was resolved.",
                university=record.university,
                summary=summary,
            )
            return [], summary

        seed_urls = self.seed_urls_for_record(record)
        seed_queue: deque[str] = deque(seed_urls)
        queue: deque[str] = deque()
        seen: set[str] = set()
        discovered_profile_links: list[str] = []
        pending_pdfs: deque[str] = deque()
        rows: list[dict] = []
        stats = {
            "pages_discovered": len(seed_urls),
            "relevant_pages_checked": 0,
            "profile_pages_followed": 0,
            "pdfs_checked": 0,
            "raw_candidate_rows_found": 0,
            "excluded_rows": 0,
        }

        def crawl_page(url: str, method_suffix: str) -> None:
            nonlocal rows
            if url in seen:
                return
            seen.add(url)
            try:
                response = self.fetch_text(url)
                content_type = response.headers.get("content-type", "")
                if "text/html" not in content_type:
                    self.crawl_log.append(
                        {
                            "university": record.university,
                            "url": url,
                            "status": "skipped_non_html",
                            "content_type": content_type,
                        }
                    )
                    return
                soup = BeautifulSoup(response.text, "html.parser")
                page_title = normalize_whitespace(soup.title.get_text(" ", strip=True)) if soup.title else ""
                if not page_title:
                    heading = soup.select_one("h1, h2, h3, h4.modal-title, .modal-title, .entry-title")
                    if heading:
                        page_title = normalize_whitespace(heading.get_text(" ", strip=True))
                page_text = soup.get_text("\n", strip=True)
                before_excluded = len(self.excluded_rows)
                extracted_rows = self.extract_records_from_text(
                    university=record.university,
                    university_type=record.university_type,
                    official_domain=record.official_domain,
                    source_url=response.url,
                    source_page_title=page_title,
                    text=page_text,
                    extraction_method=f"requests_bs4_{method_suffix}",
                )
                rows.extend(extracted_rows)
                stats["raw_candidate_rows_found"] += len(extracted_rows)
                stats["excluded_rows"] += len(self.excluded_rows) - before_excluded
                stats["relevant_pages_checked"] += 1
                if self.page_is_listing_page(response.url, page_title, page_text):
                    stats["profile_pages_followed"] += 0
                if any(token in response.url.lower() for token in ("profile", "faculty-staff", "/websites", "/all-staff")):
                    stats["profile_pages_followed"] += 1

                html_links, pdf_links = self.discover_links(
                    response.url, soup, record.official_domain, page_title
                )
                for link in html_links:
                    if "profile" in link.lower() or "staff" in link.lower():
                        if link not in discovered_profile_links:
                            discovered_profile_links.append(link)
                    if link not in seen:
                        if any(
                            token in link.lower()
                            for token in ("faculty-staff", "profile", "/all-staff", "/websites", "search/node")
                        ):
                            queue.appendleft(link)
                        else:
                            queue.append(link)
                stats["pages_discovered"] += sum(1 for link in html_links if link not in seen)
                for link in pdf_links:
                    if link not in seen:
                        pending_pdfs.append(link)
                stats["pages_discovered"] += sum(1 for link in pdf_links if link not in seen)
                self.crawl_log.append(
                    {
                        "university": record.university,
                        "url": response.url,
                        "status": "ok",
                        "content_type": content_type,
                        "records_detected": len(extracted_rows),
                        "page_title": page_title,
                        "is_listing_page": self.page_is_listing_page(response.url, page_title, page_text),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                self.crawl_log.append(
                    {
                        "university": record.university,
                        "url": url,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        while (seed_queue or queue) and len(seen) < self.max_pages:
            next_url = seed_queue.popleft() if seed_queue else queue.popleft()
            crawl_page(next_url, "html")

        if len(rows) < 3:
            for link in discovered_profile_links[: self.second_pass_pages]:
                crawl_page(link, "second_pass")
                if len(seen) >= self.max_pages + self.second_pass_pages:
                    break

        pdf_budget = 6 if len(rows) < 3 else 3
        for _ in range(pdf_budget):
            if not pending_pdfs:
                break
            pdf_url = pending_pdfs.popleft()
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            try:
                response = self.fetch_text(pdf_url)
                with pdfplumber.open(BytesIO(response.content)) as pdf:
                    pdf_text = "\n".join(page.extract_text() or "" for page in pdf.pages[:25])
                before_excluded = len(self.excluded_rows)
                extracted_rows = self.extract_records_from_text(
                    university=record.university,
                    university_type=record.university_type,
                    official_domain=record.official_domain,
                    source_url=response.url,
                    source_page_title=Path(urlparse(pdf_url).path).name,
                    text=pdf_text,
                    extraction_method="pdfplumber_pdf",
                )
                rows.extend(extracted_rows)
                stats["raw_candidate_rows_found"] += len(extracted_rows)
                stats["excluded_rows"] += len(self.excluded_rows) - before_excluded
                stats["pdfs_checked"] += 1
                self.crawl_log.append(
                    {
                        "university": record.university,
                        "url": response.url,
                        "status": "ok_pdf",
                        "content_type": response.headers.get("content-type", ""),
                        "records_detected": len(extracted_rows),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                self.crawl_log.append(
                    {
                        "university": record.university,
                        "url": pdf_url,
                        "status": "pdf_error",
                        "error": str(exc),
                    }
                )

        summary = {
            "university": record.university,
            "university_type": record.university_type,
            "official_domain": record.official_domain,
            "domain_status": record.domain_status,
            "discovery_method": record.discovery_method,
            "status": "completed",
            "pages_crawled": len(seen),
            "records_found": len(rows),
            "pages_discovered": stats["pages_discovered"],
            "relevant_pages_checked": stats["relevant_pages_checked"],
            "profile_pages_followed": stats["profile_pages_followed"],
            "pdfs_checked": stats["pdfs_checked"],
            "raw_candidate_rows_found": stats["raw_candidate_rows_found"],
            "excluded_rows": stats["excluded_rows"],
            "excluded_rows_by_reason": dict(
                Counter(
                    row["reason"]
                    for row in self.excluded_rows
                    if row.get("university") == record.university
                )
            ),
            "notes": record.discovery_notes,
        }
        if not rows:
            summary["zero_yield_reason"] = (
                "parser_limitations" if stats["relevant_pages_checked"] > 0 else "no_public_data_found"
            )
        self.emit_progress(
            "crawl_completed",
            f"Completed {record.university}",
            university=record.university,
            summary=summary,
        )
        return rows, summary

    @staticmethod
    def score_record(row: dict) -> float:
        score = 0.2
        if row["normalized_rank"] == "Professor":
            score += 0.25
        elif row["normalized_rank"] == "Associate Professor":
            score += 0.2
        elif row["normalized_rank"] == "Assistant Professor":
            score += 0.15
        if row["email_type"] == "institutional":
            score += 0.2
        if row["official_domain"] and row["source_url"]:
            official_host = OfficialCountryProfessorCrawler.reduce_domain(
                urlparse(row["official_domain"]).netloc
            )
            source_host = OfficialCountryProfessorCrawler.reduce_domain(urlparse(row["source_url"]).netloc)
            if official_host == source_host:
                score += 0.1
        if row["faculty_or_school"] or row["department"]:
            score += 0.1
        if row["evidence_snippet"] and row["full_name"].lower() in row["evidence_snippet"].lower():
            score += 0.1
        if row["source_priority"] == 1:
            score += 0.05
        return round(min(score, 0.99), 2)

    def deduplicate_records(self, rows: Iterable[dict]) -> list[dict]:
        dedupe_map: dict[str, dict] = {}
        alternatives: dict[str, list[str]] = {}

        for row in rows:
            cleaned_name = self.clean_name_line(row["full_name"])
            if self.is_name_like(cleaned_name):
                row["full_name"] = cleaned_name
                row["normalized_name"] = slugify_name(cleaned_name)
            row["confidence_score"] = self.score_record(row)
            keys = [
                f"name_uni::{row['normalized_name']}::{slugify_name(row['university'])}",
                f"name_email::{row['normalized_name']}::{row['email']}",
                f"email::{row['email']}",
            ]
            key = next((candidate for candidate in keys if candidate in dedupe_map), keys[0])
            if key not in dedupe_map or row["confidence_score"] > dedupe_map[key]["confidence_score"]:
                if key in dedupe_map:
                    alternatives.setdefault(key, []).append(dedupe_map[key]["source_url"])
                dedupe_map[key] = row
            else:
                alternatives.setdefault(key, []).append(row["source_url"])

        cleaned: list[dict] = []
        for idx, (key, row) in enumerate(dedupe_map.items(), start=1):
            row["duplicate_group_id"] = f"KEDUP-{idx:04d}"
            alt_sources = sorted(set(alternatives.get(key, [])))
            if alt_sources:
                joined = "; ".join(alt_sources[:5])
                row["notes"] = normalize_whitespace(f"{row['notes']} Alt sources: {joined}")
            cleaned.append(row)
        return sorted(
            cleaned,
            key=lambda row: (-row["confidence_score"], row["university"], row["full_name"]),
        )

    def run(self, limit: int | None = None, selected_institutions: list[str] | None = None) -> dict:
        self.emit_progress(
            "run_started",
            f"Preparing {self.country_name} extraction run.",
            country=self.country_name,
            output_dir=str(self.output_paths["output_dir"]),
        )
        universities = self.load_seed_institutions()
        self.emit_progress(
            "seed_loaded",
            f"Loaded {len(universities)} seed institutions from official sources.",
            country=self.country_name,
            seed_institutions=len(universities),
        )
        if selected_institutions:
            wanted = {
                self.config.canonicalize_institution_name(name) for name in selected_institutions
            }
            universities = [
                university
                for university in universities
                if self.config.canonicalize_institution_name(university["university"]) in wanted
            ]
            self.emit_progress(
                "seed_filtered",
                f"Filtered queue to {len(universities)} requested institutions.",
                selected_institutions=selected_institutions,
                seed_institutions=len(universities),
            )
        priority_index = {
            self.config.canonicalize_institution_name(name): idx
            for idx, name in enumerate(self.config.prioritized_institutions)
        }
        universities = sorted(
            universities,
            key=lambda university: (
                priority_index.get(
                    self.config.canonicalize_institution_name(university["university"]), 999
                ),
                university["university"],
            ),
        )
        if limit is not None:
            universities = universities[:limit]
            self.emit_progress(
                "seed_limited",
                f"Applied limit. Processing first {len(universities)} institutions.",
                seed_institutions=len(universities),
                limit=limit,
            )

        resolved_records: list[InstitutionRecord] = []
        for university in universities:
            record = self.resolve_institution_domain(
                institution=university["university"],
                institution_type=university["university_type"],
            )
            resolved_records.append(record)
            self.domain_rows.append(
                {
                    "university": record.university,
                    "university_type": record.university_type,
                    "official_domain": record.official_domain,
                    "domain_status": record.domain_status,
                    "discovery_method": record.discovery_method,
                    "notes": record.discovery_notes,
                }
            )
            self.emit_progress(
                "domain_resolved",
                f"Domain check finished for {record.university}",
                university=record.university,
                official_domain=record.official_domain,
                domain_status=record.domain_status,
                discovery_method=record.discovery_method,
            )

        all_rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self.crawl_university, record): record.university
                for record in resolved_records
            }
            for future in as_completed(futures):
                rows, summary = future.result()
                self.coverage_rows.append(summary)
                all_rows.extend(rows)
                self.emit_progress(
                    "coverage_updated",
                    f"Coverage updated for {summary['university']}",
                    university=summary["university"],
                    records_found=summary.get("records_found", 0),
                    pages_crawled=summary.get("pages_crawled", 0),
                    relevant_pages_checked=summary.get("relevant_pages_checked", 0),
                )

        final_rows = self.deduplicate_records(all_rows)
        final_by_university = Counter(row["university"] for row in final_rows)
        unique_institutional_emails = len(
            {row["email"] for row in final_rows if row["email_type"] == "institutional"}
        )
        for row in self.coverage_rows:
            row["final_included_rows"] = final_by_university.get(row["university"], 0)

        summary_rows = [
            {"metric": "country", "value": self.country_name},
            {"metric": "seed_institutions", "value": len(universities)},
            {
                "metric": "resolved_domains",
                "value": sum(1 for row in self.domain_rows if row["domain_status"] == "resolved"),
            },
            {"metric": "final_professor_rows", "value": len(final_rows)},
            {"metric": "unique_institutional_emails", "value": unique_institutional_emails},
            {"metric": "institutions_with_records", "value": len(final_by_university)},
            {"metric": "excluded_rows", "value": len(self.excluded_rows)},
            {"metric": "crawl_events", "value": len(self.crawl_log)},
        ]

        self.output_paths["output_dir"].mkdir(parents=True, exist_ok=True)
        self.emit_progress(
            "export_started",
            "Writing CSV and Excel outputs.",
            output_dir=str(self.output_paths["output_dir"]),
        )
        professor_df = pd.DataFrame(final_rows, columns=PROFESSOR_OUTPUT_COLUMNS)
        professor_df.to_csv(self.output_paths["csv"], index=False)
        write_excel_workbook(
            self.output_paths["xlsx"],
            professor_rows=professor_df,
            coverage_rows=self.coverage_rows,
            crawl_rows=self.crawl_log,
            domain_rows=self.domain_rows,
            excluded_rows=self.excluded_rows,
            method_rows=self.method_rows,
            summary_rows=summary_rows,
        )

        result = {
            "summary": {row["metric"]: row["value"] for row in summary_rows},
            "domains": self.domain_rows,
            "coverage_queue": self.coverage_rows,
            "excluded_count_by_reason": dict(Counter(row["reason"] for row in self.excluded_rows)),
            "institutions_with_records": sorted(final_by_university.items()),
            "output_paths": {
                "output_dir": str(self.output_paths["output_dir"]),
                "csv": str(self.output_paths["csv"]),
                "xlsx": str(self.output_paths["xlsx"]),
            },
        }
        self.emit_progress(
            "run_completed",
            f"{self.country_name} extraction completed.",
            summary=result["summary"],
            output_paths=result["output_paths"],
        )
        return result

def run_country_pipeline(
    config: CountryPipelineConfig,
    *,
    limit: int | None = None,
    max_pages: int = 30,
    second_pass_pages: int = 25,
    workers: int = 6,
    selected_institutions: list[str] | None = None,
    output_dir: Path | str | None = None,
    progress_callback: ProgressCallbackFn | None = None,
) -> dict:
    crawler = OfficialCountryProfessorCrawler(
        config=config,
        max_pages=max_pages,
        second_pass_pages=second_pass_pages,
        workers=workers,
        output_dir=output_dir,
        progress_callback=progress_callback,
    )
    return crawler.run(limit=limit, selected_institutions=selected_institutions)
