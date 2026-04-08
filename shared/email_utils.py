from __future__ import annotations

import re
import unicodedata
from urllib.parse import urlparse

EMAIL_PATTERN = re.compile(
    r"[A-Za-z0-9._%+-]+\s*(?:@|\[at\]|\(at\)|\sat\s)\s*"
    r"[A-Za-z0-9.-]+\s*(?:\.|\[dot\]|\(dot\)|\sdot\s)\s*[A-Za-z]{2,}",
    re.IGNORECASE,
)
STRICT_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
NAME_RE = re.compile(
    r"\b(?:Prof(?:essor)?\.?\s+)?([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,4})\b"
)
MULTISPACE_RE = re.compile(r"\s+")
GENERIC_LOCAL_PARTS = {
    "info",
    "admissions",
    "admission",
    "registrar",
    "pr",
    "law",
    "mls",
    "biological",
    "deanpharm",
    "dvcasa",
    "sobee",
    "vc",
    "dean",
    "hod",
    "office",
    "admin",
    "contact",
    "enquiries",
    "inquiries",
    "marketing",
    "support",
    "helpdesk",
    "secretary",
}

RANK_PATTERNS = [
    (
        re.compile(
            r"\b(?:associate professor|assoc(?:iate)?\.?\s*prof(?:essor)?\.?)\b",
            re.IGNORECASE,
        ),
        "Associate Professor",
    ),
    (
        re.compile(
            r"\b(?:assistant professor|asst\.?\s*prof(?:essor)?\.?)\b",
            re.IGNORECASE,
        ),
        "Assistant Professor",
    ),
    (
        re.compile(
            r"\b(?:full professor|professor|prof\.?)\b",
            re.IGNORECASE,
        ),
        "Professor",
    ),
]


def normalize_whitespace(text: str) -> str:
    return MULTISPACE_RE.sub(" ", text).strip()


def strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )


def slugify_name(text: str) -> str:
    cleaned = strip_accents(text).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return normalize_whitespace(cleaned)


def canonical_email(raw_email: str) -> str:
    email = raw_email
    email = re.sub(r"\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+", "@", email, flags=re.I)
    email = re.sub(r"\s*\[dot\]\s*|\s*\(dot\)\s*|\s+dot\s+", ".", email, flags=re.I)
    email = re.sub(r"\s+", "", email).lower()
    return email.strip(".,;:()[]{}<>'\"")


def extract_emails(text: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for match in EMAIL_PATTERN.findall(text or ""):
        email = canonical_email(match)
        if STRICT_EMAIL_RE.match(email) and email not in seen:
            seen.add(email)
            found.append(email)
    return found


def extract_rank(text: str) -> tuple[str, str]:
    source = normalize_whitespace(text)
    for pattern, label in RANK_PATTERNS:
        match = pattern.search(source)
        if match:
            return match.group(0), label
    return "", ""


def extract_name(text: str) -> str:
    candidate_text = normalize_whitespace(text.replace("|", " ").replace("/", " "))
    for match in NAME_RE.finditer(candidate_text):
        name = normalize_whitespace(match.group(1))
        lowered = name.lower()
        if any(
            token in lowered
            for token in (
                "department",
                "school",
                "faculty",
                "university",
                "email",
                "contact",
                "professor",
                "associate",
                "assistant",
            )
        ):
            continue
        return name
    return ""


def infer_email_type(email: str, official_domain: str) -> str:
    host = email.split("@", 1)[1]
    official_host = urlparse(official_domain).netloc.lower()
    official_host = official_host[4:] if official_host.startswith("www.") else official_host
    if host == official_host or host.endswith(f".{official_host}"):
        local = email.split("@", 1)[0]
        if local in GENERIC_LOCAL_PARTS:
            return "generic_institutional"
        return "institutional"
    if re.search(r"(gmail|yahoo|hotmail|outlook|icloud)\.", host):
        return "personal"
    return "external"


def make_snippet(text: str, email: str, width: int = 240) -> str:
    source = normalize_whitespace(text)
    if not source:
        return ""
    idx = source.lower().find(email.lower())
    if idx == -1:
        return source[:width]
    start = max(0, idx - width // 2)
    end = min(len(source), idx + len(email) + width // 2)
    return source[start:end].strip()
