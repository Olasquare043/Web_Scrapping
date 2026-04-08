import re
from io import BytesIO

import pdfplumber
import requests

PDF_URL = (
    "https://education.gov.ng/wp-content/uploads/2022/12/"
    "2021-Directory-of-Full-Professors-in-the-Nigerian-University-System-FINAL.pdf"
)
OUTPUT_FILE = "professor_emails.txt"

# Handles normal emails and cases where OCR introduces spaces around @ and .
EMAIL_PATTERN = re.compile(
    r"[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\s*\.\s*[A-Za-z]{2,}"
)


def normalize_email(email: str) -> str:
    cleaned = re.sub(r"\s+", "", email).lower()
    return cleaned.strip(".,;:()[]{}<>")


def extract_emails(pdf_bytes: bytes) -> list[str]:
    emails: set[str] = set()
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        print(f"Processing {total_pages} pages...")
        for i, page in enumerate(pdf.pages):
            if (i + 1) % 20 == 0:
                print(f"Scanning page {i + 1}/{total_pages}...", end="\r")
            text = page.extract_text() or ""
            for match in EMAIL_PATTERN.findall(text):
                emails.add(normalize_email(match))
    return sorted(emails)


def main() -> None:
    print("Downloading PDF...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(PDF_URL, headers=headers, timeout=120)
    response.raise_for_status()

    print("Extracting emails...")
    emails = extract_emails(response.content)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(emails))

    print(f"Done. Found {len(emails)} unique emails.")
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
