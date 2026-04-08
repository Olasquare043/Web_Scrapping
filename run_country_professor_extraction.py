from __future__ import annotations

import argparse
import json
import time

from country_pipelines import get_country_pipeline
from country_pipelines.official_country_pipeline import run_country_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the country-driven higher-institution professor-email pipeline. "
            "Start with a country name such as Kenya. "
            "The default output folder is created on demand as <country>_profs/."
        )
    )
    parser.add_argument(
        "country",
        type=str,
        help="Country to run, for example Kenya.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for institution count during testing.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=30,
        help="Maximum first-pass HTML pages per institution.",
    )
    parser.add_argument(
        "--second-pass-pages",
        type=int,
        default=25,
        help="Additional second-pass profile or department pages per institution.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Concurrent institution workers.",
    )
    parser.add_argument(
        "--institutions",
        "--universities",
        dest="institutions",
        type=str,
        default="",
        help="Comma-separated official institution names to target instead of the full queue.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Optional output folder override. Use Test/... for temporary smoke runs.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    pipeline = get_country_pipeline(args.country)
    selected = [name.strip() for name in args.institutions.split(",") if name.strip()]

    started = time.time()
    result = run_country_pipeline(
        pipeline.config,
        limit=args.limit,
        max_pages=args.max_pages,
        second_pass_pages=args.second_pass_pages,
        workers=args.workers,
        selected_institutions=selected or None,
        output_dir=args.output_dir or None,
    )
    elapsed = time.time() - started
    print(json.dumps(result["summary"], indent=2))
    print(f"{pipeline.country} extraction completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
