from __future__ import annotations

import argparse
import os
import threading
import webbrowser

from office_dashboard import create_app


def env_int(name: str, fallback: int) -> int:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return fallback
    try:
        return int(raw_value)
    except ValueError:
        return fallback


def build_parser() -> argparse.ArgumentParser:
    default_host = os.environ.get("DIALOGIC_DASHBOARD_HOST", "").strip()
    if not default_host:
        default_host = "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1"

    parser = argparse.ArgumentParser(
        description=(
            "Launch the Dialogic Solution office dashboard for the professor-email mining pipeline."
        )
    )
    parser.add_argument("--host", default=default_host, help="Host interface to bind.")
    parser.add_argument(
        "--port",
        type=int,
        default=env_int("PORT", env_int("DIALOGIC_DASHBOARD_PORT", 5080)),
        help="Port to listen on.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=env_int("DIALOGIC_DASHBOARD_THREADS", 8),
        help="Waitress worker thread count.",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open the dashboard automatically in a browser.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    app = create_app()
    url = f"http://{args.host}:{args.port}/"
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        from waitress import serve

        print(f"Dialogic Solution dashboard running at {url}")
        serve(app, host=args.host, port=args.port, threads=max(1, args.threads))
    except ImportError:
        print(f"Dialogic Solution dashboard running at {url}")
        app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
