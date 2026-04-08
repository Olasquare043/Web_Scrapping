from __future__ import annotations

import argparse
import threading
import webbrowser

from office_dashboard import create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Launch the Dialogic Solution office dashboard for the professor-email mining pipeline."
        )
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=5080, help="Port to listen on.")
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
        serve(app, host=args.host, port=args.port, threads=8)
    except ImportError:
        print(f"Dialogic Solution dashboard running at {url}")
        app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
