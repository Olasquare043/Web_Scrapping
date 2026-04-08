from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    render_template,
    request,
    send_file,
    stream_with_context,
)

from country_pipelines import get_country_pipeline
from country_pipelines.official_country_pipeline import AFRICA_COUNTRY_ALPHA2, run_country_pipeline

ROOT = Path(__file__).resolve().parents[1]
FINAL_STATUSES = {"completed", "failed"}
ARTIFACT_KEYS = {"output_dir", "csv", "xlsx"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_country_label(country_name: str) -> str:
    return " ".join(country_name.strip().replace("_", " ").replace("-", " ").split())


def parse_optional_int(value: Any) -> int | None:
    if value in (None, "", "null"):
        return None
    return int(value)


def parse_positive_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    if value in (None, "", "null"):
        return default
    parsed = int(value)
    return max(minimum, min(maximum, parsed))


def split_institutions(raw_value: str) -> list[str]:
    return [name.strip() for name in raw_value.split(",") if name.strip()]


def ensure_repo_relative_path(path_value: str) -> str:
    if not path_value:
        return ""
    candidate = Path(path_value)
    if candidate.is_absolute():
        return str(candidate)
    return str((ROOT / candidate).resolve())


@dataclass
class DashboardJob:
    id: str
    country: str
    config: dict[str, Any]
    status: str = "queued"
    created_at: str = field(default_factory=utc_now_iso)
    started_at: str = ""
    finished_at: str = ""
    error: str = ""
    summary: dict[str, Any] = field(default_factory=dict)
    output_paths: dict[str, str] = field(default_factory=dict)
    live_stats: dict[str, Any] = field(
        default_factory=lambda: {
            "seed_institutions": 0,
            "resolved_domains": 0,
            "completed_institutions": 0,
            "records_found": 0,
            "institutions_with_records": 0,
            "unique_institutional_emails": 0,
            "excluded_rows": 0,
        }
    )
    events: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    condition: threading.Condition = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.condition = threading.Condition(self.lock)

    def _append_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event["index"] = len(self.events)
        self.events.append(event)
        self._update_live_stats(event)
        return event

    def _update_live_stats(self, event: dict[str, Any]) -> None:
        phase = event.get("phase")
        if phase in {"seed_loaded", "seed_filtered", "seed_limited"}:
            seed_count = event.get("seed_institutions")
            if isinstance(seed_count, int):
                self.live_stats["seed_institutions"] = seed_count
        elif phase == "domain_resolved":
            if event.get("domain_status") == "resolved":
                self.live_stats["resolved_domains"] += 1
        elif phase in {"coverage_updated", "crawl_completed"}:
            records_found = event.get("records_found")
            if isinstance(records_found, int):
                self.live_stats["records_found"] = max(self.live_stats["records_found"], records_found)
            if phase == "crawl_completed":
                self.live_stats["completed_institutions"] += 1
        elif phase == "run_completed":
            summary = event.get("summary")
            if isinstance(summary, dict):
                self.summary = summary
                for key in (
                    "seed_institutions",
                    "resolved_domains",
                    "institutions_with_records",
                    "unique_institutional_emails",
                    "excluded_rows",
                    "final_professor_rows",
                ):
                    if key in summary:
                        mapped_key = "records_found" if key == "final_professor_rows" else key
                        self.live_stats[mapped_key] = summary[key]
            output_paths = event.get("output_paths")
            if isinstance(output_paths, dict):
                self.output_paths = output_paths

    def add_event(self, phase: str, message: str, **payload: Any) -> dict[str, Any]:
        with self.condition:
            event = {
                "timestamp": utc_now_iso(),
                "phase": phase,
                "message": message,
            }
            event.update(payload)
            appended = self._append_event(event)
            self.condition.notify_all()
            return appended

    def mark_running(self) -> None:
        with self.condition:
            self.status = "running"
            self.started_at = utc_now_iso()
            self._append_event(
                {
                    "timestamp": utc_now_iso(),
                    "phase": "dashboard_started",
                    "message": f"Dashboard run started for {self.country}.",
                }
            )
            self.condition.notify_all()

    def mark_completed(self, result: dict[str, Any]) -> None:
        with self.condition:
            self.status = "completed"
            self.finished_at = utc_now_iso()
            self.summary = result.get("summary", {})
            self.output_paths = result.get("output_paths", {})
            self._append_event(
                {
                    "timestamp": utc_now_iso(),
                    "phase": "dashboard_completed",
                    "message": f"{self.country} run finished successfully.",
                    "summary": self.summary,
                    "output_paths": self.output_paths,
                }
            )
            self.condition.notify_all()

    def mark_failed(self, error_message: str) -> None:
        with self.condition:
            self.status = "failed"
            self.finished_at = utc_now_iso()
            self.error = error_message
            self._append_event(
                {
                    "timestamp": utc_now_iso(),
                    "phase": "dashboard_failed",
                    "message": "Run failed.",
                    "error": error_message,
                }
            )
            self.condition.notify_all()

    def wait_for_event(self, next_index: int, timeout_seconds: float = 15.0) -> dict[str, Any] | None:
        with self.condition:
            if len(self.events) > next_index:
                return self.events[next_index]
            self.condition.wait(timeout_seconds)
            if len(self.events) > next_index:
                return self.events[next_index]
            return None

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "id": self.id,
                "country": self.country,
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "error": self.error,
                "summary": self.summary,
                "output_paths": self.output_paths,
                "live_stats": self.live_stats,
                "config": self.config,
                "event_count": len(self.events),
                "events_tail": self.events[-80:],
            }


class JobRegistry:
    def __init__(self) -> None:
        self._jobs: dict[str, DashboardJob] = {}
        self._lock = threading.Lock()

    def create_job(self, country: str, config: dict[str, Any]) -> DashboardJob:
        job = DashboardJob(id=uuid.uuid4().hex[:12], country=country, config=config)
        with self._lock:
            self._jobs[job.id] = job
        return job

    def get(self, job_id: str) -> DashboardJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def recent_snapshots(self) -> list[dict[str, Any]]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda item: item.created_at, reverse=True)
        return [job.snapshot() for job in jobs[:8]]


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    registry = JobRegistry()
    app.config["registry"] = registry

    example_countries = [
        country.title()
        for country in sorted(AFRICA_COUNTRY_ALPHA2.keys())
        if country not in {"republic of the congo", "democratic republic of the congo"}
    ]

    def get_registry() -> JobRegistry:
        return app.config["registry"]

    def require_job(job_id: str) -> DashboardJob:
        job = get_registry().get(job_id)
        if job is None:
            abort(404, description="Job not found.")
        return job

    def launch_job(job: DashboardJob) -> None:
        def runner() -> None:
            job.mark_running()
            try:
                pipeline = get_country_pipeline(job.country)

                def handle_progress(event: dict[str, Any]) -> None:
                    event_payload = dict(event)
                    phase = str(event_payload.pop("phase", "update"))
                    message = str(event_payload.pop("message", "Pipeline update received."))
                    job.add_event(phase, message, **event_payload)

                result = run_country_pipeline(
                    pipeline.config,
                    limit=job.config.get("limit"),
                    max_pages=job.config["max_pages"],
                    second_pass_pages=job.config["second_pass_pages"],
                    workers=job.config["workers"],
                    selected_institutions=job.config["institutions"] or None,
                    output_dir=job.config["output_dir"] or None,
                    progress_callback=handle_progress,
                )
                job.mark_completed(result)
            except Exception as exc:  # noqa: BLE001
                job.mark_failed(str(exc))

        threading.Thread(target=runner, daemon=True, name=f"dialogic-job-{job.id}").start()

    def resolve_artifact_path(job: DashboardJob, artifact: str) -> Path:
        if artifact not in ARTIFACT_KEYS:
            abort(404, description="Unknown artifact.")
        target = job.output_paths.get(artifact)
        if not target:
            abort(404, description="Artifact not ready.")
        resolved = Path(target).resolve()
        if not resolved.exists():
            abort(404, description="Artifact file not found.")
        return resolved

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            brand_name="Dialogic Solution",
            product_name="Atlas Faculty Intelligence Console",
            example_countries=example_countries,
        )

    @app.get("/api/jobs")
    def jobs_list() -> Response:
        return jsonify({"jobs": get_registry().recent_snapshots()})

    @app.post("/api/jobs")
    def jobs_create() -> Response:
        payload = request.get_json(silent=True) or request.form.to_dict()
        country = normalize_country_label(str(payload.get("country", "")))
        if not country:
            return jsonify({"error": "Country is required."}), 400

        config = {
            "country": country,
            "limit": parse_optional_int(payload.get("limit")),
            "max_pages": parse_positive_int(payload.get("max_pages"), default=30, minimum=1, maximum=120),
            "second_pass_pages": parse_positive_int(
                payload.get("second_pass_pages"),
                default=25,
                minimum=0,
                maximum=120,
            ),
            "workers": parse_positive_int(payload.get("workers"), default=6, minimum=1, maximum=12),
            "institutions": split_institutions(str(payload.get("institutions", ""))),
            "output_dir": ensure_repo_relative_path(str(payload.get("output_dir", "")).strip()),
        }

        job = get_registry().create_job(country=country, config=config)
        launch_job(job)
        return jsonify({"job": job.snapshot()}), 201

    @app.get("/api/jobs/<job_id>")
    def jobs_detail(job_id: str) -> Response:
        return jsonify({"job": require_job(job_id).snapshot()})

    @app.get("/api/jobs/<job_id>/events")
    def jobs_events(job_id: str) -> Response:
        job = require_job(job_id)
        start_index = parse_positive_int(request.args.get("since"), default=0, minimum=0, maximum=999999)

        @stream_with_context
        def event_stream() -> Any:
            snapshot = job.snapshot()
            yield f"data: {json.dumps({'type': 'snapshot', 'snapshot': snapshot})}\n\n"
            next_index = start_index
            while True:
                event = job.wait_for_event(next_index, timeout_seconds=10.0)
                snapshot = job.snapshot()
                if event is None:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'snapshot': snapshot})}\n\n"
                    if snapshot["status"] in FINAL_STATUSES:
                        break
                    continue
                next_index = event["index"] + 1
                payload = {
                    "type": "event",
                    "event": event,
                    "snapshot": snapshot,
                }
                yield f"data: {json.dumps(payload)}\n\n"
                if snapshot["status"] in FINAL_STATUSES and next_index >= snapshot["event_count"]:
                    break

        return Response(
            event_stream(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/api/jobs/<job_id>/artifacts/<artifact>")
    def jobs_download_artifact(job_id: str, artifact: str) -> Response:
        job = require_job(job_id)
        path = resolve_artifact_path(job, artifact)
        if path.is_dir():
            return jsonify({"path": str(path)})
        return send_file(path, as_attachment=True)

    @app.post("/api/jobs/<job_id>/reveal/<artifact>")
    def jobs_reveal_artifact(job_id: str, artifact: str) -> Response:
        job = require_job(job_id)
        path = resolve_artifact_path(job, artifact)
        reveal_target = path if artifact == "output_dir" else path.parent
        if not hasattr(os, "startfile"):
            return jsonify({"error": "Reveal is supported only on Windows."}), 400
        os.startfile(str(reveal_target))  # type: ignore[attr-defined]
        return jsonify({"ok": True, "path": str(reveal_target)})

    return app
