(function () {
  const state = {
    activeJobId: "",
    eventSource: null,
    renderedEvents: new Set(),
  };

  const form = document.getElementById("run-form");
  const revealSupported = document.body.dataset.revealSupported === "true";
  const runButton = document.getElementById("run-button");
  const refreshButton = document.getElementById("refresh-jobs");
  const globalStatus = document.getElementById("global-status");
  const jobPill = document.getElementById("job-pill");
  const stageTitle = document.getElementById("stage-title");
  const stageMessage = document.getElementById("stage-message");
  const logConsole = document.getElementById("log-console");
  const consoleCounter = document.getElementById("console-counter");
  const recentJobs = document.getElementById("recent-jobs");
  const floatingDomains = document.getElementById("floating-domains");
  const floatingDeliverables = document.getElementById("floating-deliverables");

  const metricMap = {
    seed_institutions: document.getElementById("metric-seeds"),
    resolved_domains: document.getElementById("metric-domains"),
    institutions_with_records: document.getElementById("metric-institutions"),
    final_professor_rows: document.getElementById("metric-rows"),
    unique_institutional_emails: document.getElementById("metric-emails"),
    excluded_rows: document.getElementById("metric-excluded"),
  };

  const artifactText = {
    output_dir: document.getElementById("artifact-output-dir"),
    csv: document.getElementById("artifact-csv"),
    xlsx: document.getElementById("artifact-xlsx"),
  };

  const artifactLinks = {
    csv: document.getElementById("download-csv"),
    xlsx: document.getElementById("download-xlsx"),
  };

  const revealButton = document.querySelector('[data-artifact="output_dir"][data-action="reveal"]');
  const timelineSteps = Array.from(document.querySelectorAll(".timeline-step"));

  const phaseLabels = {
    dashboard_started: "Dashboard Launch",
    run_started: "Run Initialization",
    seed_loaded: "Official Source Discovery",
    seed_filtered: "Institution Focus Applied",
    seed_limited: "Preview Limit Applied",
    domain_resolved: "Domain Validation",
    crawl_started: "Institution Crawl Started",
    coverage_updated: "Coverage Update",
    crawl_completed: "Institution Crawl Completed",
    export_started: "Exporting Results",
    run_completed: "Pipeline Completed",
    dashboard_completed: "Delivery Ready",
    dashboard_failed: "Run Failed",
  };

  function humanizeTimestamp(value) {
    if (!value) return "";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString();
  }

  function pathLeaf(pathValue) {
    if (!pathValue) return "";
    const parts = String(pathValue).split(/[\\/]/).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : String(pathValue);
  }

  async function parseResponsePayload(response) {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      return response.json();
    }
    return { error: (await response.text()).trim() };
  }

  function setStatus(status, label) {
    globalStatus.className = `status-pill ${status || "idle"}`;
    globalStatus.innerHTML = `<span class="status-dot"></span>${label}`;
  }

  function updateTimeline(phase) {
    const stageIndex =
      phase === "domain_resolved"
        ? 1
        : ["crawl_started", "coverage_updated", "crawl_completed"].includes(phase)
          ? 2
          : ["export_started", "run_completed", "dashboard_completed", "dashboard_failed"].includes(phase)
            ? 3
            : 0;

    timelineSteps.forEach((step, index) => {
      step.classList.toggle("active", index <= stageIndex);
    });
  }

  function metricValue(snapshot, summaryKey, fallbackKey) {
    if (snapshot.summary && snapshot.summary[summaryKey] != null) {
      return snapshot.summary[summaryKey];
    }
    if (snapshot.live_stats && snapshot.live_stats[fallbackKey] != null) {
      return snapshot.live_stats[fallbackKey];
    }
    return 0;
  }

  function updateMetrics(snapshot) {
    metricMap.seed_institutions.textContent = metricValue(snapshot, "seed_institutions", "seed_institutions");
    metricMap.resolved_domains.textContent = metricValue(snapshot, "resolved_domains", "resolved_domains");
    metricMap.institutions_with_records.textContent = metricValue(
      snapshot,
      "institutions_with_records",
      "institutions_with_records"
    );
    metricMap.final_professor_rows.textContent = metricValue(snapshot, "final_professor_rows", "records_found");
    metricMap.unique_institutional_emails.textContent = metricValue(
      snapshot,
      "unique_institutional_emails",
      "unique_institutional_emails"
    );
    metricMap.excluded_rows.textContent = metricValue(snapshot, "excluded_rows", "excluded_rows");

    floatingDomains.textContent = `${metricValue(snapshot, "resolved_domains", "resolved_domains")} validated`;
    floatingDeliverables.textContent =
      snapshot.output_paths && snapshot.output_paths.xlsx ? "Workbook Ready" : "CSV + XLSX";
  }

  function updateArtifacts(snapshot) {
    const hasOutputs = snapshot.output_paths && snapshot.output_paths.output_dir;
    const outputDir = snapshot.output_paths && snapshot.output_paths.output_dir;
    const csvPath = snapshot.output_paths && snapshot.output_paths.csv;
    const xlsxPath = snapshot.output_paths && snapshot.output_paths.xlsx;
    artifactText.output_dir.textContent = hasOutputs
      ? `Ready: ${pathLeaf(outputDir)}`
      : "Run a job to generate an output folder.";
    artifactText.csv.textContent = csvPath
      ? `Ready: ${pathLeaf(csvPath)}`
      : "CSV becomes available after a successful run.";
    artifactText.xlsx.textContent = xlsxPath
      ? `Ready: ${pathLeaf(xlsxPath)}`
      : "Workbook includes Summary, Crawl Log, Domains, and Review_Excluded.";

    artifactText.output_dir.title = outputDir || "";
    artifactText.csv.title = csvPath || "";
    artifactText.xlsx.title = xlsxPath || "";

    revealButton.disabled = !hasOutputs || !revealSupported;
    revealButton.dataset.jobId = snapshot.id || "";
    revealButton.title = revealSupported
      ? "Open the output folder on this machine."
      : "Folder reveal is available only on a local Windows deployment.";

    ["csv", "xlsx"].forEach((key) => {
      const link = artifactLinks[key];
      const ready = snapshot.output_paths && snapshot.output_paths[key];
      link.href = ready ? `/api/jobs/${snapshot.id}/artifacts/${key}` : "#";
      link.setAttribute("aria-disabled", ready ? "false" : "true");
    });
  }

  function appendLog(event) {
    if (state.renderedEvents.has(event.index)) return;
    state.renderedEvents.add(event.index);

    const row = document.createElement("article");
    row.className = "log-line";
    row.innerHTML = `
      <div class="log-tag">${phaseLabels[event.phase] || event.phase}</div>
      <div>
        <p>${event.message}</p>
        <time>${humanizeTimestamp(event.timestamp)}</time>
      </div>
    `;
    logConsole.appendChild(row);
    logConsole.scrollTop = logConsole.scrollHeight;
  }

  function renderSnapshot(snapshot, options = {}) {
    if (snapshot.id) {
      state.activeJobId = snapshot.id;
    }
    const lastEvent = options.lastEvent || (snapshot.events_tail || []).slice(-1)[0];
    const stage = lastEvent ? phaseLabels[lastEvent.phase] || lastEvent.phase : "Standby";
    const message = lastEvent ? lastEvent.message : "Launch a run to begin streaming progress from the extraction pipeline.";

    jobPill.textContent = snapshot.id ? `Job ${snapshot.id}` : "No Active Job";
    stageTitle.textContent = stage;
    stageMessage.textContent = snapshot.error || message;
    consoleCounter.textContent = `${snapshot.event_count || 0} events`;

    const statusLabel =
      snapshot.status === "running"
        ? `Running ${snapshot.country || ""}`.trim()
        : snapshot.status === "completed"
          ? "Delivery Ready"
          : snapshot.status === "failed"
            ? "Attention Needed"
            : "Ready For Launch";
    setStatus(snapshot.status, statusLabel);
    updateTimeline(lastEvent ? lastEvent.phase : "");
    updateMetrics(snapshot);
    updateArtifacts(snapshot);
  }

  function resetConsole() {
    state.renderedEvents.clear();
    logConsole.innerHTML = `
      <div class="empty-state">
        Live run events will appear here once a job starts.
      </div>
    `;
  }

  function renderInitialLogs(snapshot) {
    if (!snapshot.events_tail || !snapshot.events_tail.length) {
      resetConsole();
      return;
    }
    logConsole.innerHTML = "";
    snapshot.events_tail.forEach(appendLog);
  }

  function renderRecentJobs(jobs) {
    if (!jobs.length) {
      recentJobs.innerHTML = '<div class="empty-state">No dashboard activity yet. Launch the first run to populate this panel.</div>';
      return;
    }
    recentJobs.innerHTML = jobs
      .map((job) => {
        const summary = job.summary || {};
        const rowCount = summary.final_professor_rows != null ? summary.final_professor_rows : job.live_stats.records_found;
        const isActive = state.activeJobId && state.activeJobId === job.id;
        return `
          <article class="recent-job ${isActive ? "active" : ""}" data-job-id="${job.id}" role="button" tabindex="0">
            <div class="recent-job-top">
              <h3>${job.country}</h3>
              <span class="status-label ${job.status}">${job.status}</span>
            </div>
            <p>Created ${humanizeTimestamp(job.created_at)}</p>
            <p>${rowCount || 0} final rows, ${summary.resolved_domains || job.live_stats.resolved_domains || 0} resolved domains.</p>
          </article>
        `;
      })
      .join("");
  }

  async function refreshRecentJobs() {
    const response = await fetch("/api/jobs");
    const payload = await response.json();
    const jobs = payload.jobs || [];
    renderRecentJobs(jobs);
    if (!state.activeJobId && jobs.length) {
      selectJob(jobs[0].id, false);
    }
  }

  async function selectJob(jobId, connectStream = true) {
    if (!jobId) return;
    const response = await fetch(`/api/jobs/${jobId}`);
    const payload = await response.json();
    const snapshot = payload.job;
    renderSnapshot(snapshot);
    renderInitialLogs(snapshot);
    renderRecentJobs((await (await fetch("/api/jobs")).json()).jobs || []);
    if (connectStream) {
      connectEvents(jobId);
    }
  }

  function closeEventSource() {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
  }

  function connectEvents(jobId) {
    closeEventSource();
    state.activeJobId = jobId;
    state.eventSource = new EventSource(`/api/jobs/${jobId}/events`);

    state.eventSource.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.type === "snapshot") {
        renderSnapshot(payload.snapshot);
        renderInitialLogs(payload.snapshot);
        return;
      }
      if (payload.type === "heartbeat") {
        renderSnapshot(payload.snapshot);
        return;
      }
      if (payload.type === "event") {
        renderSnapshot(payload.snapshot, { lastEvent: payload.event });
        appendLog(payload.event);
        if (["completed", "failed"].includes(payload.snapshot.status)) {
          runButton.disabled = false;
          runButton.innerHTML = `
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5v14l11-7z" fill="currentColor"></path></svg>
            Launch Run
          `;
          refreshRecentJobs();
        }
      }
    };

    state.eventSource.onerror = () => {
      if (state.eventSource) {
        state.eventSource.close();
      }
    };
  }

  async function launchRun(event) {
    event.preventDefault();
    const payload = {
      country: document.getElementById("country").value,
      limit: document.getElementById("limit").value,
      max_pages: document.getElementById("max_pages").value,
      second_pass_pages: document.getElementById("second_pass_pages").value,
      workers: document.getElementById("workers").value,
      institutions: document.getElementById("institutions").value,
      output_dir: document.getElementById("output_dir").value,
    };

    runButton.disabled = true;
    runButton.innerHTML = `
      <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2a10 10 0 1 1-7.07 2.93" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"></path></svg>
      Launching...
    `;
    resetConsole();

    const response = await fetch("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      runButton.disabled = false;
      runButton.innerHTML = `
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5v14l11-7z" fill="currentColor"></path></svg>
        Launch Run
      `;
      alert(result.error || "Unable to start the run.");
      return;
    }

    renderSnapshot(result.job);
    renderInitialLogs(result.job);
    connectEvents(result.job.id);
    refreshRecentJobs();
  }

  async function revealArtifact() {
    const jobId = revealButton.dataset.jobId || state.activeJobId;
    if (!jobId) {
      alert("Select or run a job first so the output folder is available.");
      return;
    }
    const response = await fetch(`/api/jobs/${jobId}/reveal/output_dir`, {
      method: "POST",
    });
    const payload = await parseResponsePayload(response);
    if (!response.ok) {
      alert(payload.error || "Unable to reveal the output folder.");
      return;
    }
    if (payload && payload.path) {
      console.info(`Output folder opened: ${payload.path}`);
    }
  }

  form.addEventListener("submit", launchRun);
  refreshButton.addEventListener("click", refreshRecentJobs);
  revealButton.addEventListener("click", revealArtifact);
  recentJobs.addEventListener("click", (event) => {
    const card = event.target.closest(".recent-job");
    if (!card) return;
    selectJob(card.dataset.jobId);
  });
  recentJobs.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const card = event.target.closest(".recent-job");
    if (!card) return;
    event.preventDefault();
    selectJob(card.dataset.jobId);
  });
  window.addEventListener("beforeunload", closeEventSource);

  resetConsole();
  refreshRecentJobs();
})();
