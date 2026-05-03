(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  const state = {
    users: [],
    allRoles: [],
    requests: [],
    filePagination: {
      saved: { page: 1, pageSize: 5 },
      generated: { page: 1, pageSize: 5 },
    },
    logsModal: {
      pipelineId: "",
      pipelineName: "",
      logs: [],
    },
    airflowDags: [],
  };

  const usersBody = byId("admin-users-body");
  const rolesChips = byId("admin-roles-chips");
  const statusBox = byId("admin-users-status");
  const filesStatusBox = byId("admin-files-status");
  const totalUsersKpi = byId("admin-total-users");
  const totalSavedPipelinesKpi = byId("admin-total-saved-pipelines");
  const totalGeneratedDagsKpi = byId("admin-total-generated-dags");
  const savedList = byId("admin-recent-saved-list");
  const generatedList = byId("admin-recent-generated-list");
  const savedPaginationBox = byId("admin-saved-pagination");
  const generatedPaginationBox = byId("admin-generated-pagination");
  const requestsBody = byId("admin-requests-body");
  const requestsStatusBox = byId("admin-requests-status");
  const airflowDagsBody = byId("admin-airflow-dags-body");
  const airflowDagsStatusBox = byId("admin-airflow-dags-status");
  const airflowDagsRefreshBtn = byId("admin-airflow-dags-refresh");
  const scheduleOverlay = byId("admin-schedule-overlay");
  const scheduleOkBtn = byId("admin-schedule-ok");
  const scheduleCancelBtn = byId("admin-schedule-cancel");
  const scheduleCloseIconBtn = byId("admin-schedule-close-icon");
  const scheduleStatus = byId("admin-schedule-status");
  const scheduleDailyWrap = byId("admin-schedule-daily-wrap");
  const scheduleHourlyWrap = byId("admin-schedule-hourly-wrap");
  const scheduleCustomWrap = byId("admin-schedule-custom-wrap");
  const scheduleDailyTimeInput = byId("admin-schedule-daily-time");
  const scheduleHourlyMinuteInput = byId("admin-schedule-hourly-minute");
  const scheduleCustomInput = byId("admin-schedule-custom-input");
  const scheduleDailyPreview = byId("admin-schedule-daily-preview");
  const scheduleHourlyPreview = byId("admin-schedule-hourly-preview");
  const scheduleTimezoneLabel = byId("admin-schedule-timezone");
  const pipelineLogsOverlay = byId("admin-pipeline-logs-overlay");
  const pipelineLogsCloseBtn = byId("admin-pipeline-logs-close");
  const pipelineLogsBody = byId("admin-pipeline-logs-body");
  const pipelineLogsSubtitle = byId("admin-pipeline-logs-subtitle");
  const pipelineLogsStatus = byId("admin-pipeline-logs-status");
  const logDetailsOverlay = byId("admin-log-details-overlay");
  const logDetailsCloseBtn = byId("admin-log-details-close");
  const logDetailsBody = byId("admin-log-details-body");
  const logDetailsSubtitle = byId("admin-log-details-subtitle");
  const logDetailsStatus = byId("admin-log-details-status");
  const scheduleRadios = Array.from(document.querySelectorAll('input[name="admin-schedule-mode"]'));
  const scheduleModalState = {
    dagId: "",
    resolve: null,
    timezone: "",
    previousMode: "",
  };

  function parseFileRows(listEl, kind) {
    const rows = [];
    if (!listEl) {
      return rows;
    }
    const items = listEl.querySelectorAll("li");
    items.forEach(function (li) {
      const nameEl = li.querySelector("span");
      const dateEl = li.querySelector("small");
      const deleteBtn = li.querySelector(".admin-file-delete");
      const logsBtn = li.querySelector(".admin-file-logs");
      const filename = deleteBtn ? String(deleteBtn.getAttribute("data-filename") || "").trim() : "";
      const pipelineId = logsBtn ? String(logsBtn.getAttribute("data-pipeline-id") || "").trim() : "";
      rows.push({
        filename: filename || (nameEl ? String(nameEl.textContent || "").trim() : ""),
        updated_at: dateEl ? String(dateEl.textContent || "").trim() : "",
        pipeline_id: pipelineId,
        kind: kind,
      });
    });
    return rows;
  }

  function renderFileListPage(kind, rows, listEl, pagerEl) {
    if (!listEl || !pagerEl) {
      return;
    }

    const pagerState = state.filePagination[kind] || { page: 1, pageSize: 5 };
    const pageSize = Math.max(1, Number(pagerState.pageSize) || 5);
    const totalItems = rows.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
    const currentPage = Math.min(Math.max(1, Number(pagerState.page) || 1), totalPages);
    state.filePagination[kind] = { page: currentPage, pageSize: pageSize };

    const start = (currentPage - 1) * pageSize;
    const end = start + pageSize;
    const pageItems = rows.slice(start, end);

    listEl.innerHTML = "";
    pageItems.forEach(function (item) {
      const li = document.createElement("li");
      const logsButtonHtml = kind === "saved"
        ? '<button type="button" class="btn btn-secondary admin-file-logs" data-file-kind="saved" data-filename="' +
          (item.filename || "") +
          '" data-pipeline-id="' +
          (item.pipeline_id || (item.filename || "").replace(/\.json$/i, "")) +
          '">Logs</button>'
        : "";
      li.innerHTML =
        "<span>" + (item.filename || "-") + "</span>" +
        '<span class="admin-file-meta">' +
        "<small>" + (item.updated_at || "-") + "</small>" +
        logsButtonHtml +
        '<button type="button" class="btn btn-secondary btn-danger-soft admin-file-delete" data-file-kind="' +
        kind +
        '" data-filename="' +
        (item.filename || "") +
        '">Delete</button></span>';
      listEl.appendChild(li);
    });

    pagerEl.innerHTML = "";
    if (totalItems <= pageSize) {
      return;
    }

    const prevBtn = document.createElement("button");
    prevBtn.type = "button";
    prevBtn.className = "btn btn-secondary";
    prevBtn.textContent = "Previous";
    prevBtn.disabled = currentPage <= 1;
    prevBtn.addEventListener("click", function () {
      state.filePagination[kind].page = Math.max(1, currentPage - 1);
      renderFileListPage(kind, rows, listEl, pagerEl);
    });

    const nextBtn = document.createElement("button");
    nextBtn.type = "button";
    nextBtn.className = "btn btn-secondary";
    nextBtn.textContent = "Next";
    nextBtn.disabled = currentPage >= totalPages;
    nextBtn.addEventListener("click", function () {
      state.filePagination[kind].page = Math.min(totalPages, currentPage + 1);
      renderFileListPage(kind, rows, listEl, pagerEl);
    });

    const info = document.createElement("span");
    info.className = "admin-empty";
    info.textContent = "Page " + currentPage + " / " + totalPages + " (" + totalItems + " items)";

    pagerEl.appendChild(prevBtn);
    pagerEl.appendChild(info);
    pagerEl.appendChild(nextBtn);
  }

  function initializeFilePagination() {
    if (savedList && savedPaginationBox) {
      const savedRows = parseFileRows(savedList, "saved");
      renderFileListPage("saved", savedRows, savedList, savedPaginationBox);
    }
    if (generatedList && generatedPaginationBox) {
      const generatedRows = parseFileRows(generatedList, "generated");
      renderFileListPage("generated", generatedRows, generatedList, generatedPaginationBox);
    }
  }

  function setStatus(message, kind) {
    if (!statusBox) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      statusBox.textContent = "";
      statusBox.style.display = "none";
      return;
    }
    statusBox.style.display = "";
    statusBox.className = "admin-inline-status " + (kind || "info");
    statusBox.textContent = text;
  }

  function setFilesStatus(message, kind) {
    if (!filesStatusBox) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      filesStatusBox.textContent = "";
      filesStatusBox.style.display = "none";
      return;
    }
    filesStatusBox.style.display = "";
    filesStatusBox.className = "admin-inline-status " + (kind || "info");
    filesStatusBox.textContent = text;
  }

  function setRequestsStatus(message, kind) {
    if (!requestsStatusBox) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      requestsStatusBox.textContent = "";
      requestsStatusBox.style.display = "none";
      return;
    }
    requestsStatusBox.style.display = "";
    requestsStatusBox.className = "admin-inline-status " + (kind || "info");
    requestsStatusBox.textContent = text;
  }

  function setAirflowDagsStatus(message, kind) {
    if (!airflowDagsStatusBox) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      airflowDagsStatusBox.textContent = "";
      airflowDagsStatusBox.style.display = "none";
      return;
    }
    airflowDagsStatusBox.style.display = "";
    airflowDagsStatusBox.className = "admin-inline-status " + (kind || "info");
    airflowDagsStatusBox.textContent = text;
  }

  function setPipelineLogsStatus(message, kind) {
    if (!pipelineLogsStatus) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      pipelineLogsStatus.textContent = "";
      pipelineLogsStatus.style.display = "none";
      return;
    }
    pipelineLogsStatus.style.display = "";
    pipelineLogsStatus.className = "admin-inline-status " + (kind || "info");
    pipelineLogsStatus.textContent = text;
  }

  function setLogDetailsStatus(message, kind) {
    if (!logDetailsStatus) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      logDetailsStatus.textContent = "";
      logDetailsStatus.style.display = "none";
      return;
    }
    logDetailsStatus.style.display = "";
    logDetailsStatus.className = "admin-inline-status " + (kind || "info");
    logDetailsStatus.textContent = text;
  }

  function setScheduleStatus(message, kind) {
    if (!scheduleStatus) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      scheduleStatus.textContent = "";
      scheduleStatus.style.display = "none";
      return;
    }
    scheduleStatus.style.display = "";
    scheduleStatus.className = "admin-inline-status " + (kind || "info");
    scheduleStatus.textContent = text;
  }

  function formatReadableDate(value) {
    const text = String(value || "").trim();
    if (!text) {
      return "-";
    }
    const dt = new Date(text);
    if (Number.isNaN(dt.getTime())) {
      return text;
    }
    return dt.toLocaleString();
  }

  function formatDuration(ms) {
    const value = Number(ms);
    if (!Number.isFinite(value) || value <= 0) {
      return "-";
    }
    if (value < 1000) {
      return Math.round(value) + " ms";
    }
    const totalSeconds = Math.round(value / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (hours > 0) {
      return hours + "h " + minutes + "m " + seconds + "s";
    }
    if (minutes > 0) {
      return minutes + "m " + seconds + "s";
    }
    return seconds + "s";
  }

  function statusBadgeClass(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "success") {
      return "admin-status-badge is-success";
    }
    if (value === "failed") {
      return "admin-status-badge is-failed";
    }
    if (value === "cancelled" || value === "canceled") {
      return "admin-status-badge is-cancelled";
    }
    if (value === "running") {
      return "admin-status-badge is-running";
    }
    return "admin-status-badge is-pending";
  }

  function closePipelineLogsModal() {
    if (!pipelineLogsOverlay) {
      return;
    }
    pipelineLogsOverlay.hidden = true;
    setPipelineLogsStatus("", "info");
  }

  function closeLogDetailsModal() {
    if (!logDetailsOverlay) {
      return;
    }
    logDetailsOverlay.hidden = true;
    setLogDetailsStatus("", "info");
  }

  function selectedScheduleMode() {
    const selected = scheduleRadios.find(function (radio) {
      return radio.checked;
    });
    return selected ? String(selected.value || "").trim() : "";
  }

  function twoDigit(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) {
      return "00";
    }
    return n < 10 ? "0" + n : String(n);
  }

  function getLocalTimezoneLabel() {
    try {
      return String(Intl.DateTimeFormat().resolvedOptions().timeZone || "").trim();
    } catch (_err) {
      return "";
    }
  }

  function buildFromNowDefaults() {
    const now = new Date();
    const minute = Number(now.getMinutes());
    const hour = Number(now.getHours());
    return {
      dailyTime: twoDigit(hour) + ":" + twoDigit(minute),
      hourlyMinute: String(minute),
    };
  }

  function parseDailyCronToTime(cron) {
    const text = String(cron || "").trim();
    const m = text.match(/^(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\*$/);
    if (!m) {
      return "";
    }
    const minute = Number(m[1]);
    const hour = Number(m[2]);
    if (minute < 0 || minute > 59 || hour < 0 || hour > 23) {
      return "";
    }
    return twoDigit(hour) + ":" + twoDigit(minute);
  }

  function parseHourlyCronToMinute(cron) {
    const text = String(cron || "").trim();
    const m = text.match(/^(\d{1,2})\s+\*\s+\*\s+\*\s+\*$/);
    if (!m) {
      return "";
    }
    const minute = Number(m[1]);
    if (minute < 0 || minute > 59) {
      return "";
    }
    return String(minute);
  }

  function buildDailyCronFromInput() {
    const value = String((scheduleDailyTimeInput && scheduleDailyTimeInput.value) || "").trim();
    const m = value.match(/^(\d{2}):(\d{2})$/);
    if (!m) {
      return "";
    }
    const hour = Number(m[1]);
    const minute = Number(m[2]);
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
      return "";
    }
    return String(minute) + " " + String(hour) + " * * *";
  }

  function buildHourlyCronFromInput() {
    const raw = String((scheduleHourlyMinuteInput && scheduleHourlyMinuteInput.value) || "").trim();
    if (raw === "") {
      return "";
    }
    const minute = Number(raw);
    if (!Number.isInteger(minute) || minute < 0 || minute > 59) {
      return "";
    }
    return String(minute) + " * * * *";
  }

  function scheduleModalResultFromState() {
    const mode = selectedScheduleMode();
    if (!mode) {
      return { isValid: false, error: "Please select a scheduling mode.", schedule: null };
    }
    if (mode === "manual") {
      return { isValid: true, schedule: null };
    }
    if (mode === "@daily") {
      const cron = buildDailyCronFromInput();
      if (!cron) {
        return { isValid: false, error: "Please select a valid daily run time.", schedule: null };
      }
      return { isValid: true, schedule: cron };
    }
    if (mode === "@hourly") {
      const cron = buildHourlyCronFromInput();
      if (!cron) {
        return { isValid: false, error: "Minute must be between 0 and 59.", schedule: null };
      }
      return { isValid: true, schedule: cron };
    }
    if (mode === "custom") {
      const cron = String((scheduleCustomInput && scheduleCustomInput.value) || "").trim();
      if (!cron) {
        return { isValid: false, error: "Cron expression is required for custom scheduling.", schedule: null };
      }
      return { isValid: true, schedule: cron };
    }
    return { isValid: false, error: "Invalid scheduling mode.", schedule: null };
  }

  function refreshScheduleModalUi() {
    const mode = selectedScheduleMode();
    const showCustom = mode === "custom";
    const showDaily = mode === "@daily";
    const showHourly = mode === "@hourly";
    scheduleRadios.forEach(function (radio) {
      const card = radio.closest(".admin-schedule-option");
      if (!card) {
        return;
      }
      if (radio.checked) {
        card.classList.add("is-selected");
      } else {
        card.classList.remove("is-selected");
      }
    });
    if (scheduleDailyWrap) {
      scheduleDailyWrap.hidden = !showDaily;
    }
    if (scheduleHourlyWrap) {
      scheduleHourlyWrap.hidden = !showHourly;
    }
    if (scheduleCustomWrap) {
      scheduleCustomWrap.hidden = !showCustom;
    }
    if (scheduleDailyPreview) {
      scheduleDailyPreview.hidden = !showDaily;
      if (showDaily) {
        const timeValue = String((scheduleDailyTimeInput && scheduleDailyTimeInput.value) || "").trim();
        const tz = scheduleModalState.timezone ? " (" + scheduleModalState.timezone + ")" : "";
        scheduleDailyPreview.textContent = timeValue
          ? "Runs every day at " + timeValue + tz
          : "Select a run time.";
      }
    }
    if (scheduleHourlyPreview) {
      scheduleHourlyPreview.hidden = !showHourly;
      if (showHourly) {
        const minute = String((scheduleHourlyMinuteInput && scheduleHourlyMinuteInput.value) || "").trim();
        const tz2 = scheduleModalState.timezone ? " (" + scheduleModalState.timezone + ")" : "";
        scheduleHourlyPreview.textContent = minute !== ""
          ? "Runs every hour at minute " + twoDigit(minute) + tz2
          : "Select a minute (0-59).";
      }
    }
    const result = scheduleModalResultFromState();
    if (scheduleOkBtn) {
      scheduleOkBtn.disabled = !result.isValid;
    }
    if (result.isValid) {
      setScheduleStatus("", "info");
    }
  }

  function preselectScheduleMode(existingSchedule) {
    const safe = String(existingSchedule || "").trim();
    let mode = "manual";
    let customValue = "";
    const parsedDaily = parseDailyCronToTime(safe);
    const parsedHourly = parseHourlyCronToMinute(safe);
    if (safe === "@daily") {
      mode = "@daily";
    } else if (safe === "@hourly") {
      mode = "@hourly";
    } else if (parsedDaily) {
      mode = "@daily";
    } else if (parsedHourly) {
      mode = "@hourly";
    } else if (safe && safe !== "manual") {
      mode = "custom";
      customValue = safe;
    }
    scheduleRadios.forEach(function (radio) {
      radio.checked = String(radio.value || "") === mode;
    });
    if (scheduleDailyTimeInput) {
      scheduleDailyTimeInput.value = parsedDaily || scheduleDailyTimeInput.value || "";
    }
    if (scheduleHourlyMinuteInput) {
      scheduleHourlyMinuteInput.value = parsedHourly || scheduleHourlyMinuteInput.value || "";
    }
    if (scheduleCustomInput) {
      scheduleCustomInput.value = customValue;
    }
    scheduleModalState.previousMode = mode;
    refreshScheduleModalUi();
  }

  function closeScheduleModal() {
    if (!scheduleOverlay) {
      return;
    }
    scheduleOverlay.hidden = true;
    scheduleModalState.dagId = "";
    scheduleModalState.resolve = null;
    setScheduleStatus("", "info");
  }

  function openScheduleModal(existingSchedule, dagId) {
    if (!scheduleOverlay) {
      return Promise.resolve(null);
    }
    scheduleOverlay.hidden = false;
    scheduleModalState.dagId = String(dagId || "").trim();
    const defaults = buildFromNowDefaults();
    if (scheduleDailyTimeInput) {
      scheduleDailyTimeInput.value = defaults.dailyTime;
    }
    if (scheduleHourlyMinuteInput) {
      scheduleHourlyMinuteInput.value = defaults.hourlyMinute;
    }
    if (scheduleTimezoneLabel) {
      const timezone = getLocalTimezoneLabel();
      scheduleModalState.timezone = timezone;
      scheduleTimezoneLabel.textContent = timezone ? "Local timezone: " + timezone : "Local timezone";
    }
    preselectScheduleMode(existingSchedule);
    if (selectedScheduleMode() === "custom" && scheduleCustomInput) {
      scheduleCustomInput.focus();
    } else if (selectedScheduleMode() === "@daily" && scheduleDailyTimeInput) {
      scheduleDailyTimeInput.focus();
    } else if (selectedScheduleMode() === "@hourly" && scheduleHourlyMinuteInput) {
      scheduleHourlyMinuteInput.focus();
    }
    return new Promise(function (resolve) {
      scheduleModalState.resolve = resolve;
    });
  }

  function resolveScheduleModal(result) {
    const resolver = scheduleModalState.resolve;
    closeScheduleModal();
    if (typeof resolver === "function") {
      resolver(result);
    }
  }

  function renderPipelineLogsRows(logs) {
    if (!pipelineLogsBody) {
      return;
    }
    pipelineLogsBody.innerHTML = "";
    if (!Array.isArray(logs) || !logs.length) {
      const row = document.createElement("tr");
      row.innerHTML = '<td colspan="8" class="admin-empty">No logs found for the last 7 days.</td>';
      pipelineLogsBody.appendChild(row);
      return;
    }

    logs.forEach(function (log) {
      const status = String(log.status || "").trim().toLowerCase();
      const row = document.createElement("tr");
      const executionId = String(log.executionId || "").trim();
      const detailsBtn = executionId
        ? '<button type="button" class="btn btn-secondary admin-log-details-btn" data-execution-id="' + executionId + '">Show details</button>'
        : '<span class="admin-empty">-</span>';
      row.innerHTML =
        "<td>" + formatReadableDate(log.launchedAt || log.createdAt) + "</td>" +
        "<td>" + (log.username || "-") + "</td>" +
        "<td>" + (log.actionType || "-") + "</td>" +
        "<td>" + String(log.retryCount != null ? log.retryCount : "-") + "</td>" +
        "<td>" + formatDuration(log.executionTimeMs) + "</td>" +
        '<td><span class="' + statusBadgeClass(status) + '">' + (status || "unknown") + "</span></td>" +
        "<td>" + (log.message || "-") + "</td>" +
        "<td>" + detailsBtn + "</td>";
      pipelineLogsBody.appendChild(row);
    });
  }

  function renderPipelineLogsLoading() {
    if (!pipelineLogsBody) {
      return;
    }
    pipelineLogsBody.innerHTML = '<tr><td colspan="8" class="admin-empty">Loading logs...</td></tr>';
  }

  function renderLogDetailsLoading() {
    if (!logDetailsBody) {
      return;
    }
    logDetailsBody.innerHTML = '<p class="admin-empty">Loading execution details...</p>';
  }

  function renderLogDetailsTasks(payload) {
    if (!logDetailsBody) {
      return;
    }
    const tasks = payload && Array.isArray(payload.tasks) ? payload.tasks : [];
    if (!tasks.length) {
      logDetailsBody.innerHTML = '<p class="admin-empty">No task/module logs found for this execution.</p>';
      return;
    }

    logDetailsBody.innerHTML = "";
    tasks.forEach(function (task) {
      const box = document.createElement("article");
      box.className = "admin-task-log-box";

      const head = document.createElement("div");
      head.className = "admin-task-log-head";
      head.innerHTML =
        "<strong>" + String(task.task_id || "task") + "</strong>" +
        '<span class="' + statusBadgeClass(task.state) + '">' + String(task.state || "unknown") + "</span>";

      const meta = document.createElement("div");
      meta.className = "admin-task-log-meta admin-empty";
      meta.textContent =
        "Try #" + String(task.try_number || 1) +
        " • Started: " + formatReadableDate(task.start_date) +
        " • Finished: " + formatReadableDate(task.end_date);

      const content = document.createElement("pre");
      content.className = "admin-task-log-content";
      content.textContent = String(task.log_content || task.log_error || "[No log content]").trim();

      box.appendChild(head);
      box.appendChild(meta);
      box.appendChild(content);
      logDetailsBody.appendChild(box);
    });
  }

  async function openLogDetailsModal(executionId) {
    const safeExecutionId = String(executionId || "").trim();
    const safePipelineId = String((state.logsModal && state.logsModal.pipelineId) || "").trim();
    if (!safeExecutionId || !safePipelineId || !logDetailsOverlay) {
      return;
    }
    logDetailsOverlay.hidden = false;
    if (logDetailsSubtitle) {
      logDetailsSubtitle.textContent = safeExecutionId;
    }
    setLogDetailsStatus("Loading execution details...", "info");
    renderLogDetailsLoading();

    try {
      const data = await fetchJson(
        "/api/admin/pipeline-logs/" +
          encodeURIComponent(safePipelineId) +
          "/" +
          encodeURIComponent(safeExecutionId) +
          "/details"
      );
      if (logDetailsSubtitle) {
        const airflowDagId = String(data.airflow_dag_id || "").trim();
        logDetailsSubtitle.textContent = safeExecutionId + (airflowDagId ? " • " + airflowDagId : "");
      }
      renderLogDetailsTasks(data);
      setLogDetailsStatus("", "info");
    } catch (error) {
      renderLogDetailsTasks({ tasks: [] });
      setLogDetailsStatus(error && error.message ? error.message : "Unable to load execution details.", "error");
    }
  }

  async function openPipelineLogsModal(pipelineId, pipelineName) {
    const safePipelineId = String(pipelineId || "").trim();
    if (!safePipelineId || !pipelineLogsOverlay) {
      return;
    }
    state.logsModal.pipelineId = safePipelineId;
    state.logsModal.pipelineName = String(pipelineName || safePipelineId).trim() || safePipelineId;
    state.logsModal.logs = [];

    if (pipelineLogsSubtitle) {
      pipelineLogsSubtitle.textContent = state.logsModal.pipelineName + " • Last 7 days";
    }
    pipelineLogsOverlay.hidden = false;
    setPipelineLogsStatus("Loading logs...", "info");
    renderPipelineLogsLoading();

    try {
      const data = await fetchJson("/api/admin/pipeline-logs/" + encodeURIComponent(safePipelineId));
      state.logsModal.logs = Array.isArray(data.logs) ? data.logs : [];
      renderPipelineLogsRows(state.logsModal.logs);
      setPipelineLogsStatus("", "info");
    } catch (error) {
      renderPipelineLogsRows([]);
      setPipelineLogsStatus(error && error.message ? error.message : "Unable to load logs.", "error");
    }
  }

  function updateMetrics(metrics) {
    if (!metrics || typeof metrics !== "object") {
      return;
    }
    if (totalUsersKpi && metrics.total_users != null) {
      totalUsersKpi.textContent = String(metrics.total_users);
    }
    if (totalSavedPipelinesKpi && metrics.total_saved_pipelines != null) {
      totalSavedPipelinesKpi.textContent = String(metrics.total_saved_pipelines);
    }
    if (totalGeneratedDagsKpi && metrics.total_generated_dags != null) {
      totalGeneratedDagsKpi.textContent = String(metrics.total_generated_dags);
    }
  }

  async function fetchJson(url, options) {
    const resp = await fetch(url, options || {});
    const text = await resp.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch (_err) {
      data = {};
    }
    if (!resp.ok || !data.success) {
      throw new Error(data.error || "Request failed.");
    }
    return data;
  }

  function renderRolesChips() {
    if (!rolesChips) {
      return;
    }
    rolesChips.innerHTML = "";
    state.allRoles.forEach(function (role) {
      const chip = document.createElement("span");
      chip.className = "context-chip";
      chip.textContent = role;
      rolesChips.appendChild(chip);
    });
  }

  function renderUsersTable() {
    if (!usersBody) {
      return;
    }
    usersBody.innerHTML = "";
    if (!state.users.length) {
      const row = document.createElement("tr");
      row.innerHTML = '<td colspan="5" class="admin-empty">No users found.</td>';
      usersBody.appendChild(row);
      return;
    }

    state.users.forEach(function (user) {
      const row = document.createElement("tr");
      const rolesText = (user.roles || []).join(", ") || "-";
      const typeText = user.is_system ? "System" : "Custom";

      row.innerHTML =
        "<td>" +
        (user.id || "-") +
        "</td>" +
        "<td>" +
        (user.username || "-") +
        "</td>" +
        "<td>" +
        rolesText +
        "</td>" +
        "<td>" +
        typeText +
        "</td>" +
        '<td class="connections-actions-cell"></td>';

      const actionsCell = row.querySelector(".connections-actions-cell");
      if (user.is_system) {
        const label = document.createElement("span");
        label.className = "admin-empty";
        label.textContent = "Protected";
        actionsCell.appendChild(label);
      } else {
        const editLink = document.createElement("a");
        editLink.className = "btn btn-secondary";
        editLink.href = "/admin/users/" + encodeURIComponent(user.id) + "/edit";
        editLink.textContent = "Edit";

        const delBtn = document.createElement("button");
        delBtn.type = "button";
        delBtn.className = "btn btn-secondary btn-danger-soft";
        delBtn.textContent = "Delete";
        delBtn.addEventListener("click", function () {
          deleteUser(user);
        });

        actionsCell.appendChild(editLink);
        actionsCell.appendChild(delBtn);
      }

      usersBody.appendChild(row);
    });
  }

  function requestTypeLabel(value) {
    const v = String(value || "").trim().toLowerCase();
    if (v === "add_module") {
      return "Add Module";
    }
    if (v === "signal_bug") {
      return "Signal a Bug";
    }
    if (v === "request_role") {
      return "Request Role";
    }
    if (v === "other_request") {
      return "Other Request";
    }
    return v || "-";
  }

  function renderRequestsTable() {
    if (!requestsBody) {
      return;
    }
    requestsBody.innerHTML = "";
    if (!state.requests.length) {
      const row = document.createElement("tr");
      row.innerHTML = '<td colspan="7" class="admin-empty">No user requests found.</td>';
      requestsBody.appendChild(row);
      return;
    }

    state.requests.forEach(function (item) {
      const requestId = String(item.id || "");
      const status = String(item.status || "pending").toLowerCase();
      const row = document.createElement("tr");
      const statusBadgeClass = status === "finished" ? "admin-status-badge is-finished" : "admin-status-badge is-pending";
      const statusLabel = status === "finished" ? "Finished" : "Pending";
      row.innerHTML =
        "<td>" + requestTypeLabel(item.request_type) + "</td>" +
        "<td>" + (item.title || "-") + "</td>" +
        "<td>" + (item.description || "-") + "</td>" +
        "<td>" + (item.requester_username || item.requester_user_id || "-") + "</td>" +
        '<td><span class="' + statusBadgeClass + '">' + statusLabel + "</span></td>" +
        "<td>" + (item.created_at || "-") + "</td>" +
        '<td class="connections-actions-cell"></td>';

      const actionsCell = row.querySelector(".connections-actions-cell");
      if (status !== "finished") {
        const finishBtn = document.createElement("button");
        finishBtn.type = "button";
        finishBtn.className = "btn btn-primary admin-mark-finished";
        finishBtn.textContent = "Mark Finished";
        finishBtn.addEventListener("click", function () {
          markRequestFinished(requestId);
        });
        actionsCell.appendChild(finishBtn);
      }

      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "btn btn-secondary btn-danger-soft";
      delBtn.textContent = "Delete";
      delBtn.addEventListener("click", function () {
        deleteRequest(requestId, item.title || requestId);
      });
      actionsCell.appendChild(delBtn);

      requestsBody.appendChild(row);
    });
  }

  function renderAirflowDagsTable() {
    if (!airflowDagsBody) {
      return;
    }
    airflowDagsBody.innerHTML = "";
    if (!Array.isArray(state.airflowDags) || !state.airflowDags.length) {
      const row = document.createElement("tr");
      row.innerHTML = '<td colspan="6" class="admin-empty">No DAGs found in Airflow.</td>';
      airflowDagsBody.appendChild(row);
      return;
    }

    state.airflowDags.forEach(function (item) {
      const dagId = String(item.dag_id || "").trim();
      const schedule = String(item.timetable_summary || item.schedule_interval || "").trim() || "manual";
      const nextRun = formatReadableDate(item.next_dagrun);
      const pausedLabel = item.is_paused ? "Yes" : "No";
      const builderLabel = item.has_builder_version
        ? String(item.pipeline_id || "") + " / " + String(item.version_id || "")
        : "Not linked";
      const row = document.createElement("tr");
      row.innerHTML =
        '<td><button type="button" class="btn btn-secondary admin-airflow-dag-open" data-builder-url="' +
        String(item.builder_url || "") +
        '">' +
        (dagId || "-") +
        "</button></td>" +
        "<td>" + schedule + "</td>" +
        "<td>" + nextRun + "</td>" +
        "<td>" + pausedLabel + "</td>" +
        "<td>" + builderLabel + "</td>" +
        '<td class="connections-actions-cell">' +
        '<button type="button" class="btn btn-primary admin-airflow-dag-run" data-dag-id="' + dagId + '">Run</button>' +
        '<button type="button" class="btn btn-secondary admin-airflow-dag-schedule" data-dag-id="' + dagId + '" data-current-schedule="' + schedule + '">Scheduling Mode</button>' +
        '<button type="button" class="btn btn-secondary btn-danger-soft admin-airflow-dag-delete" data-dag-id="' + dagId + '">Delete</button>' +
        "</td>";
      airflowDagsBody.appendChild(row);
    });
  }

  async function loadAirflowDags() {
    try {
      setAirflowDagsStatus("Loading Airflow DAGs...", "info");
      const data = await fetchJson("/api/admin/airflow-dags");
      state.airflowDags = Array.isArray(data.dags) ? data.dags : [];
      renderAirflowDagsTable();
      setAirflowDagsStatus("", "info");
    } catch (error) {
      state.airflowDags = [];
      renderAirflowDagsTable();
      setAirflowDagsStatus(error && error.message ? error.message : "Unable to load Airflow DAGs.", "error");
    }
  }

  async function runAirflowDag(dagId) {
    const safeDagId = String(dagId || "").trim();
    if (!safeDagId) {
      return;
    }
    try {
      await fetchJson("/api/admin/airflow-dags/" + encodeURIComponent(safeDagId) + "/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      setAirflowDagsStatus("Run started for " + safeDagId + ".", "success");
    } catch (error) {
      setAirflowDagsStatus(error && error.message ? error.message : "Unable to run DAG.", "error");
    }
  }

  async function deleteAirflowDag(dagId) {
    const safeDagId = String(dagId || "").trim();
    if (!safeDagId) {
      return;
    }
    if (!window.confirm("Delete DAG '" + safeDagId + "' from Airflow?")) {
      return;
    }
    try {
      await fetchJson("/api/admin/airflow-dags/" + encodeURIComponent(safeDagId), { method: "DELETE" });
      setAirflowDagsStatus("Deleted DAG " + safeDagId + ".", "success");
      await loadAirflowDags();
    } catch (error) {
      setAirflowDagsStatus(error && error.message ? error.message : "Unable to delete DAG.", "error");
    }
  }

  async function changeAirflowDagSchedule(dagId, currentSchedule) {
    const safeDagId = String(dagId || "").trim();
    if (!safeDagId) {
      return;
    }
    const selection = await openScheduleModal(currentSchedule, safeDagId);
    if (!selection || selection.cancelled) {
      return;
    }
    try {
      const data = await fetchJson("/api/admin/airflow-dags/" + encodeURIComponent(safeDagId) + "/schedule", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ schedule: selection.schedule }),
      });
      if (data && data.warning) {
        setAirflowDagsStatus("Schedule updated for " + safeDagId + ". " + String(data.warning), "success");
      } else {
        setAirflowDagsStatus("Schedule updated for " + safeDagId + ".", "success");
      }
      await loadAirflowDags();
    } catch (error) {
      setAirflowDagsStatus(
        (error && error.message ? error.message : "Unable to update schedule.") +
          " Airflow may not allow schedule patching in this deployment.",
        "error"
      );
    }
  }

  async function deleteUser(user) {
    if (!user || !user.id) {
      return;
    }
    const ok = window.confirm("Delete user '" + (user.username || user.id) + "'?");
    if (!ok) {
      return;
    }
    try {
      const data = await fetchJson("/api/admin/users/" + encodeURIComponent(user.id), { method: "DELETE" });
      state.users = Array.isArray(data.users) ? data.users : state.users;
      state.allRoles = Array.isArray(data.all_roles) ? data.all_roles : state.allRoles;
      updateMetrics(data.metrics || {});
      renderRolesChips();
      renderUsersTable();
      setStatus("User deleted.", "success");
    } catch (error) {
      setStatus(error && error.message ? error.message : "Unable to delete user.", "error");
    }
  }

  async function loadUsers() {
    try {
      const initialStatusText = statusBox ? String(statusBox.textContent || "").trim() : "";
      if (!statusBox || !initialStatusText) {
        setStatus("Loading users...", "info");
      }
      const data = await fetchJson("/api/admin/users");
      state.users = Array.isArray(data.users) ? data.users : [];
      state.allRoles = Array.isArray(data.all_roles) ? data.all_roles : [];
      renderRolesChips();
      renderUsersTable();
      const currentStatusText = statusBox ? String(statusBox.textContent || "").trim().toLowerCase() : "";
      const isLoadingStatus = currentStatusText === "loading users...";
      if (statusBox && (isLoadingStatus || statusBox.className.indexOf("error") !== -1)) {
        setStatus("", "info");
      }
    } catch (error) {
      setStatus(error && error.message ? error.message : "Failed to load users.", "error");
      renderUsersTable();
    }
  }

  async function loadRequests() {
    try {
      setRequestsStatus("Loading requests...", "info");
      const data = await fetchJson("/api/admin/requests");
      state.requests = Array.isArray(data.requests) ? data.requests : [];
      renderRequestsTable();
      setRequestsStatus("", "info");
    } catch (error) {
      setRequestsStatus(error && error.message ? error.message : "Failed to load requests.", "error");
      renderRequestsTable();
    }
  }

  async function markRequestFinished(requestId) {
    const safeId = String(requestId || "").trim();
    if (!safeId) {
      return;
    }
    try {
      const data = await fetchJson("/api/admin/requests/" + encodeURIComponent(safeId) + "/finish", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      state.requests = Array.isArray(data.requests) ? data.requests : state.requests;
      renderRequestsTable();
      setRequestsStatus("Request marked as finished.", "success");
    } catch (error) {
      setRequestsStatus(error && error.message ? error.message : "Unable to update request.", "error");
    }
  }

  async function deleteRequest(requestId, title) {
    const safeId = String(requestId || "").trim();
    if (!safeId) {
      return;
    }
    const ok = window.confirm("Delete request '" + (title || safeId) + "'?");
    if (!ok) {
      return;
    }
    try {
      const data = await fetchJson("/api/admin/requests/" + encodeURIComponent(safeId), { method: "DELETE" });
      state.requests = Array.isArray(data.requests) ? data.requests : state.requests;
      renderRequestsTable();
      setRequestsStatus("Request deleted.", "success");
    } catch (error) {
      setRequestsStatus(error && error.message ? error.message : "Unable to delete request.", "error");
    }
  }

  function maybeRenderEmptyState(listElement, cardElement, message, emptyId) {
    if (!listElement) {
      return;
    }
    if (listElement.querySelector("li")) {
      return;
    }
    listElement.remove();
    if (byId(emptyId)) {
      return;
    }
    const empty = document.createElement("p");
    empty.id = emptyId;
    empty.className = "admin-empty";
    empty.textContent = message;
    if (cardElement) {
      cardElement.appendChild(empty);
    }
  }

  async function deleteAdminFile(kind, filename, buttonEl) {
    const safeKind = String(kind || "").trim().toLowerCase();
    const safeName = String(filename || "").trim();
    if (!safeName || (safeKind !== "saved" && safeKind !== "generated")) {
      return;
    }

    const label = safeKind === "saved" ? "saved pipeline file" : "generated DAG file";
    const confirmed = window.confirm("Delete " + label + " '" + safeName + "'?");
    if (!confirmed) {
      return;
    }

    const endpointBase = safeKind === "saved" ? "/api/admin/saved-pipelines/" : "/api/admin/generated-dags/";
    if (buttonEl) {
      buttonEl.disabled = true;
    }

    try {
      const data = await fetchJson(endpointBase + encodeURIComponent(safeName), { method: "DELETE" });
      updateMetrics(data.metrics || {});
      const row = buttonEl ? buttonEl.closest("li") : null;
      if (row) {
        const list = row.parentElement;
        const card = list ? list.closest(".admin-card") : null;
        row.remove();
        if (safeKind === "saved") {
          maybeRenderEmptyState(list, card, "No saved pipelines yet.", "admin-empty-saved");
        } else {
          maybeRenderEmptyState(list, card, "No generated DAG files yet.", "admin-empty-generated");
        }
      }
      setFilesStatus("Deleted: " + safeName, "success");
    } catch (error) {
      setFilesStatus(error && error.message ? error.message : "Unable to delete file.", "error");
      if (buttonEl) {
        buttonEl.disabled = false;
      }
    }
  }

  document.addEventListener("click", function (event) {
    const openDagBtn = event.target.closest(".admin-airflow-dag-open");
    if (openDagBtn) {
      event.preventDefault();
      const builderUrl = String(openDagBtn.getAttribute("data-builder-url") || "").trim();
      if (!builderUrl) {
        setAirflowDagsStatus("No linked pipeline version exists for this DAG.", "info");
        return;
      }
      window.location.href = builderUrl;
      return;
    }

    const runDagBtn = event.target.closest(".admin-airflow-dag-run");
    if (runDagBtn) {
      event.preventDefault();
      runAirflowDag(runDagBtn.getAttribute("data-dag-id") || "");
      return;
    }

    const deleteDagBtn = event.target.closest(".admin-airflow-dag-delete");
    if (deleteDagBtn) {
      event.preventDefault();
      deleteAirflowDag(deleteDagBtn.getAttribute("data-dag-id") || "");
      return;
    }

    const scheduleDagBtn = event.target.closest(".admin-airflow-dag-schedule");
    if (scheduleDagBtn) {
      event.preventDefault();
      changeAirflowDagSchedule(
        scheduleDagBtn.getAttribute("data-dag-id") || "",
        scheduleDagBtn.getAttribute("data-current-schedule") || ""
      );
      return;
    }

    const logsBtn = event.target.closest(".admin-file-logs");
    if (logsBtn) {
      event.preventDefault();
      const pipelineId = logsBtn.getAttribute("data-pipeline-id") || "";
      const filename = logsBtn.getAttribute("data-filename") || pipelineId;
      openPipelineLogsModal(pipelineId, filename);
      return;
    }

    const detailBtn = event.target.closest(".admin-log-details-btn");
    if (detailBtn) {
      event.preventDefault();
      const executionId = detailBtn.getAttribute("data-execution-id") || "";
      openLogDetailsModal(executionId);
      return;
    }

    const btn = event.target.closest(".admin-file-delete");
    if (!btn) {
      return;
    }
    event.preventDefault();
    const kind = btn.getAttribute("data-file-kind");
    const filename = btn.getAttribute("data-filename");
    deleteAdminFile(kind, filename, btn);
  });

  if (pipelineLogsCloseBtn) {
    pipelineLogsCloseBtn.addEventListener("click", function () {
      closePipelineLogsModal();
    });
  }
  if (pipelineLogsOverlay) {
    pipelineLogsOverlay.addEventListener("click", function (event) {
      if (event.target === pipelineLogsOverlay) {
        closePipelineLogsModal();
      }
    });
  }
  if (logDetailsCloseBtn) {
    logDetailsCloseBtn.addEventListener("click", function () {
      closeLogDetailsModal();
    });
  }
  if (logDetailsOverlay) {
    logDetailsOverlay.addEventListener("click", function (event) {
      if (event.target === logDetailsOverlay) {
        closeLogDetailsModal();
      }
    });
  }
  if (scheduleOkBtn) {
    scheduleOkBtn.addEventListener("click", function () {
      const result = scheduleModalResultFromState();
      if (!result.isValid) {
        setScheduleStatus(result.error || "Please provide a valid scheduling mode.", "error");
        return;
      }
      resolveScheduleModal({ cancelled: false, schedule: result.schedule });
    });
  }
  if (scheduleCancelBtn) {
    scheduleCancelBtn.addEventListener("click", function () {
      resolveScheduleModal({ cancelled: true, schedule: null });
    });
  }
  if (scheduleCloseIconBtn) {
    scheduleCloseIconBtn.addEventListener("click", function () {
      resolveScheduleModal({ cancelled: true, schedule: null });
    });
  }
  if (scheduleOverlay) {
    scheduleOverlay.addEventListener("click", function (event) {
      if (event.target === scheduleOverlay) {
        resolveScheduleModal({ cancelled: true, schedule: null });
      }
    });
  }
  scheduleRadios.forEach(function (radio) {
    radio.addEventListener("change", function () {
      const mode = String(radio.value || "").trim();
      if (radio.checked) {
        scheduleModalState.previousMode = mode;
      }
      refreshScheduleModalUi();
    });
  });
  if (scheduleDailyTimeInput) {
    scheduleDailyTimeInput.addEventListener("input", function () {
      refreshScheduleModalUi();
    });
  }
  if (scheduleHourlyMinuteInput) {
    scheduleHourlyMinuteInput.addEventListener("input", function () {
      refreshScheduleModalUi();
    });
  }
  if (scheduleCustomInput) {
    scheduleCustomInput.addEventListener("input", function () {
      refreshScheduleModalUi();
    });
  }
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && pipelineLogsOverlay && !pipelineLogsOverlay.hidden) {
      closePipelineLogsModal();
      return;
    }
    if (event.key === "Escape" && logDetailsOverlay && !logDetailsOverlay.hidden) {
      closeLogDetailsModal();
      return;
    }
    if (event.key === "Escape" && scheduleOverlay && !scheduleOverlay.hidden) {
      resolveScheduleModal({ cancelled: true, schedule: null });
    }
  });

  if (airflowDagsRefreshBtn) {
    airflowDagsRefreshBtn.addEventListener("click", function () {
      loadAirflowDags();
    });
  }

  loadUsers();
  loadRequests();
  loadAirflowDags();
  initializeFilePagination();
})();
