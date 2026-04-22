(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  const formEl = byId("requests-form");
  const typeEl = byId("request-type");
  const statusEl = byId("requests-status");
  const submitBtn = byId("requests-submit");
  const bodyEl = byId("requests-body");

  const moduleNameWrap = byId("field-module-name");
  const bugTitleWrap = byId("field-bug-title");
  const roleNamesWrap = byId("field-role-names");
  const requestTitleWrap = byId("field-request-title");

  const moduleNameEl = byId("module-name");
  const bugTitleEl = byId("bug-title");
  const roleNamesEl = byId("role-names");
  const requestTitleEl = byId("request-title");
  const descriptionEl = byId("request-description");

  function setStatus(message, kind) {
    if (!statusEl) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      statusEl.style.display = "none";
      statusEl.textContent = "";
      return;
    }
    statusEl.style.display = "";
    statusEl.className = "admin-inline-status " + (kind || "info");
    statusEl.textContent = text;
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
      const details = Array.isArray(data.errors) && data.errors.length ? " " + data.errors.join(" | ") : "";
      throw new Error((data.error || "Request failed.") + details);
    }
    return data;
  }

  function normalizedTypeLabel(value) {
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

  function normalizedStatusLabel(value) {
    const v = String(value || "").trim().toLowerCase();
    return v === "finished" ? "Finished" : "Pending";
  }

  function resetTypeFields() {
    moduleNameEl.value = "";
    bugTitleEl.value = "";
    roleNamesEl.value = "";
    requestTitleEl.value = "";
  }

  function applyTypeUi() {
    const requestType = String(typeEl.value || "").trim().toLowerCase();
    moduleNameWrap.style.display = requestType === "add_module" ? "" : "none";
    bugTitleWrap.style.display = requestType === "signal_bug" ? "" : "none";
    roleNamesWrap.style.display = requestType === "request_role" ? "" : "none";
    requestTitleWrap.style.display = requestType === "other_request" ? "" : "none";
  }

  function buildPayloadFromForm() {
    const payload = {
      request_type: String(typeEl.value || "").trim().toLowerCase(),
      description: String(descriptionEl.value || "").trim(),
    };

    if (payload.request_type === "add_module") {
      payload.module_name = String(moduleNameEl.value || "").trim();
    } else if (payload.request_type === "signal_bug") {
      payload.bug_title = String(bugTitleEl.value || "").trim();
    } else if (payload.request_type === "request_role") {
      payload.role_names = String(roleNamesEl.value || "")
        .split(",")
        .map(function (part) {
          return part.trim();
        })
        .filter(Boolean);
    } else if (payload.request_type === "other_request") {
      payload.request_title = String(requestTitleEl.value || "").trim();
    }

    return payload;
  }

  function renderRequests(items) {
    if (!bodyEl) {
      return;
    }
    const rows = Array.isArray(items) ? items : [];
    bodyEl.innerHTML = "";

    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = '<td colspan="5" class="admin-empty">No requests submitted yet.</td>';
      bodyEl.appendChild(tr);
      return;
    }

    rows.forEach(function (item) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + normalizedTypeLabel(item.request_type) + "</td>" +
        "<td>" + (item.title || "-") + "</td>" +
        "<td>" + (item.description || "-") + "</td>" +
        "<td>" + normalizedStatusLabel(item.status) + "</td>" +
        "<td>" + (item.created_at || "-") + "</td>";
      bodyEl.appendChild(tr);
    });
  }

  async function loadRequests() {
    try {
      const data = await fetchJson("/api/requests");
      renderRequests(data.requests || []);
    } catch (error) {
      renderRequests([]);
      setStatus(error && error.message ? error.message : "Unable to load requests.", "error");
    }
  }

  async function submitRequest(event) {
    event.preventDefault();
    setStatus("", "info");
    submitBtn.disabled = true;
    submitBtn.textContent = "Submitting...";

    try {
      const payload = buildPayloadFromForm();
      await fetchJson("/api/requests", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setStatus("Request submitted successfully.", "success");
      resetTypeFields();
      descriptionEl.value = "";
      await loadRequests();
    } catch (error) {
      setStatus(error && error.message ? error.message : "Unable to submit request.", "error");
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Submit Request";
    }
  }

  typeEl.addEventListener("change", applyTypeUi);
  formEl.addEventListener("submit", submitRequest);
  applyTypeUi();
  loadRequests();
})();
