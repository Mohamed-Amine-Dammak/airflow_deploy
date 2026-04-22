(function () {
  const canViewConnections = Boolean(window.CAN_VIEW_CONNECTIONS);
  const canManageConnections = Boolean(window.CAN_MANAGE_CONNECTIONS);
  const state = {
    items: [],
    loading: false,
    submitting: false,
    search: "",
    searchTouched: false,
    mode: "hidden",
    editingId: "",
    form: emptyForm(),
  };

  const statusEl = document.getElementById("connections-status");
  const tableBodyEl = document.getElementById("connections-table-body");
  const searchEl = document.getElementById("connections-search");
  const refreshBtn = document.getElementById("connections-refresh");
  const createBtn = document.getElementById("connections-create");
  const editorTitleEl = document.getElementById("conn-editor-title");
  const editorOverlayEl = document.getElementById("connections-editor-overlay");
  const editorCardEl = document.getElementById("connections-editor-card");
  const modalCloseBtn = document.getElementById("conn-modal-close");
  const resetBtn = document.getElementById("conn-editor-reset");
  const cancelBtn = document.getElementById("conn-cancel");
  const saveBtn = document.getElementById("conn-save");
  const actionsHeaderEl = document.querySelector(".connections-table thead th:last-child");

  const fieldEls = {
    connection_id: document.getElementById("conn-id"),
    connection_type: document.getElementById("conn-type"),
    description: document.getElementById("conn-description"),
    host: document.getElementById("conn-host"),
    login: document.getElementById("conn-login"),
    password: document.getElementById("conn-password"),
    port: document.getElementById("conn-port"),
    schema: document.getElementById("conn-schema"),
    account_url: document.getElementById("conn-account-url"),
    blob_login: document.getElementById("conn-blob-login"),
    blob_key: document.getElementById("conn-blob-key"),
    service_account_file_upload: document.getElementById("conn-gcp-service-account-file"),
    extra: document.getElementById("conn-extra"),
  };

  const errorEls = {
    connection_id: document.getElementById("err-connection_id"),
    connection_type: document.getElementById("err-connection_type"),
    host: document.getElementById("err-host"),
    port: document.getElementById("err-port"),
    account_url: document.getElementById("err-account_url"),
    service_account_file_upload: document.getElementById("err-service_account_file_upload"),
    extra: document.getElementById("err-extra"),
  };

  const httpFieldsEl = document.getElementById("http-fields");
  const wasbFieldsEl = document.getElementById("wasb-fields");
  const gcpFieldsEl = document.getElementById("gcp-fields");

  function emptyForm() {
    return {
      connection_id: "",
      connection_type: "",
      description: "",
      host: "",
      login: "",
      password: "",
      port: "",
      schema: "",
      account_url: "",
      service_account_file_upload: null,
      extra: "",
    };
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function normalizeConnectionType(rawType) {
    const value = String(rawType || "").trim().toLowerCase();
    if (value === "http") {
      return "http";
    }
    if (value === "wasb" || value === "azure_blob_storage" || value === "azure_blob") {
      return "wasb";
    }
    if (value === "gcp_service_account" || value === "google_service_account" || value === "gcp" || value === "google") {
      return "gcp_service_account";
    }
    return "";
  }

  function getApiBaseOverride() {
    try {
      const params = new URLSearchParams(window.location.search || "");
      if (params.get("clearApiBase") === "1") {
        window.localStorage.removeItem("pipeline_api_base");
      }
      const queryBase = String(params.get("apiBase") || "").trim();
      if (queryBase) {
        window.localStorage.setItem("pipeline_api_base", queryBase);
      }
      const storedBase = String(window.localStorage.getItem("pipeline_api_base") || "").trim();
      return String(window.PIPELINE_API_BASE || queryBase || storedBase || "").trim();
    } catch (_err) {
      return String(window.PIPELINE_API_BASE || "").trim();
    }
  }

  function buildApiCandidates(path) {
    const apiBaseOverride = getApiBaseOverride();
    const cleanPath = String(path || "").startsWith("/") ? String(path) : "/" + String(path || "");
    const protocol = String(window.location.protocol || "").toLowerCase();
    const host = String(window.location.hostname || "").trim();
    const rawBases = [];
    if (apiBaseOverride) {
      rawBases.push(String(apiBaseOverride));
    }
    rawBases.push("");
    rawBases.push(window.location.origin);
    if (protocol === "https:") {
      rawBases.push("https://127.0.0.1:5000");
      rawBases.push("https://localhost:5000");
    } else {
      rawBases.push("http://127.0.0.1:5000");
      rawBases.push("http://localhost:5000");
      rawBases.push("http://127.0.0.1:5001");
      rawBases.push("http://localhost:5001");
    }
    if (host && host !== "127.0.0.1" && host !== "localhost") {
      rawBases.push(protocol === "https:" ? "https://" + host + ":5000" : "http://" + host + ":5000");
    }

    const unique = [];
    rawBases.forEach(function (base) {
      const normalized = String(base || "").trim().replace(/\/+$/, "");
      const url = normalized ? normalized + cleanPath : cleanPath;
      if (!unique.includes(url)) {
        unique.push(url);
      }
    });
    return unique;
  }

  async function fetchApi(path, options) {
    const candidates = buildApiCandidates(path);
    let lastError = null;

    for (let i = 0; i < candidates.length; i += 1) {
      const url = candidates[i];
      try {
        return await fetch(url, options || {});
      } catch (error) {
        lastError = error;
        const msg = error && error.message ? String(error.message).toLowerCase() : "";
        if (msg.includes("failed to fetch") || msg.includes("networkerror")) {
          continue;
        }
        throw error;
      }
    }

    throw new Error(
      "Unable to reach backend API. Tried: " +
        candidates.join(", ") +
        ". " +
        (lastError && lastError.message ? lastError.message : "Network request failed.")
    );
  }

  async function parseApiJson(response) {
    const text = await response.text();
    try {
      return JSON.parse(text);
    } catch (_err) {
      throw new Error("Invalid server response (" + response.status + ").");
    }
  }

  function setStatus(kind, message) {
    statusEl.className = "connections-status " + (kind ? "status-" + kind : "");
    statusEl.textContent = message || "";
  }

  function clearFieldErrors() {
    Object.keys(errorEls).forEach(function (key) {
      if (errorEls[key]) {
        errorEls[key].hidden = true;
        errorEls[key].textContent = "";
      }
    });
  }

  function setFieldErrors(errors) {
    clearFieldErrors();
    Object.keys(errors || {}).forEach(function (field) {
      const el = errorEls[field];
      if (el) {
        el.hidden = false;
        el.textContent = String(errors[field] || "");
      }
    });
  }

  function syncFormFromInputs() {
    state.form.connection_id = fieldEls.connection_id.value.trim();
    state.form.connection_type = normalizeConnectionType(fieldEls.connection_type.value);
    state.form.description = fieldEls.description.value.trim();

    if (state.form.connection_type === "http") {
      state.form.host = fieldEls.host.value.trim();
      state.form.login = fieldEls.login.value.trim();
      state.form.password = fieldEls.password.value;
      state.form.port = fieldEls.port.value.trim();
      state.form.schema = fieldEls.schema.value.trim();
      state.form.account_url = "";
    } else if (state.form.connection_type === "wasb") {
      state.form.account_url = fieldEls.account_url.value.trim();
      state.form.login = fieldEls.blob_login.value.trim();
      state.form.password = fieldEls.blob_key.value;
      state.form.host = "";
      state.form.port = "";
      state.form.schema = "";
      state.form.service_account_file_upload = null;
    } else if (state.form.connection_type === "gcp_service_account") {
      state.form.host = "";
      state.form.login = "";
      state.form.password = "";
      state.form.port = "";
      state.form.schema = "";
      state.form.account_url = "";
      state.form.service_account_file_upload = fieldEls.service_account_file_upload.files
        ? fieldEls.service_account_file_upload.files[0] || null
        : null;
    } else {
      state.form.host = "";
      state.form.login = "";
      state.form.password = "";
      state.form.port = "";
      state.form.schema = "";
      state.form.account_url = "";
      state.form.service_account_file_upload = null;
    }

    state.form.extra = fieldEls.extra.value.trim();
  }

  function fillInputsFromForm() {
    fieldEls.connection_id.value = state.form.connection_id || "";
    fieldEls.connection_type.value = state.form.connection_type || "";
    fieldEls.description.value = state.form.description || "";
    fieldEls.host.value = state.form.host || "";
    fieldEls.login.value = state.form.login || "";
    fieldEls.password.value = state.form.password || "";
    fieldEls.port.value = state.form.port || "";
    fieldEls.schema.value = state.form.schema || "";
    fieldEls.account_url.value = state.form.account_url || "";
    fieldEls.blob_login.value = state.form.login || "";
    fieldEls.blob_key.value = state.form.password || "";
    if (fieldEls.service_account_file_upload) {
      fieldEls.service_account_file_upload.value = "";
    }
    fieldEls.extra.value = state.form.extra || "";
  }

  function updateTypeSections() {
    const type = normalizeConnectionType(fieldEls.connection_type.value);
    httpFieldsEl.hidden = type !== "http";
    wasbFieldsEl.hidden = type !== "wasb";
    gcpFieldsEl.hidden = type !== "gcp_service_account";
  }

  function setEditorMode(mode, item) {
    if (!canManageConnections && mode !== "hidden") {
      return;
    }
    clearFieldErrors();
    state.mode = mode;
    state.editingId = mode === "edit" && item ? String(item.connection_id || "") : "";

    if (mode === "hidden") {
      state.mode = "hidden";
      state.editingId = "";
      state.form = emptyForm();
      editorTitleEl.textContent = "Create Connection";
      fieldEls.connection_id.readOnly = false;
      fieldEls.connection_type.disabled = false;
      fillInputsFromForm();
      updateTypeSections();
      editorOverlayEl.hidden = true;
      editorCardEl.hidden = true;
      return;
    }

    editorOverlayEl.hidden = false;
    editorCardEl.hidden = false;

    if (mode === "edit" && item) {
      const normalizedType = normalizeConnectionType(item.conn_type);
      state.form = {
        connection_id: String(item.connection_id || ""),
        connection_type: normalizedType,
        description: String(item.description || ""),
        host: normalizedType === "http" ? String(item.host || "") : "",
        login: String(item.login || ""),
        password: "",
        port: item.port == null ? "" : String(item.port),
        schema: String(item.schema || ""),
        account_url: normalizedType === "wasb" ? String(item.host || "") : "",
        service_account_file_upload: null,
        extra:
          typeof item.extra === "string"
            ? item.extra
            : item.extra
            ? JSON.stringify(item.extra, null, 2)
            : "",
      };
      editorTitleEl.textContent = "Modify Connection";
      fieldEls.connection_id.readOnly = true;
      fieldEls.connection_type.disabled = true;
    } else {
      state.form = emptyForm();
      editorTitleEl.textContent = "Create Connection";
      fieldEls.connection_id.readOnly = false;
      fieldEls.connection_type.disabled = false;
    }

    fillInputsFromForm();
    updateTypeSections();
    window.requestAnimationFrame(function () {
      if (state.mode === "edit") {
        if (state.form.connection_type === "http" && fieldEls.host) {
          fieldEls.host.focus();
          return;
        }
        if (state.form.connection_type === "wasb" && fieldEls.account_url) {
          fieldEls.account_url.focus();
          return;
        }
        if (state.form.connection_type === "gcp_service_account" && fieldEls.service_account_file_upload) {
          fieldEls.service_account_file_upload.focus();
          return;
        }
      }
      if (fieldEls.connection_id) {
        fieldEls.connection_id.focus();
      }
    });
  }

  function validateForm() {
    syncFormFromInputs();
    const errors = {};

    if (state.mode === "create" && !state.form.connection_id) {
      errors.connection_id = "Connection ID is required.";
    }
    if (!state.form.connection_type) {
      errors.connection_type = "Connection type is required.";
    }

    if (state.form.connection_type === "http" && !state.form.host) {
      errors.host = "Host is required for HTTP connections.";
    }

    if (state.form.connection_type === "wasb" && !state.form.account_url) {
      errors.account_url = "Account URL is required for Azure Blob Storage connections.";
    }
    if (
      state.form.connection_type === "gcp_service_account" &&
      state.mode === "create" &&
      !state.form.service_account_file_upload
    ) {
      errors.service_account_file_upload = "Service account JSON file is required.";
    }

    if (state.form.port) {
      const portNum = Number(state.form.port);
      if (!Number.isInteger(portNum) || portNum <= 0) {
        errors.port = "Port must be a valid integer greater than 0.";
      }
    }

    if (state.form.extra) {
      try {
        const parsed = JSON.parse(state.form.extra);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          errors.extra = "Extra must be a JSON object.";
        }
      } catch (_err) {
        errors.extra = "Extra must be valid JSON.";
      }
    }

    setFieldErrors(errors);
    return errors;
  }

  function renderTable() {
    const q = state.searchTouched ? String(state.search || "").trim().toLowerCase() : "";
    const items = q
      ? state.items.filter(function (item) {
          return (
            String(item.connection_id || "").toLowerCase().includes(q) ||
            String(item.conn_type || "").toLowerCase().includes(q) ||
            String(item.description || "").toLowerCase().includes(q)
          );
        })
      : state.items;

    if (!items.length) {
      tableBodyEl.innerHTML =
        '<tr><td colspan="4" class="connections-empty-row">' +
        (state.loading ? "Loading connections..." : "No connections found.") +
        "</td></tr>";
      return;
    }

    tableBodyEl.innerHTML = items
      .map(function (item) {
        const safeId = escapeHtml(item.connection_id || "");
        const normalizedType = normalizeConnectionType(item.conn_type);
        const safeType = normalizedType === "http"
          ? "HTTP"
          : normalizedType === "wasb"
          ? "Azure Blob Storage"
          : normalizedType === "gcp_service_account"
          ? "Google Service Account"
          : "Unknown";
        return (
          "<tr>" +
          "<td>" +
          safeId +
          "</td>" +
          "<td>" +
          safeType +
          "</td>" +
          "<td>" +
          escapeHtml(item.description || "-") +
          "</td>" +
          '<td class="connections-actions-cell">' +
          (canManageConnections
            ? '<button class="btn btn-secondary conn-action-edit" type="button" data-conn-id="' +
              safeId +
              '">Edit</button>' +
              '<button class="btn btn-secondary conn-action-delete" type="button" data-conn-id="' +
              safeId +
              '">Delete</button>'
            : '<span class="admin-empty">Read-only</span>') +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadConnections(showMessageOnError) {
    if (!canViewConnections) {
      state.loading = false;
      state.items = [];
      renderTable();
      if (showMessageOnError) {
        setStatus("warn", "You do not have permission to view Airflow connections.");
      }
      return false;
    }
    state.loading = true;
    renderTable();
    try {
      const resp = await fetchApi("/api/airflow/connections");
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to fetch Airflow connections.");
      }
      state.items = Array.isArray(data.connections) ? data.connections : [];
      state.loading = false;
      renderTable();
      return true;
    } catch (error) {
      state.loading = false;
      renderTable();
      if (showMessageOnError) {
        setStatus("error", error && error.message ? error.message : "Unable to load Airflow connections.");
      }
      return false;
    }
  }

  function buildPayload() {
    const payload = {
      connection_type: state.form.connection_type,
      description: state.form.description,
      login: state.form.login,
      password: state.form.password,
      extra: state.form.extra,
    };

    if (state.mode === "create") {
      payload.connection_id = state.form.connection_id;
    }

    if (state.form.connection_type === "http") {
      payload.host = state.form.host;
      payload.port = state.form.port;
      payload.schema = state.form.schema;
    } else if (state.form.connection_type === "wasb") {
      payload.account_url = state.form.account_url;
      payload.host = state.form.account_url;
    } else if (state.form.connection_type === "gcp_service_account") {
      payload.connection_type = "gcp_service_account";
    }

    if (!payload.extra) {
      delete payload.extra;
    }
    if (!payload.port) {
      delete payload.port;
    }
    if (!payload.schema) {
      delete payload.schema;
    }
    if (!payload.login) {
      delete payload.login;
    }
    if (!payload.password) {
      delete payload.password;
    }
    return payload;
  }

  function buildMultipartPayload() {
    const formData = new FormData();
    if (state.mode === "create") {
      formData.append("connection_id", state.form.connection_id);
    }
    formData.append("connection_type", "gcp_service_account");
    formData.append("description", state.form.description || "");
    if (state.form.service_account_file_upload) {
      formData.append("service_account_file_upload", state.form.service_account_file_upload);
    }
    return formData;
  }

  function setSubmitting(isSubmitting) {
    state.submitting = isSubmitting;
    saveBtn.disabled = isSubmitting;
    saveBtn.textContent = isSubmitting ? "Saving..." : "Save Connection";
    refreshBtn.disabled = isSubmitting;
    createBtn.disabled = isSubmitting;
    resetBtn.disabled = isSubmitting;
    cancelBtn.disabled = isSubmitting;
    modalCloseBtn.disabled = isSubmitting;
  }

  async function saveConnection() {
    if (!canManageConnections) {
      setStatus("warn", "Viewer role is read-only for connections.");
      return;
    }
    const errors = validateForm();
    if (Object.keys(errors).length > 0) {
      setStatus("error", "Please fix form validation errors.");
      return;
    }

    setSubmitting(true);
    setStatus("", "");

    try {
      const payload = buildPayload();
      const isGcpConnection = state.form.connection_type === "gcp_service_account";
      let resp;
      if (state.mode === "create") {
        if (isGcpConnection) {
          resp = await fetchApi("/api/airflow/connections", {
            method: "POST",
            body: buildMultipartPayload(),
          });
        } else {
          resp = await fetchApi("/api/airflow/connections", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
        }
      } else {
        if (isGcpConnection) {
          resp = await fetchApi("/api/airflow/connections/" + encodeURIComponent(state.editingId), {
            method: "PUT",
            body: buildMultipartPayload(),
          });
        } else {
          resp = await fetchApi("/api/airflow/connections/" + encodeURIComponent(state.editingId), {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
        }
      }

      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        const msg =
          data && Array.isArray(data.errors) && data.errors.length > 0
            ? data.errors.join(" | ")
            : (data && data.error) || "Unable to save connection.";
        throw new Error(msg);
      }

      const successMessage =
        state.mode === "create" ? "Connection created successfully." : "Connection updated successfully.";
      setStatus("ok", successMessage);
      setEditorMode("hidden");

      const refreshed = await loadConnections(false);
      if (!refreshed) {
        setStatus(
          "warn",
          successMessage +
            " Connection was saved, but automatic list refresh failed. Click Refresh to retry."
        );
      }
    } catch (error) {
      setStatus("error", error && error.message ? error.message : "Unable to save connection.");
    } finally {
      setSubmitting(false);
    }
  }

  async function deleteConnection(connectionId) {
    if (!canManageConnections) {
      setStatus("warn", "Viewer role is read-only for connections.");
      return;
    }
    const safeId = String(connectionId || "").trim();
    if (!safeId) {
      return;
    }
    if (!window.confirm("Delete connection '" + safeId + "'?")) {
      return;
    }

    setStatus("", "");
    setSubmitting(true);
    try {
      const resp = await fetchApi("/api/airflow/connections/" + encodeURIComponent(safeId), {
        method: "DELETE",
      });
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Unable to delete connection.");
      }
      setStatus("ok", "Connection deleted: " + safeId);
      await loadConnections(false);
      if (state.mode === "edit" && state.editingId === safeId) {
        setEditorMode("hidden");
      }
    } catch (error) {
      setStatus("error", error && error.message ? error.message : "Unable to delete connection.");
    } finally {
      setSubmitting(false);
    }
  }

  tableBodyEl.addEventListener("click", function (event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const editBtn = target.closest(".conn-action-edit");
    if (editBtn) {
      if (!canManageConnections) {
        setStatus("warn", "Viewer role is read-only for connections.");
        return;
      }
      const id = String(editBtn.getAttribute("data-conn-id") || "");
      const item = state.items.find(function (entry) {
        return String(entry.connection_id || "") === id;
      });
      if (item) {
        setEditorMode("edit", item);
        setStatus("", "");
      }
      return;
    }

    const deleteBtn = target.closest(".conn-action-delete");
    if (deleteBtn) {
      if (!canManageConnections) {
        setStatus("warn", "Viewer role is read-only for connections.");
        return;
      }
      const id = String(deleteBtn.getAttribute("data-conn-id") || "");
      deleteConnection(id);
    }
  });

  searchEl.addEventListener("input", function () {
    state.searchTouched = true;
    state.search = searchEl.value;
    renderTable();
  });

  refreshBtn.addEventListener("click", function () {
    loadConnections(true);
  });

  createBtn.addEventListener("click", function () {
    if (!canManageConnections) {
      setStatus("warn", "Viewer role is read-only for connections.");
      return;
    }
    setEditorMode("create");
    setStatus("", "");
  });

  resetBtn.addEventListener("click", function () {
    setEditorMode(
      state.mode === "edit" ? "edit" : "create",
      state.mode === "edit"
        ? state.items.find(function (item) {
            return String(item.connection_id || "") === state.editingId;
          })
        : null
    );
    setStatus("", "");
  });

  cancelBtn.addEventListener("click", function () {
    setEditorMode("hidden");
    setStatus("", "");
  });

  modalCloseBtn.addEventListener("click", function () {
    setEditorMode("hidden");
    setStatus("", "");
  });

  editorOverlayEl.addEventListener("click", function (event) {
    if (event.target === editorOverlayEl) {
      setEditorMode("hidden");
      setStatus("", "");
    }
  });

  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !editorOverlayEl.hidden) {
      setEditorMode("hidden");
      setStatus("", "");
    }
  });

  saveBtn.addEventListener("click", function () {
    saveConnection();
  });

  fieldEls.connection_type.addEventListener("change", function () {
    updateTypeSections();
    clearFieldErrors();
  });

  // Force clean initial search state (avoids browser autofill side effects).
  state.searchTouched = false;
  state.search = "";
  searchEl.value = "";
  if (!canManageConnections) {
    createBtn.disabled = true;
    createBtn.textContent = "Read-only";
    if (actionsHeaderEl) {
      actionsHeaderEl.textContent = "Access";
    }
  }
  if (!canViewConnections) {
    refreshBtn.disabled = true;
    searchEl.disabled = true;
    createBtn.disabled = true;
    if (actionsHeaderEl) {
      actionsHeaderEl.textContent = "Access";
    }
  }
  setEditorMode("hidden");
  if (!canManageConnections) {
    editorTitleEl.textContent = "Connection Details";
  }
  window.addEventListener("pageshow", function () {
    state.searchTouched = false;
    state.search = "";
    searchEl.value = "";
    renderTable();
    setEditorMode("hidden");
  });
  if (!canViewConnections) {
    setStatus("warn", "No access: your role cannot view Airflow connections.");
  } else if (!canManageConnections) {
    setStatus("ok", "Viewer mode: you can view connections but cannot create, edit, or delete.");
  }
  loadConnections(true);
})();
