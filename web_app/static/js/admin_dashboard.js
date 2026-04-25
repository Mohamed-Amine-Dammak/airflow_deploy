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
      const filename = deleteBtn ? String(deleteBtn.getAttribute("data-filename") || "").trim() : "";
      rows.push({
        filename: filename || (nameEl ? String(nameEl.textContent || "").trim() : ""),
        updated_at: dateEl ? String(dateEl.textContent || "").trim() : "",
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
      li.innerHTML =
        "<span>" + (item.filename || "-") + "</span>" +
        '<span class="admin-file-meta">' +
        "<small>" + (item.updated_at || "-") + "</small>" +
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
    const btn = event.target.closest(".admin-file-delete");
    if (!btn) {
      return;
    }
    event.preventDefault();
    const kind = btn.getAttribute("data-file-kind");
    const filename = btn.getAttribute("data-filename");
    deleteAdminFile(kind, filename, btn);
  });

  loadUsers();
  loadRequests();
  initializeFilePagination();
})();
