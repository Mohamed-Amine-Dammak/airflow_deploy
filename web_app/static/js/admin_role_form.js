(function () {
  const cfg = window.ADMIN_ROLE_FORM || {
    mode: "create",
    role: {},
    role_catalog: [],
    permission_catalog: [],
    role_name_readonly: false,
  };

  const mode = String(cfg.mode || "create").toLowerCase() === "edit" ? "edit" : "create";
  const role = cfg.role || {};
  const roleCatalog = Array.isArray(cfg.role_catalog) ? cfg.role_catalog : [];
  const permissionCatalog = Array.isArray(cfg.permission_catalog) ? cfg.permission_catalog : [];
  const roleNameReadonly = Boolean(cfg.role_name_readonly);

  const draftKey = mode === "edit"
    ? "admin_role_form_draft_edit_" + String(role.name || "")
    : "admin_role_form_draft_create";

  function byId(id) {
    return document.getElementById(id);
  }

  const statusBox = byId("admin-role-form-status");
  const roleInput = byId("admin-role-name");
  const descriptionInput = byId("admin-role-description");
  const cloneSelect = byId("admin-role-clone");
  const errName = byId("admin-role-err-name");
  const saveBtn = byId("admin-role-save");
  const draftBtn = byId("admin-role-draft");
  const privilegeInput = byId("admin-role-privilege-input");
  const privilegeAddBtn = byId("admin-role-privilege-add");
  const privilegeError = byId("admin-role-privilege-error");
  const privilegeChips = byId("admin-role-privileges-chips");
  const permissionsWrap = byId("admin-role-permissions");
  const permissionsEmpty = byId("admin-role-permissions-empty");
  const searchInput = byId("admin-role-permission-search");
  const selectAllBtn = byId("admin-role-select-all");
  const clearAllBtn = byId("admin-role-clear-all");
  const selectedCountChip = byId("admin-role-permission-count");
  const predefinedWrap = byId("admin-role-predefined");

  const summaryName = byId("admin-role-summary-name");
  const summaryDescription = byId("admin-role-summary-description");
  const summaryPermissions = byId("admin-role-summary-permissions");
  const summaryPrivileges = byId("admin-role-summary-privileges");
  const summaryWarnings = byId("admin-role-summary-warnings");
  const summaryCategories = byId("admin-role-summary-categories");

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
    statusBox.className = "admin-inline-status role-builder-status " + (kind || "info");
    statusBox.textContent = text;
  }

  function normalizeRoleName(raw) {
    return String(raw || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "_")
      .replace(/^_+|_+$/g, "");
  }

  function roleNameValidationError(value) {
    const text = String(value || "").trim();
    if (!text) {
      return "Role name is required.";
    }
    if (!/^[A-Za-z0-9_-]+$/.test(text)) {
      return "Use only letters, numbers, underscore, or hyphen.";
    }
    return "";
  }

  function showRoleNameError(message) {
    if (!errName) {
      return;
    }
    const text = String(message || "").trim();
    if (!text) {
      errName.hidden = true;
      errName.textContent = "";
      return;
    }
    errName.hidden = false;
    errName.textContent = text;
  }

  function normalizePrivilege(value) {
    return String(value || "").trim().replace(/\s+/g, " ");
  }

  function dedupePrivileges(values) {
    const out = [];
    const seen = new Set();
    (Array.isArray(values) ? values : []).forEach(function (item) {
      const normalized = normalizePrivilege(item);
      if (!normalized) {
        return;
      }
      const key = normalized.toLowerCase();
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      out.push(normalized);
    });
    return out;
  }

  function categoryForPermission(permission) {
    const key = String(permission.key || "").trim().toLowerCase();
    if (key === "builder_view") {
      return "Builder";
    }
    if (key.startsWith("pipeline_")) {
      return "Pipelines";
    }
    if (key.startsWith("dag_") || key === "task_retry") {
      return "Execution";
    }
    if (key === "logs_view") {
      return "Monitoring";
    }
    if (key.startsWith("connections_")) {
      return "Connections";
    }
    if (key === "admin_dashboard" || key === "users_manage" || key === "roles_manage") {
      return "Administration";
    }
    return "Other";
  }

  const state = {
    roleName: "",
    description: "",
    cloneRoleName: "",
    privileges: [],
    selectedPermissions: new Set(),
    permissionSearch: "",
    collapsedCategories: {},
  };

  function splitBySearchMatch(permission, query) {
    const q = String(query || "").trim().toLowerCase();
    if (!q) {
      return true;
    }
    const hay = [
      String(permission.key || ""),
      String(permission.label || ""),
      String(permission.description || ""),
      String(categoryForPermission(permission) || ""),
    ].join(" ").toLowerCase();
    return hay.indexOf(q) !== -1;
  }

  function groupedPermissions() {
    const groups = {};
    permissionCatalog.forEach(function (permission) {
      if (!permission || !permission.key) {
        return;
      }
      const category = categoryForPermission(permission);
      if (!groups[category]) {
        groups[category] = [];
      }
      groups[category].push(permission);
    });

    const orderedCategories = ["Builder", "Pipelines", "Execution", "Monitoring", "Connections", "Administration", "Other"];
    const entries = [];
    orderedCategories.forEach(function (name) {
      if (groups[name] && groups[name].length) {
        entries.push([name, groups[name]]);
      }
    });
    return entries;
  }

  function selectedCount() {
    return state.selectedPermissions.size;
  }

  function addPrivilege(rawValue) {
    const value = normalizePrivilege(rawValue);
    if (!value) {
      return;
    }
    const exists = state.privileges.some(function (item) {
      return item.toLowerCase() === value.toLowerCase();
    });
    if (exists) {
      if (privilegeError) {
        privilegeError.hidden = false;
        privilegeError.textContent = "Duplicate privilege ignored.";
      }
      return;
    }
    if (privilegeError) {
      privilegeError.hidden = true;
      privilegeError.textContent = "";
    }
    state.privileges.push(value);
    renderPrivileges();
    renderSummary();
  }

  function removePrivilege(index) {
    if (index < 0 || index >= state.privileges.length) {
      return;
    }
    state.privileges.splice(index, 1);
    renderPrivileges();
    renderSummary();
  }

  function renderPrivileges() {
    if (!privilegeChips) {
      return;
    }
    privilegeChips.innerHTML = "";
    if (!state.privileges.length) {
      const empty = document.createElement("p");
      empty.className = "admin-empty";
      empty.textContent = "No custom privileges added yet.";
      privilegeChips.appendChild(empty);
      return;
    }

    state.privileges.forEach(function (privilege, index) {
      const chip = document.createElement("span");
      chip.className = "role-privilege-chip";
      chip.tabIndex = 0;
      chip.setAttribute("aria-label", "Privilege " + privilege);

      const text = document.createElement("span");
      text.textContent = privilege;

      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.className = "role-privilege-chip-remove";
      removeBtn.setAttribute("aria-label", "Remove privilege " + privilege);
      removeBtn.textContent = "x";
      removeBtn.addEventListener("click", function () {
        removePrivilege(index);
      });

      chip.appendChild(text);
      chip.appendChild(removeBtn);
      privilegeChips.appendChild(chip);
    });
  }

  function renderCloneOptions() {
    if (!cloneSelect) {
      return;
    }
    const current = cloneSelect.value;
    cloneSelect.innerHTML = '<option value="">Start from scratch</option>';

    roleCatalog.forEach(function (item) {
      if (!item || !item.name) {
        return;
      }
      const opt = document.createElement("option");
      opt.value = String(item.name);
      opt.textContent = String(item.name) + (item.is_predefined ? " (predefined)" : " (custom)");
      cloneSelect.appendChild(opt);
    });

    cloneSelect.value = current || "";
  }

  function applyBaselineRole(roleName) {
    const name = normalizeRoleName(roleName);
    if (!name) {
      return;
    }
    const baseline = roleCatalog.find(function (item) {
      return normalizeRoleName(item.name) === name;
    });
    if (!baseline) {
      return;
    }

    if (descriptionInput && !String(descriptionInput.value || "").trim()) {
      descriptionInput.value = String(baseline.description || "");
      state.description = String(descriptionInput.value || "").trim();
    }

    const privileges = dedupePrivileges(Array.isArray(baseline.privileges) ? baseline.privileges : []);
    if (privileges.length) {
      state.privileges = privileges;
      renderPrivileges();
    }

    const permissions = Array.isArray(baseline.permissions) ? baseline.permissions : [];
    state.selectedPermissions = new Set(
      permissions
        .map(function (item) { return String(item || "").trim(); })
        .filter(Boolean)
    );

    renderPermissionMatrix();
    renderSummary();
    setStatus("Baseline role applied: " + baseline.name, "success");
  }

  function permissionByKey(key) {
    const safe = String(key || "").trim();
    return permissionCatalog.find(function (item) {
      return String(item.key || "").trim() === safe;
    }) || null;
  }

  function updateSelectedCount() {
    if (!selectedCountChip) {
      return;
    }
    selectedCountChip.textContent = selectedCount() + " selected";
  }

  function selectedByCategory() {
    const counts = {};
    state.selectedPermissions.forEach(function (key) {
      const permission = permissionByKey(key);
      if (!permission) {
        return;
      }
      const category = categoryForPermission(permission);
      counts[category] = (counts[category] || 0) + 1;
    });
    return counts;
  }

  function renderPermissionMatrix() {
    if (!permissionsWrap) {
      return;
    }

    const groups = groupedPermissions();
    permissionsWrap.innerHTML = "";

    let anyVisible = false;

    groups.forEach(function (entry) {
      const category = entry[0];
      const permissions = entry[1].filter(function (permission) {
        return splitBySearchMatch(permission, state.permissionSearch);
      });

      if (!permissions.length) {
        return;
      }
      anyVisible = true;

      const detail = document.createElement("details");
      detail.className = "role-permission-group";
      detail.open = state.collapsedCategories[category] !== true;
      detail.addEventListener("toggle", function () {
        state.collapsedCategories[category] = !detail.open;
      });

      const summary = document.createElement("summary");
      summary.className = "role-permission-group-summary";

      const title = document.createElement("span");
      title.className = "role-permission-group-title";
      title.textContent = category;

      const selectedInCategory = permissions.filter(function (permission) {
        return state.selectedPermissions.has(String(permission.key || ""));
      }).length;

      const meta = document.createElement("span");
      meta.className = "role-permission-group-meta";
      meta.textContent = selectedInCategory + " / " + permissions.length + " selected";

      summary.appendChild(title);
      summary.appendChild(meta);
      detail.appendChild(summary);

      const body = document.createElement("div");
      body.className = "role-permission-group-body";

      const controls = document.createElement("div");
      controls.className = "role-permission-group-controls";

      const selectBtn = document.createElement("button");
      selectBtn.type = "button";
      selectBtn.className = "btn btn-secondary";
      selectBtn.textContent = "Select all in " + category;
      selectBtn.addEventListener("click", function () {
        permissions.forEach(function (permission) {
          state.selectedPermissions.add(String(permission.key || ""));
        });
        renderPermissionMatrix();
        renderSummary();
      });

      const clearBtn = document.createElement("button");
      clearBtn.type = "button";
      clearBtn.className = "btn btn-secondary";
      clearBtn.textContent = "Clear";
      clearBtn.addEventListener("click", function () {
        permissions.forEach(function (permission) {
          state.selectedPermissions.delete(String(permission.key || ""));
        });
        renderPermissionMatrix();
        renderSummary();
      });

      controls.appendChild(selectBtn);
      controls.appendChild(clearBtn);
      body.appendChild(controls);

      const list = document.createElement("div");
      list.className = "role-permission-list";

      permissions.forEach(function (permission) {
        const row = document.createElement("label");
        row.className = "role-permission-row";
        row.setAttribute("tabindex", "0");

        const input = document.createElement("input");
        input.type = "checkbox";
        input.value = String(permission.key || "");
        input.checked = state.selectedPermissions.has(input.value);
        input.setAttribute("aria-label", String(permission.label || permission.key || "permission"));

        input.addEventListener("change", function () {
          if (input.checked) {
            state.selectedPermissions.add(input.value);
          } else {
            state.selectedPermissions.delete(input.value);
          }
          updateSelectedCount();
          renderSummary();
          renderPermissionMatrix();
        });

        const copy = document.createElement("div");
        copy.className = "role-permission-copy";

        const heading = document.createElement("strong");
        heading.textContent = String(permission.label || permission.key || "");

        const description = document.createElement("small");
        description.textContent = String(permission.description || permission.key || "");

        copy.appendChild(heading);
        copy.appendChild(description);

        row.appendChild(input);
        row.appendChild(copy);
        list.appendChild(row);
      });

      body.appendChild(list);
      detail.appendChild(body);
      permissionsWrap.appendChild(detail);
    });

    if (permissionsEmpty) {
      permissionsEmpty.style.display = anyVisible ? "none" : "";
    }

    updateSelectedCount();
  }

  function renderReferenceRoles() {
    if (!predefinedWrap) {
      return;
    }
    predefinedWrap.innerHTML = "";

    const predefined = roleCatalog.filter(function (item) {
      return Boolean(item && item.is_predefined);
    });

    if (!predefined.length) {
      const empty = document.createElement("p");
      empty.className = "admin-empty";
      empty.textContent = "No predefined roles found.";
      predefinedWrap.appendChild(empty);
      return;
    }

    predefined.forEach(function (item) {
      const detail = document.createElement("details");
      detail.className = "role-reference-item";

      const summary = document.createElement("summary");
      summary.className = "role-reference-summary";
      summary.textContent = String(item.name || "role");

      const body = document.createElement("div");
      body.className = "role-reference-body";

      const desc = document.createElement("p");
      desc.className = "admin-help";
      desc.textContent = String(item.description || "No description.");

      const permissionLabels = Array.isArray(item.permission_labels)
        ? item.permission_labels
        : Array.isArray(item.permissions)
        ? item.permissions
        : [];

      const keyPerms = document.createElement("ul");
      keyPerms.className = "role-reference-perms";
      (permissionLabels.length ? permissionLabels.slice(0, 6) : ["No key permissions listed."]).forEach(function (entry) {
        const li = document.createElement("li");
        li.textContent = String(entry || "");
        keyPerms.appendChild(li);
      });

      const baselineBtn = document.createElement("button");
      baselineBtn.type = "button";
      baselineBtn.className = "btn btn-secondary";
      baselineBtn.textContent = "Use as baseline";
      baselineBtn.addEventListener("click", function () {
        applyBaselineRole(item.name);
      });

      body.appendChild(desc);
      body.appendChild(keyPerms);
      body.appendChild(baselineBtn);
      detail.appendChild(summary);
      detail.appendChild(body);
      predefinedWrap.appendChild(detail);
    });
  }

  function renderSummary() {
    const roleName = String(state.roleName || "").trim();
    const description = String(state.description || "").trim();

    if (summaryName) {
      summaryName.textContent = roleName || "Untitled role";
    }
    if (summaryDescription) {
      summaryDescription.textContent = description || "No description yet";
    }
    if (summaryPermissions) {
      summaryPermissions.textContent = String(selectedCount());
    }
    if (summaryPrivileges) {
      summaryPrivileges.textContent = String(state.privileges.length);
    }

    if (summaryWarnings) {
      summaryWarnings.innerHTML = "";
      const warnings = [];
      if (state.selectedPermissions.has("users_manage")) {
        warnings.push("This role can manage users.");
      }
      if (state.selectedPermissions.has("roles_manage")) {
        warnings.push("This role can manage roles.");
      }
      if (state.selectedPermissions.has("admin_dashboard")) {
        warnings.push("This role includes admin-level capabilities.");
      }
      warnings.forEach(function (warning) {
        const row = document.createElement("p");
        row.className = "role-summary-warning";
        row.textContent = warning;
        summaryWarnings.appendChild(row);
      });
    }

    if (summaryCategories) {
      summaryCategories.innerHTML = "";
      const counts = selectedByCategory();
      const categories = Object.keys(counts).sort();
      if (!categories.length) {
        const empty = document.createElement("p");
        empty.className = "admin-empty";
        empty.textContent = "No permissions selected yet.";
        summaryCategories.appendChild(empty);
      } else {
        categories.forEach(function (category) {
          const row = document.createElement("div");
          row.className = "role-summary-category";
          const name = document.createElement("span");
          name.textContent = category;
          const value = document.createElement("strong");
          value.textContent = String(counts[category]);
          row.appendChild(name);
          row.appendChild(value);
          summaryCategories.appendChild(row);
        });
      }
    }
  }

  function collectPayload() {
    return {
      role_name: String(state.roleName || "").trim(),
      description: String(state.description || "").trim(),
      privileges: dedupePrivileges(state.privileges),
      permissions: Array.from(state.selectedPermissions),
    };
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

  function saveDraft() {
    try {
      const payload = collectPayload();
      window.localStorage.setItem(draftKey, JSON.stringify(payload));
      setStatus("Draft saved locally.", "success");
    } catch (error) {
      setStatus("Unable to save draft in this browser.", "error");
    }
  }

  function restoreDraftIfAny() {
    if (mode !== "create") {
      return;
    }
    const raw = window.localStorage.getItem(draftKey);
    if (!raw) {
      return;
    }
    let parsed = null;
    try {
      parsed = JSON.parse(raw);
    } catch (_err) {
      parsed = null;
    }
    if (!parsed || typeof parsed !== "object") {
      return;
    }

    const shouldRestore = window.confirm("A saved draft was found for role creation. Restore it?");
    if (!shouldRestore) {
      return;
    }

    state.roleName = String(parsed.role_name || "").trim();
    state.description = String(parsed.description || "").trim();
    state.privileges = dedupePrivileges(parsed.privileges || []);
    state.selectedPermissions = new Set(
      (Array.isArray(parsed.permissions) ? parsed.permissions : [])
        .map(function (item) { return String(item || "").trim(); })
        .filter(Boolean)
    );

    if (roleInput) {
      roleInput.value = state.roleName;
    }
    if (descriptionInput) {
      descriptionInput.value = state.description;
    }

    renderPrivileges();
    renderPermissionMatrix();
    renderSummary();
    setStatus("Draft restored.", "success");
  }

  function validateBeforeSave() {
    const error = roleNameValidationError(state.roleName);
    showRoleNameError(error);
    return !error;
  }

  async function saveRole() {
    if (!validateBeforeSave()) {
      setStatus("Please fix form errors before saving.", "error");
      return;
    }

    const payload = collectPayload();

    try {
      if (saveBtn) {
        saveBtn.disabled = true;
      }
      setStatus("Saving role...", "info");
      const endpoint = mode === "edit"
        ? "/api/admin/roles/" + encodeURIComponent(String(role.name || ""))
        : "/api/admin/roles";
      const method = mode === "edit" ? "PUT" : "POST";
      await fetchJson(endpoint, {
        method: method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      try {
        window.localStorage.removeItem(draftKey);
      } catch (_err) {
      }

      const msg = mode === "edit" ? "Custom role updated successfully." : "Custom role created successfully.";
      window.location.href = "/admin/roles?msg=" + encodeURIComponent(msg);
    } catch (error) {
      const message = error && error.message ? error.message : "Unable to save role.";
      showRoleNameError(message);
      setStatus(message, "error");
    } finally {
      if (saveBtn) {
        saveBtn.disabled = false;
      }
    }
  }

  function bindEvents() {
    if (roleInput) {
      roleInput.addEventListener("input", function () {
        state.roleName = normalizeRoleName(roleInput.value);
        roleInput.value = state.roleName;
        showRoleNameError(roleNameValidationError(state.roleName));
        renderSummary();
      });
    }

    if (descriptionInput) {
      descriptionInput.addEventListener("input", function () {
        state.description = String(descriptionInput.value || "").trim();
        renderSummary();
      });
    }

    if (cloneSelect) {
      cloneSelect.addEventListener("change", function () {
        const selected = String(cloneSelect.value || "");
        state.cloneRoleName = selected;
        if (selected) {
          applyBaselineRole(selected);
        }
      });
    }

    if (privilegeInput) {
      privilegeInput.addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          event.preventDefault();
          addPrivilege(privilegeInput.value);
          privilegeInput.value = "";
        }
      });
    }

    if (privilegeAddBtn && privilegeInput) {
      privilegeAddBtn.addEventListener("click", function () {
        addPrivilege(privilegeInput.value);
        privilegeInput.value = "";
        privilegeInput.focus();
      });
    }

    if (searchInput) {
      searchInput.addEventListener("input", function () {
        state.permissionSearch = String(searchInput.value || "").trim();
        renderPermissionMatrix();
      });
    }

    if (selectAllBtn) {
      selectAllBtn.addEventListener("click", function () {
        permissionCatalog.forEach(function (permission) {
          if (!permission || !permission.key) {
            return;
          }
          if (!splitBySearchMatch(permission, state.permissionSearch)) {
            return;
          }
          state.selectedPermissions.add(String(permission.key));
        });
        renderPermissionMatrix();
        renderSummary();
      });
    }

    if (clearAllBtn) {
      clearAllBtn.addEventListener("click", function () {
        if (!state.permissionSearch) {
          state.selectedPermissions = new Set();
        } else {
          permissionCatalog.forEach(function (permission) {
            if (!permission || !permission.key) {
              return;
            }
            if (splitBySearchMatch(permission, state.permissionSearch)) {
              state.selectedPermissions.delete(String(permission.key));
            }
          });
        }
        renderPermissionMatrix();
        renderSummary();
      });
    }

    if (draftBtn) {
      draftBtn.addEventListener("click", saveDraft);
    }

    if (saveBtn) {
      saveBtn.addEventListener("click", saveRole);
    }
  }

  function hydrateInitialState() {
    state.roleName = mode === "edit" ? String(role.name || "") : "";
    state.description = mode === "edit" ? String(role.description || "") : "";
    state.privileges = dedupePrivileges(mode === "edit" && Array.isArray(role.privileges) ? role.privileges : []);

    state.selectedPermissions = new Set(
      (mode === "edit" && Array.isArray(role.permissions) ? role.permissions : [])
        .map(function (item) { return String(item || "").trim(); })
        .filter(Boolean)
    );

    if (roleInput) {
      roleInput.value = state.roleName;
      roleInput.readOnly = roleNameReadonly;
    }
    if (descriptionInput) {
      descriptionInput.value = state.description;
    }

    renderCloneOptions();
    renderPrivileges();
    renderPermissionMatrix();
    renderReferenceRoles();
    renderSummary();

    if (mode === "create") {
      restoreDraftIfAny();
    }

    setStatus(mode === "edit" ? "Update role details and permissions." : "Create a custom role and assign it to users.", "info");
  }

  bindEvents();
  hydrateInitialState();
})();
