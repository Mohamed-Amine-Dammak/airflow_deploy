(function () {
  const cfg = window.ADMIN_USER_FORM || { mode: "create", user: {}, all_roles: [], role_catalog: [] };
  const mode = String(cfg.mode || "create").toLowerCase() === "edit" ? "edit" : "create";
  const user = cfg.user || {};
  const allRoles = Array.isArray(cfg.all_roles) ? cfg.all_roles : [];
  const roleCatalog = Array.isArray(cfg.role_catalog) ? cfg.role_catalog : [];
  const draftKey = mode === "edit" ? "admin_user_form_draft_edit_" + String(user.id || "") : "admin_user_form_draft_create";

  function byId(id) {
    return document.getElementById(id);
  }

  const statusBox = byId("admin-user-form-status");
  const usernameInput = byId("admin-user-username");
  const displayNameInput = byId("admin-user-display-name");
  const emailInput = byId("admin-user-email");
  const passwordInput = byId("admin-user-password");
  const confirmPasswordInput = byId("admin-user-confirm-password");
  const togglePasswordBtn = byId("admin-user-toggle-password");
  const toggleConfirmPasswordBtn = byId("admin-user-toggle-confirm-password");
  const setLaterInput = byId("admin-user-set-later");
  const saveBtn = byId("admin-user-save");
  const draftBtn = byId("admin-user-draft");

  const roleSearchInput = byId("admin-user-role-search");
  const roleSortSelect = byId("admin-user-role-sort");
  const rolesList = byId("admin-user-roles-list");
  const rolesEmpty = byId("admin-user-roles-empty");
  const selectedCountChip = byId("admin-user-selected-count");
  const selectedChipsWrap = byId("admin-user-selected-role-chips");

  const summaryUsername = byId("admin-user-summary-username");
  const summaryEmail = byId("admin-user-summary-email");
  const summaryCount = byId("admin-user-summary-count");
  const summaryRoleChips = byId("admin-user-summary-role-chips");
  const summaryWarnings = byId("admin-user-summary-warnings");
  const summaryChecklist = byId("admin-user-checklist");

  const inspectEmpty = byId("admin-user-inspect-empty");
  const inspector = byId("admin-user-role-inspector");
  const inspectName = byId("admin-user-inspect-name");
  const inspectRisk = byId("admin-user-inspect-risk");
  const inspectDescription = byId("admin-user-inspect-description");
  const inspectPrivileges = byId("admin-user-inspect-privileges");
  const inspectPermissions = byId("admin-user-inspect-permissions");

  const strengthFill = byId("admin-user-password-strength-fill");
  const strengthText = byId("admin-user-password-strength-text");
  const ruleLength = byId("admin-user-rule-length");
  const ruleUpperLower = byId("admin-user-rule-upper-lower");
  const ruleNumberSymbol = byId("admin-user-rule-number-symbol");

  const errUsername = byId("admin-user-err-username");
  const errEmail = byId("admin-user-err-email");
  const errPassword = byId("admin-user-err-password");
  const errConfirmPassword = byId("admin-user-err-confirm-password");
  const errRoles = byId("admin-user-err-roles");

  const roleMap = {};
  roleCatalog.forEach(function (item) {
    if (item && item.name) {
      roleMap[String(item.name)] = item;
    }
  });

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
    statusBox.className = "admin-inline-status user-builder-status " + (kind || "info");
    statusBox.textContent = text;
  }

  function setFieldError(node, message) {
    if (!node) {
      return;
    }
    const text = String(message || "").trim();
    node.hidden = !text;
    node.textContent = text;
  }

  function normalizeUsername(value) {
    return String(value || "").trim().toLowerCase();
  }

  function usernameError(value) {
    const text = normalizeUsername(value);
    if (!text) {
      return "Username is required.";
    }
    if (!/^[a-zA-Z0-9_.-]{3,64}$/.test(text)) {
      return "Username must be 3-64 chars using letters, digits, dot, underscore, or dash.";
    }
    return "";
  }

  function emailError(value) {
    const text = String(value || "").trim();
    if (!text) {
      return "";
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text)) {
      return "Enter a valid email address.";
    }
    return "";
  }

  function passwordChecks(password) {
    const text = String(password || "");
    const hasLength = text.length >= 6;
    const hasUpper = /[A-Z]/.test(text);
    const hasLower = /[a-z]/.test(text);
    const hasUpperLower = hasUpper && hasLower;
    const hasNumberOrSymbol = /[0-9]|[^A-Za-z0-9]/.test(text);

    let score = 0;
    if (hasLength) score += 1;
    if (hasUpperLower) score += 1;
    if (hasNumberOrSymbol) score += 1;
    if (text.length >= 10) score += 1;

    return {
      hasLength: hasLength,
      hasUpperLower: hasUpperLower,
      hasNumberOrSymbol: hasNumberOrSymbol,
      score: score,
    };
  }

  function strengthLabel(score) {
    if (score <= 1) {
      return "Weak";
    }
    if (score === 2) {
      return "Fair";
    }
    if (score === 3) {
      return "Good";
    }
    return "Strong";
  }

  function riskLevelForRole(roleItem) {
    const details = roleItem || {};
    const permissions = Array.isArray(details.permissions) ? details.permissions : [];
    const permissionSet = new Set(permissions.map(function (item) { return String(item || ""); }));

    if (String(details.name || "").toLowerCase() === "admin") {
      return "High";
    }
    if (permissionSet.has("admin_dashboard") || permissionSet.has("users_manage") || permissionSet.has("roles_manage")) {
      return "High";
    }
    if (permissionSet.has("dag_run") || permissionSet.has("dag_generate") || permissionSet.has("dag_retry") || permissionSet.has("task_retry")) {
      return "Medium";
    }
    return "Low";
  }

  const state = {
    username: "",
    displayName: "",
    email: "",
    password: "",
    confirmPassword: "",
    setPasswordLater: false,
    roleSearch: "",
    roleSort: "name",
    selectedRoles: new Set(),
    inspectedRole: "",
  };

  function roleItemsWithMeta() {
    return allRoles
      .map(function (roleName) {
        const details = roleMap[roleName] || { name: roleName, description: "" };
        const privileges = Array.isArray(details.privileges) ? details.privileges : [];
        const permissions = Array.isArray(details.permissions) ? details.permissions : [];
        return {
          name: String(roleName),
          description: String(details.description || ""),
          privileges: privileges,
          permissions: permissions,
          permissionLabels: Array.isArray(details.permission_labels) ? details.permission_labels : permissions,
          risk: riskLevelForRole(details),
        };
      })
      .filter(function (item) {
        const q = state.roleSearch.trim().toLowerCase();
        if (!q) {
          return true;
        }
        const hay = [item.name, item.description, item.risk].join(" ").toLowerCase();
        return hay.indexOf(q) !== -1;
      })
      .sort(function (a, b) {
        if (state.roleSort === "selected") {
          const aSel = state.selectedRoles.has(a.name) ? 1 : 0;
          const bSel = state.selectedRoles.has(b.name) ? 1 : 0;
          if (aSel !== bSel) {
            return bSel - aSel;
          }
        }
        if (state.roleSort === "risk") {
          const order = { High: 0, Medium: 1, Low: 2 };
          const cmp = (order[a.risk] || 3) - (order[b.risk] || 3);
          if (cmp !== 0) {
            return cmp;
          }
        }
        return a.name.localeCompare(b.name);
      });
  }

  function renderSelectedRoleChips(target) {
    if (!target) {
      return;
    }
    target.innerHTML = "";
    const selected = Array.from(state.selectedRoles).sort();
    if (!selected.length) {
      return;
    }
    selected.forEach(function (roleName) {
      const chip = document.createElement("span");
      chip.className = "user-role-chip";
      chip.textContent = roleName;

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "user-role-chip-remove";
      remove.textContent = "x";
      remove.setAttribute("aria-label", "Remove role " + roleName);
      remove.addEventListener("click", function () {
        state.selectedRoles.delete(roleName);
        renderRoles();
        renderSummary();
        renderInspector();
      });

      chip.appendChild(remove);
      target.appendChild(chip);
    });
  }

  function renderRoles() {
    if (!rolesList) {
      return;
    }

    const items = roleItemsWithMeta();
    rolesList.innerHTML = "";

    if (rolesEmpty) {
      rolesEmpty.style.display = items.length ? "none" : "";
    }

    items.forEach(function (item) {
      const row = document.createElement("label");
      row.className = "user-role-row" + (state.selectedRoles.has(item.name) ? " is-selected" : "");
      row.tabIndex = 0;

      const left = document.createElement("div");
      left.className = "user-role-left";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = item.name;
      checkbox.checked = state.selectedRoles.has(item.name);
      checkbox.setAttribute("aria-label", "Assign role " + item.name);

      const copy = document.createElement("div");
      copy.className = "user-role-copy";

      const nameLine = document.createElement("div");
      nameLine.className = "user-role-name-line";
      const title = document.createElement("strong");
      title.textContent = item.name;
      const risk = document.createElement("span");
      risk.className = "user-risk-pill risk-" + item.risk.toLowerCase();
      risk.textContent = item.risk;
      nameLine.appendChild(title);
      nameLine.appendChild(risk);

      const desc = document.createElement("small");
      desc.textContent = item.description || "No description.";

      const meta = document.createElement("small");
      meta.className = "user-role-meta";
      meta.textContent = item.privileges.length + " privileges | " + item.permissionLabels.length + " permissions";

      copy.appendChild(nameLine);
      copy.appendChild(desc);
      copy.appendChild(meta);
      left.appendChild(checkbox);
      left.appendChild(copy);

      const inspectBtn = document.createElement("button");
      inspectBtn.type = "button";
      inspectBtn.className = "btn btn-secondary";
      inspectBtn.textContent = "Inspect";
      inspectBtn.addEventListener("click", function (event) {
        event.preventDefault();
        state.inspectedRole = item.name;
        renderInspector();
      });

      checkbox.addEventListener("change", function () {
        if (checkbox.checked) {
          state.selectedRoles.add(item.name);
        } else {
          state.selectedRoles.delete(item.name);
        }
        setFieldError(errRoles, "");
        renderRoles();
        renderSummary();
      });

      row.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          checkbox.click();
        }
      });

      row.appendChild(left);
      row.appendChild(inspectBtn);
      rolesList.appendChild(row);
    });

    if (selectedCountChip) {
      selectedCountChip.textContent = state.selectedRoles.size + " selected";
    }

    renderSelectedRoleChips(selectedChipsWrap);
  }

  function renderInspector() {
    const roleName = state.inspectedRole || Array.from(state.selectedRoles)[0] || "";
    if (!roleName) {
      if (inspector) {
        inspector.style.display = "none";
      }
      if (inspectEmpty) {
        inspectEmpty.style.display = "";
      }
      return;
    }

    const details = roleMap[roleName] || { name: roleName, description: "", privileges: [], permissions: [] };
    const privileges = Array.isArray(details.privileges) ? details.privileges : [];
    const permissionLabels = Array.isArray(details.permission_labels)
      ? details.permission_labels
      : Array.isArray(details.permissions)
      ? details.permissions
      : [];
    const risk = riskLevelForRole(details);

    if (inspectEmpty) {
      inspectEmpty.style.display = "none";
    }
    if (inspector) {
      inspector.style.display = "";
    }
    if (inspectName) {
      inspectName.textContent = roleName;
    }
    if (inspectRisk) {
      inspectRisk.className = "user-risk-pill risk-" + String(risk).toLowerCase();
      inspectRisk.textContent = risk;
    }
    if (inspectDescription) {
      inspectDescription.textContent = String(details.description || "No description.");
    }

    function fillList(node, values, fallback) {
      if (!node) {
        return;
      }
      node.innerHTML = "";
      const items = values && values.length ? values : [fallback];
      items.forEach(function (item) {
        const li = document.createElement("li");
        li.textContent = String(item);
        node.appendChild(li);
      });
    }

    fillList(inspectPrivileges, privileges, "No privileges defined.");
    fillList(inspectPermissions, permissionLabels, "No enforced access defined.");
  }

  function renderPasswordStatus() {
    const checks = passwordChecks(state.password);

    function setRuleState(node, ok) {
      if (!node) {
        return;
      }
      node.classList.toggle("is-valid", !!ok);
      node.classList.toggle("is-invalid", !ok && state.password.length > 0);
    }

    setRuleState(ruleLength, checks.hasLength);
    setRuleState(ruleUpperLower, checks.hasUpperLower);
    setRuleState(ruleNumberSymbol, checks.hasNumberOrSymbol);

    if (strengthFill) {
      const pct = state.password ? Math.max(10, Math.min(100, (checks.score / 4) * 100)) : 0;
      strengthFill.style.width = pct + "%";
      strengthFill.className = "strength-" + strengthLabel(checks.score).toLowerCase();
    }
    if (strengthText) {
      strengthText.textContent = state.password ? "Strength: " + strengthLabel(checks.score) : "Strength: Not set";
    }
  }

  function renderSummary() {
    if (summaryUsername) {
      summaryUsername.textContent = state.username || "Not set";
    }
    if (summaryEmail) {
      summaryEmail.textContent = state.email || "Not set";
    }
    if (summaryCount) {
      summaryCount.textContent = String(state.selectedRoles.size);
    }

    renderSelectedRoleChips(summaryRoleChips);

    if (summaryWarnings) {
      summaryWarnings.innerHTML = "";
      const selected = Array.from(state.selectedRoles).map(function (roleName) {
        return roleMap[roleName] || { name: roleName, permissions: [] };
      });

      const permissionSet = new Set();
      selected.forEach(function (roleItem) {
        const perms = Array.isArray(roleItem.permissions) ? roleItem.permissions : [];
        perms.forEach(function (perm) {
          permissionSet.add(String(perm || ""));
        });
      });

      const warnings = [];
      if (selected.some(function (item) { return String(item.name || "").toLowerCase() === "admin"; })) {
        warnings.push("Includes admin access.");
      }
      if (permissionSet.has("users_manage") || permissionSet.has("roles_manage")) {
        warnings.push("Can manage users and roles.");
      }
      if (permissionSet.has("dag_run") || permissionSet.has("dag_generate") || permissionSet.has("dag_retry")) {
        warnings.push("Has pipeline execution permissions.");
      }

      warnings.forEach(function (message) {
        const row = document.createElement("p");
        row.className = "user-summary-warning";
        row.textContent = message;
        summaryWarnings.appendChild(row);
      });
    }

    if (summaryChecklist) {
      summaryChecklist.innerHTML = "";
      const usernameOk = !usernameError(state.username);
      const rolesOk = state.selectedRoles.size > 0;
      const passwordRequired = mode === "create" && !state.setPasswordLater;
      const passwordOk = !passwordRequired || passwordChecks(state.password).hasLength;
      const confirmOk = !passwordRequired || state.password === state.confirmPassword;

      const items = [
        { label: "Username is valid", ok: usernameOk },
        { label: "At least one role selected", ok: rolesOk },
        { label: passwordRequired ? "Password meets requirements" : "Password policy satisfied", ok: passwordRequired ? passwordOk : true },
        { label: passwordRequired ? "Password confirmation matches" : "Confirmation not required", ok: passwordRequired ? confirmOk : true },
      ];

      items.forEach(function (item) {
        const row = document.createElement("div");
        row.className = "user-checklist-item " + (item.ok ? "is-ok" : "is-pending");
        row.textContent = (item.ok ? "OK " : "TODO ") + item.label;
        summaryChecklist.appendChild(row);
      });
    }
  }

  function generateTemporaryPassword() {
    const token = Math.random().toString(36).slice(2, 8);
    return "Temp#" + token + "A1";
  }

  function collectPayload() {
    const payload = {
      username: normalizeUsername(state.username),
      roles: Array.from(state.selectedRoles),
    };

    if (mode === "create") {
      payload.password = state.setPasswordLater ? generateTemporaryPassword() : state.password;
    } else if (state.password) {
      payload.password = state.password;
    }

    return payload;
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

  function validateForm() {
    setFieldError(errUsername, usernameError(state.username));
    setFieldError(errEmail, emailError(state.email));
    setFieldError(errRoles, state.selectedRoles.size ? "" : "Select at least one role.");

    const passwordReq = mode === "create" && !state.setPasswordLater;
    const checks = passwordChecks(state.password);
    let passError = "";
    if (passwordReq && !state.password) {
      passError = "Password is required.";
    } else if (state.password && !checks.hasLength) {
      passError = "Password must be at least 6 characters.";
    }
    setFieldError(errPassword, passError);

    let confirmError = "";
    if ((passwordReq || state.password) && state.password !== state.confirmPassword) {
      confirmError = "Password confirmation does not match.";
    }
    setFieldError(errConfirmPassword, confirmError);

    return !(usernameError(state.username) || emailError(state.email) || !state.selectedRoles.size || passError || confirmError);
  }

  function saveDraft() {
    try {
      const draft = {
        username: state.username,
        display_name: state.displayName,
        email: state.email,
        set_password_later: state.setPasswordLater,
        selected_roles: Array.from(state.selectedRoles),
        inspected_role: state.inspectedRole,
      };
      window.localStorage.setItem(draftKey, JSON.stringify(draft));
      setStatus("Draft saved locally.", "success");
    } catch (_err) {
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

    const ok = window.confirm("A saved draft was found. Restore it?");
    if (!ok) {
      return;
    }

    state.username = normalizeUsername(parsed.username || "");
    state.displayName = String(parsed.display_name || "").trim();
    state.email = String(parsed.email || "").trim();
    state.setPasswordLater = Boolean(parsed.set_password_later);
    state.selectedRoles = new Set(
      (Array.isArray(parsed.selected_roles) ? parsed.selected_roles : [])
        .map(function (item) { return String(item || "").trim(); })
        .filter(Boolean)
    );
    state.inspectedRole = String(parsed.inspected_role || "");

    if (usernameInput) usernameInput.value = state.username;
    if (displayNameInput) displayNameInput.value = state.displayName;
    if (emailInput) emailInput.value = state.email;
    if (setLaterInput) setLaterInput.checked = state.setPasswordLater;

    renderSecurityMode();
    renderRoles();
    renderInspector();
    renderSummary();
    setStatus("Draft restored.", "success");
  }

  function renderSecurityMode() {
    const disabled = mode === "create" && state.setPasswordLater;
    if (passwordInput) {
      passwordInput.disabled = disabled;
      if (disabled) {
        passwordInput.value = "";
        state.password = "";
      }
    }
    if (confirmPasswordInput) {
      confirmPasswordInput.disabled = disabled;
      if (disabled) {
        confirmPasswordInput.value = "";
        state.confirmPassword = "";
      }
    }
    renderPasswordStatus();
    renderSummary();
  }

  async function saveUser() {
    if (!validateForm()) {
      setStatus("Please fix validation issues before saving.", "error");
      return;
    }

    try {
      if (saveBtn) {
        saveBtn.disabled = true;
      }
      setStatus("Saving user...", "info");
      const endpoint = mode === "edit"
        ? "/api/admin/users/" + encodeURIComponent(String(user.id || ""))
        : "/api/admin/users";
      const method = mode === "edit" ? "PUT" : "POST";

      await fetchJson(endpoint, {
        method: method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(collectPayload()),
      });

      try {
        window.localStorage.removeItem(draftKey);
      } catch (_err) {
      }

      const msg = mode === "edit" ? "User updated successfully." : "User created successfully.";
      window.location.href = "/admin-dashboard?msg=" + encodeURIComponent(msg);
    } catch (error) {
      setStatus(error && error.message ? error.message : "Unable to save user.", "error");
    } finally {
      if (saveBtn) {
        saveBtn.disabled = false;
      }
    }
  }

  function bindEvents() {
    if (usernameInput) {
      usernameInput.addEventListener("input", function () {
        state.username = normalizeUsername(usernameInput.value);
        usernameInput.value = state.username;
        setFieldError(errUsername, usernameError(state.username));
        renderSummary();
      });
    }

    if (displayNameInput) {
      displayNameInput.addEventListener("input", function () {
        state.displayName = String(displayNameInput.value || "").trim();
      });
    }

    if (emailInput) {
      emailInput.addEventListener("input", function () {
        state.email = String(emailInput.value || "").trim();
        setFieldError(errEmail, emailError(state.email));
        renderSummary();
      });
    }

    if (passwordInput) {
      passwordInput.addEventListener("input", function () {
        state.password = String(passwordInput.value || "");
        renderPasswordStatus();
        renderSummary();
      });
    }

    if (confirmPasswordInput) {
      confirmPasswordInput.addEventListener("input", function () {
        state.confirmPassword = String(confirmPasswordInput.value || "");
        setFieldError(errConfirmPassword, "");
        renderSummary();
      });
    }

    if (togglePasswordBtn && passwordInput) {
      togglePasswordBtn.addEventListener("click", function () {
        const show = passwordInput.type === "password";
        passwordInput.type = show ? "text" : "password";
        togglePasswordBtn.textContent = show ? "Hide" : "Show";
      });
    }

    if (toggleConfirmPasswordBtn && confirmPasswordInput) {
      toggleConfirmPasswordBtn.addEventListener("click", function () {
        const show = confirmPasswordInput.type === "password";
        confirmPasswordInput.type = show ? "text" : "password";
        toggleConfirmPasswordBtn.textContent = show ? "Hide" : "Show";
      });
    }

    if (setLaterInput) {
      setLaterInput.addEventListener("change", function () {
        state.setPasswordLater = Boolean(setLaterInput.checked);
        renderSecurityMode();
      });
    }

    if (roleSearchInput) {
      roleSearchInput.addEventListener("input", function () {
        state.roleSearch = String(roleSearchInput.value || "");
        renderRoles();
      });
    }

    if (roleSortSelect) {
      roleSortSelect.addEventListener("change", function () {
        state.roleSort = String(roleSortSelect.value || "name");
        renderRoles();
      });
    }

    if (saveBtn) {
      saveBtn.addEventListener("click", saveUser);
    }

    if (draftBtn) {
      draftBtn.addEventListener("click", saveDraft);
    }
  }

  function hydrateInitialState() {
    state.username = mode === "edit" ? normalizeUsername(user.username || "") : "";
    state.displayName = "";
    state.email = "";
    state.password = "";
    state.confirmPassword = "";
    state.setPasswordLater = false;
    state.roleSearch = "";
    state.roleSort = "name";
    state.selectedRoles = new Set(
      (Array.isArray(user.roles) ? user.roles : [])
        .map(function (item) { return String(item || "").trim(); })
        .filter(Boolean)
    );
    state.inspectedRole = Array.from(state.selectedRoles)[0] || "";

    if (usernameInput) {
      usernameInput.value = state.username;
    }
    if (displayNameInput) {
      displayNameInput.value = state.displayName;
    }
    if (emailInput) {
      emailInput.value = state.email;
    }

    renderSecurityMode();
    renderRoles();
    renderInspector();
    renderSummary();
    renderPasswordStatus();

    if (mode === "create") {
      restoreDraftIfAny();
    }

    setStatus(mode === "edit" ? "Update user details and role assignment." : "Fill user details and assign roles.", "info");
  }

  bindEvents();
  hydrateInitialState();
})();
