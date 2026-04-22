(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  const statusBox = byId("admin-roles-status");
  const bodyEl = byId("admin-roles-body");
  const refreshBtn = byId("admin-roles-refresh");

  function setStatus(message, kind) {
    if (!statusBox) {
      return;
    }
    statusBox.className = "admin-inline-status " + (kind || "info");
    statusBox.textContent = message || "";
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

  function renderRows(roles) {
    if (!bodyEl) {
      return;
    }
    bodyEl.innerHTML = "";
    if (!Array.isArray(roles) || !roles.length) {
      bodyEl.innerHTML = '<tr><td colspan="5" class="admin-empty">No roles found.</td></tr>';
      return;
    }

    roles.forEach(function (role) {
      const row = document.createElement("tr");
      const privileges = Array.isArray(role.privileges) && role.privileges.length
        ? role.privileges.join(" | ")
        : "-";
      const roleType = role.is_predefined ? "Predefined" : "Custom";
      row.innerHTML =
        "<td>" + (role.name || "-") + "</td>" +
        "<td>" + roleType + "</td>" +
        "<td>" + (role.description || "-") + "</td>" +
        "<td>" + privileges + "</td>" +
        '<td class="connections-actions-cell"></td>';

      const actionsCell = row.querySelector(".connections-actions-cell");
      if (role.editable) {
        const editLink = document.createElement("a");
        editLink.className = "btn btn-secondary";
        editLink.href = "/admin/roles/" + encodeURIComponent(role.name) + "/edit";
        editLink.textContent = "Edit";
        actionsCell.appendChild(editLink);
      }

      if (role.deletable) {
        const delBtn = document.createElement("button");
        delBtn.type = "button";
        delBtn.className = "btn btn-secondary btn-danger-soft";
        delBtn.textContent = "Delete";
        delBtn.addEventListener("click", function () {
          deleteRole(role);
        });
        actionsCell.appendChild(delBtn);
      }

      if (!role.editable && !role.deletable) {
        const label = document.createElement("span");
        label.className = "admin-empty";
        label.textContent = "Read-only";
        actionsCell.appendChild(label);
      }

      bodyEl.appendChild(row);
    });
  }

  async function loadRoles() {
    try {
      setStatus("Loading roles...", "info");
      const data = await fetchJson("/api/admin/roles");
      renderRows(Array.isArray(data.roles) ? data.roles : []);
      setStatus("Roles loaded.", "success");
    } catch (error) {
      renderRows([]);
      setStatus(error && error.message ? error.message : "Unable to load roles.", "error");
    }
  }

  async function deleteRole(role) {
    if (!role || !role.name) {
      return;
    }
    const ok = window.confirm("Delete role '" + role.name + "'?");
    if (!ok) {
      return;
    }
    try {
      setStatus("Deleting role...", "info");
      const data = await fetchJson("/api/admin/roles/" + encodeURIComponent(role.name), {
        method: "DELETE",
      });
      renderRows(Array.isArray(data.roles) ? data.roles : []);
      const affected = Number(data.affected_users || 0);
      const suffix = affected > 0 ? " Updated users: " + affected + "." : "";
      setStatus("Role deleted." + suffix, "success");
    } catch (error) {
      setStatus(error && error.message ? error.message : "Unable to delete role.", "error");
    }
  }

  if (refreshBtn) {
    refreshBtn.addEventListener("click", loadRoles);
  }

  loadRoles();
})();
