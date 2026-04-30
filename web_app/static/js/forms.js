(function () {
  const h = React.createElement;
  const SCHEDULE_MODE_OPTIONS = [
    { value: "", label: "Select a scheduling mode..." },
    { value: "manual", label: "Manual" },
    { value: "preset", label: "Preset" },
    { value: "cron", label: "Cron" },
    { value: "interval", label: "Interval" },
    { value: "event", label: "Event / Dataset" },
    { value: "custom", label: "Custom" },
  ];
  const SCHEDULE_PRESET_OPTIONS = [
    { value: "", label: "Choose a preset..." },
    { value: "once", label: "Once" },
    { value: "hourly", label: "Hourly" },
    { value: "daily", label: "Daily" },
    { value: "weekly", label: "Weekly" },
    { value: "monthly", label: "Monthly" },
    { value: "yearly", label: "Yearly" },
  ];
  const INTERVAL_UNITS = [
    { value: "minutes", label: "Minutes" },
    { value: "hours", label: "Hours" },
    { value: "days", label: "Days" },
  ];
  const ROUTER_COMMON_XCOM_KEYS_BY_TYPE = {
    azure: ["downloaded_files", "uploaded", "blob_name", "container_name", "create_container", "deleted", "deleted_count", "deleted_blobs"],
    azure_blob_sensor: ["matches", "matched_blobs", "blob_name", "blob_path"],
    cloudrun: ["cloud_run_url", "status_code", "response_text"],
    dataform: ["invocation_name", "state", "tag"],
    gcs: ["action", "bucket_name", "uploaded_count", "moved", "deleted_count", "uploaded_objects"],
    n8n: ["workflow_name", "workflow_id", "webhook_path", "status_code"],
    powerbi: ["workspace_id", "dataset_id", "status_code"],
    sftp_sensor: ["detected_file_name", "detected_file_path", "remote_dir", "prefix", "detected"],
    sftp_upload: [
      "action",
      "uploaded",
      "upload_mode",
      "uploaded_count",
      "local_file_path",
      "local_directory",
      "remote_path",
      "remote_paths",
      "downloaded_count",
      "downloaded_files",
      "local_dir_download",
      "deleted_count",
      "deleted_files",
    ],
    talend: ["job_name", "executable_id", "execution_id", "last_log"],
    y2: ["results", "local_file", "remote_path", "uploaded"],
    conditional_router: ["selected_branch_node_id", "selected_branch_task_id", "input_value"],
    if_else_router: ["selected_branch_node_id", "selected_branch_task_id", "input_value"],
    switch_router: ["selected_branch_node_id", "selected_branch_task_id", "input_value"],
    delay: ["delayed_minutes", "status"],
    manual_approval: ["approved", "approval_conf_key", "approval_value"],
    retry_wrapper: ["status"],
    try_catch: ["selected_branch_node_id", "selected_branch_task_id", "upstream_states"],
    quorum_join: ["quorum_met", "success_count", "required_success_count", "failed_count", "upstream_states"],
    parallel_limit: ["status"],
    semaphore_lock: ["pool_name", "pool_slots"],
  };
  const ROUTER_INPUT_NODE_TYPES = new Set(["conditional_router", "if_else_router", "switch_router"]);

  function buildRouterInputSuggestions(node, allEdges, allNodes) {
    if (!node || !ROUTER_INPUT_NODE_TYPES.has(node.type) || !node.id) {
      return { upstreamNodeIds: [], suggestions: [] };
    }

    const upstreamNodeIds = Array.isArray(allEdges)
      ? allEdges
          .filter(function (edge) {
            return edge && edge.target === node.id;
          })
          .map(function (edge) {
            return edge.source;
          })
          .filter(Boolean)
      : [];

    const uniqueUpstreamNodeIds = Array.from(new Set(upstreamNodeIds));
    const byNodeId = {};
    (Array.isArray(allNodes) ? allNodes : []).forEach(function (item) {
      if (item && item.id) {
        byNodeId[item.id] = item;
      }
    });
    const safeEdges = Array.isArray(allEdges) ? allEdges : [];

    const suggestions = [];
    uniqueUpstreamNodeIds.forEach(function (sourceNodeId) {
      const sourceNode = byNodeId[sourceNodeId] || {};
      const sourceType = String(sourceNode.type || "");

      // If router is connected to a Parallel Join, suggest the joined source nodes
      // rather than only the join node's aggregate/full XCom reference.
      const expandedSourceNodeIds = sourceType === "parallel_join"
        ? Array.from(
            new Set(
              safeEdges
                .filter(function (edge) {
                  return edge && edge.target === sourceNodeId && edge.source;
                })
                .map(function (edge) {
                  return edge.source;
                })
            )
          )
        : [];
      const candidateNodeIds = expandedSourceNodeIds.length ? expandedSourceNodeIds : [sourceNodeId];
      const joinLabel = String(sourceNode.label || sourceType || sourceNodeId);

      candidateNodeIds.forEach(function (candidateNodeId) {
        const candidateNode = byNodeId[candidateNodeId] || {};
        const candidateType = String(candidateNode.type || "");
        const candidateLabel = String(candidateNode.label || candidateType || candidateNodeId);
        const commonKeys = ROUTER_COMMON_XCOM_KEYS_BY_TYPE[candidateType] || [];
        const viaJoinSuffix = expandedSourceNodeIds.length ? " via " + joinLabel + " (" + sourceNodeId + ")" : "";

        suggestions.push({
          value: "xcom_node:" + candidateNodeId,
          label: candidateLabel + " (" + candidateNodeId + ") - full XCom value" + viaJoinSuffix,
        });

        commonKeys.forEach(function (keyName) {
          suggestions.push({
            value: "xcom_node:" + candidateNodeId + "." + keyName,
            label: candidateLabel + " (" + candidateNodeId + ") ." + keyName + viaJoinSuffix,
          });
        });
      });
    });

    const uniqueSuggestions = Array.from(new Set(suggestions.map(function (item) {
      return item.value;
    }))).map(function (value) {
      return suggestions.find(function (item) {
        return item.value === value;
      });
    }).filter(Boolean);

    return {
      upstreamNodeIds: uniqueUpstreamNodeIds,
      suggestions: uniqueSuggestions,
    };
  }

  async function uploadLocalFilesToBackend(files, formContext) {
    const safeFiles = Array.isArray(files) ? files.filter(Boolean) : [];
    if (!safeFiles.length) {
      return [];
    }
    const nodeId = String(formContext && formContext.node && formContext.node.id ? formContext.node.id : "").trim();
    const pipelineDagId = String(formContext && formContext.pipelineDagId ? formContext.pipelineDagId : "").trim();
    if (!nodeId || !pipelineDagId) {
      return [];
    }

    const formData = new FormData();
    formData.append("pipeline_id", pipelineDagId);
    formData.append("node_id", nodeId);
    safeFiles.forEach(function (file) {
      formData.append("files", file);
    });

    const resp = await fetch("/api/upload-local-files", {
      method: "POST",
      body: formData,
      credentials: "same-origin",
    });
    const data = await resp.json().catch(function () {
      return {};
    });
    if (!resp.ok || !data || data.success !== true) {
      throw new Error((data && data.error) || "Unable to upload local files.");
    }
    return Array.isArray(data.files) ? data.files : [];
  }

  function basenameFromPath(pathValue) {
    const normalized = String(pathValue || "").replace(/\\/g, "/").trim();
    if (!normalized) {
      return "";
    }
    const parts = normalized.split("/").filter(Boolean);
    return parts.length ? parts[parts.length - 1] : "";
  }

  function looksLikePath(pathValue) {
    const text = String(pathValue || "").trim();
    if (!text) {
      return false;
    }
    return text.indexOf("/") >= 0 || text.indexOf("\\") >= 0;
  }

  function renderField(field, value, onChange, config, formContext, readOnly) {
    const fieldType = field.type || "text";
    const current = value === undefined || value === null ? "" : value;

    if (fieldType === "textarea") {
      return h("textarea", {
        value: String(current),
        placeholder: field.placeholder || "",
        readOnly: Boolean(readOnly),
        disabled: Boolean(readOnly),
        onChange: function (event) {
          onChange(field.name, event.target.value);
        },
      });
    }

    if (fieldType === "json") {
      const textValue = typeof current === "string" ? current : JSON.stringify(current || {}, null, 2);
      return h("textarea", {
        value: textValue,
        placeholder: field.placeholder || "{}",
        readOnly: Boolean(readOnly),
        disabled: Boolean(readOnly),
        onChange: function (event) {
          onChange(field.name, event.target.value);
        },
      });
    }

    if (fieldType === "select") {
      return h(
        "select",
        {
          value: String(current),
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange(field.name, event.target.value);
          },
        },
        (field.options || []).map(function (option) {
          return h("option", { key: option.value, value: option.value }, option.label);
        })
      );
    }

    if (fieldType === "branch_target_select") {
      const targetOptions = (formContext && Array.isArray(formContext.routerTargetNodeIds))
        ? formContext.routerTargetNodeIds
        : [];
      const currentTarget = String(current || "");
      const mergedTargetOptions = targetOptions.includes(currentTarget) || !currentTarget
        ? targetOptions
        : targetOptions.concat([currentTarget]);
      return h(
        "select",
        {
          value: currentTarget,
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange(field.name, event.target.value);
          },
        },
        h("option", { value: "" }, "Target branch node"),
        mergedTargetOptions.map(function (nodeId) {
          return h("option", { key: "branch_target_" + field.name + "_" + nodeId, value: nodeId }, nodeId);
        })
      );
    }

    if (fieldType === "boolean") {
      return h(
        "div",
        { className: "field-inline" },
        h("input", {
          type: "checkbox",
          checked: Boolean(current),
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange(field.name, event.target.checked);
          },
        }),
        h("span", null, "Enabled")
      );
    }

    if (fieldType === "radio_boolean") {
      const boolValue = current === true || String(current).toLowerCase() === "true";
      return h(
        "div",
        { className: "radio-boolean-group" },
        h(
          "label",
          { className: "radio-boolean-item" },
          h("input", {
            type: "radio",
            name: "radio_" + field.name,
            checked: boolValue === true,
            disabled: Boolean(readOnly),
            onChange: function () {
              onChange(field.name, true);
            },
          }),
          h("span", null, "True")
        ),
        h(
          "label",
          { className: "radio-boolean-item" },
          h("input", {
            type: "radio",
            name: "radio_" + field.name,
            checked: boolValue === false,
            disabled: Boolean(readOnly),
            onChange: function () {
              onChange(field.name, false);
            },
          }),
          h("span", null, "False")
        )
      );
    }

    if (fieldType === "list") {
      const displayValue = Array.isArray(current) ? current.join(", ") : String(current);
      return h("input", {
        type: "text",
        value: displayValue,
        placeholder: field.placeholder || "item1,item2",
        readOnly: Boolean(readOnly),
        disabled: Boolean(readOnly),
        onChange: function (event) {
          onChange(field.name, event.target.value);
        },
      });
    }

    if (fieldType === "file_single") {
      function resolveFilePath(file) {
        const fromFilePath = file && typeof file.path === "string" ? file.path.trim() : "";
        if (fromFilePath) {
          return fromFilePath;
        }
        const fromName = file && typeof file.name === "string" ? file.name.trim() : "";
        return fromName;
      }

      return h(
        "div",
        { className: "file-picker-group" },
        h("input", {
          type: "file",
          disabled: Boolean(readOnly),
          onChange: async function (event) {
            const file = event.target.files && event.target.files[0];
            const selectedPath = resolveFilePath(file);
            const isLocalUploadBrowserField = field.name === "local_file_browser";
            const isAzureNode = Boolean(formContext && formContext.node && formContext.node.type === "azure");
            const isSftpUploadNode = Boolean(formContext && formContext.node && formContext.node.type === "sftp_upload");
            const isAzureLocalUploadBrowserField = isLocalUploadBrowserField && isAzureNode;
            if (!isLocalUploadBrowserField) {
              onChange(field.name, selectedPath);
            }
            if (!file || readOnly) {
              if (isLocalUploadBrowserField) {
                onChange(field.name, "");
                onChange("local_file_path", "");
                if (isAzureLocalUploadBrowserField) {
                  onChange("local_file_path_container", "");
                }
              }
              return;
            }
            try {
              if (isLocalUploadBrowserField) {
                onChange(field.name, "Uploading to container...");
                onChange("local_file_path", "Uploading to container...");
              }
              const uploaded = await uploadLocalFilesToBackend([file], formContext);
              const containerPath = uploaded[0] && uploaded[0].container_path ? String(uploaded[0].container_path) : "";
              if (!containerPath) {
                if (isLocalUploadBrowserField) {
                  if (isAzureLocalUploadBrowserField) {
                    onChange(field.name, "");
                    onChange("local_file_path", "");
                    onChange("local_file_path_container", "");
                  } else if (isSftpUploadNode) {
                    if (looksLikePath(selectedPath)) {
                      const fallbackPath = String(selectedPath).trim();
                      onChange(field.name, basenameFromPath(fallbackPath) || fallbackPath);
                      onChange("local_file_path", fallbackPath);
                    } else {
                      onChange(field.name, "");
                      onChange("local_file_path", "");
                      if (typeof window !== "undefined" && typeof window.showToast === "function") {
                        window.showToast(
                          "SFTP file staging failed. Set Pipeline ID (dag_id) first, then browse file again.",
                          "warning",
                          5000
                        );
                      }
                    }
                  } else {
                    onChange(field.name, "");
                    onChange("local_file_path", "");
                  }
                }
                return;
              }
              if (isLocalUploadBrowserField) {
                if (isAzureLocalUploadBrowserField) {
                  const fileName = basenameFromPath(containerPath) || basenameFromPath(selectedPath) || "uploaded_file";
                  onChange(field.name, fileName);
                  onChange("local_file_path", fileName);
                  onChange("local_file_path_container", containerPath);
                } else {
                  onChange(field.name, containerPath);
                  onChange("local_file_path", containerPath);
                }
              } else {
                onChange(field.name, containerPath);
              }
            } catch (_error) {
              if (isLocalUploadBrowserField) {
                if (isAzureLocalUploadBrowserField) {
                  onChange(field.name, "");
                  onChange("local_file_path", "");
                  onChange("local_file_path_container", "");
                } else if (isSftpUploadNode) {
                  if (looksLikePath(selectedPath)) {
                    const fallbackPath = String(selectedPath).trim();
                    onChange(field.name, basenameFromPath(fallbackPath) || fallbackPath);
                    onChange("local_file_path", fallbackPath);
                  } else {
                    onChange(field.name, "");
                    onChange("local_file_path", "");
                    if (typeof window !== "undefined" && typeof window.showToast === "function") {
                      window.showToast(
                        "SFTP file staging failed. Set Pipeline ID (dag_id) first, then browse file again.",
                        "warning",
                        5000
                      );
                    }
                  }
                } else {
                  onChange(field.name, "");
                  onChange("local_file_path", "");
                }
              }
            }
          },
        }),
        current
          ? h("small", { className: "field-help" }, "Selected: " + String(current))
          : h("small", { className: "field-help" }, "No file selected.")
      );
    }

    if (fieldType === "file_multiple") {
      const selected = Array.isArray(current) ? current : [];

      function resolveFilePath(file) {
        const fromFilePath = file && typeof file.path === "string" ? file.path.trim() : "";
        if (fromFilePath) {
          return fromFilePath;
        }
        const fromName = file && typeof file.name === "string" ? file.name.trim() : "";
        return fromName;
      }

      function commonDirectory(filePaths) {
        if (!Array.isArray(filePaths) || filePaths.length === 0) {
          return "";
        }
        const withDir = filePaths.filter(function (item) {
          return /[\\/]/.test(String(item || ""));
        });
        if (withDir.length === 0) {
          return "";
        }
        const dirs = withDir.map(function (item) {
          return String(item || "").replace(/[\\/][^\\/]*$/, "");
        });
        const first = dirs[0];
        return dirs.every(function (value) { return value === first; }) ? first : "";
      }

      return h(
        "div",
        { className: "file-picker-group" },
        h("input", {
          type: "file",
          multiple: true,
          disabled: Boolean(readOnly),
          onChange: async function (event) {
            const files = Array.from((event.target && event.target.files) || []);
            const names = files.map(function (file) {
              return resolveFilePath(file);
            }).filter(Boolean);
            const isLocalUploadMultipleField = field.name === "local_file_paths";
            const isLocalDeleteMultipleField = field.name === "delete_local_file_paths";
            const isSftpUploadNode = Boolean(formContext && formContext.node && formContext.node.type === "sftp_upload");
            const isGcsNode = Boolean(formContext && formContext.node && formContext.node.type === "gcs");
            const isAzureNode = Boolean(formContext && formContext.node && formContext.node.type === "azure");
            if (!isLocalUploadMultipleField && !isLocalDeleteMultipleField) {
              onChange(field.name, names);
            }
            if (!files.length || readOnly) {
              if (isLocalUploadMultipleField) {
                onChange(field.name, []);
                onChange("local_file_path", "");
                onChange("local_directory_path", "");
              } else if (isLocalDeleteMultipleField) {
                onChange(field.name, []);
                onChange("delete_local_directory_path", "");
              }
              return;
            }
            try {
              if (isLocalUploadMultipleField || isLocalDeleteMultipleField) {
                onChange(field.name, ["Uploading to container..."]);
                if (isLocalUploadMultipleField) {
                  onChange("local_file_path", "Uploading to container...");
                  onChange("local_directory_path", "");
                } else {
                  onChange("delete_local_directory_path", "");
                }
              }
              const uploaded = await uploadLocalFilesToBackend(files, formContext);
              const containerPaths = uploaded
                .map(function (item) { return item && item.container_path ? String(item.container_path) : ""; })
                .filter(Boolean);
              if (!containerPaths.length) {
                if (isLocalUploadMultipleField) {
                  if (isSftpUploadNode) {
                    const validPathNames = names.filter(looksLikePath);
                    if (validPathNames.length > 0) {
                      onChange(field.name, validPathNames);
                      onChange("local_file_path", validPathNames[0] || "");
                    } else {
                      onChange(field.name, []);
                      onChange("local_file_path", "");
                      if (typeof window !== "undefined" && typeof window.showToast === "function") {
                        window.showToast(
                          "SFTP file staging failed. Set Pipeline ID (dag_id) first, then browse files again.",
                          "warning",
                          5000
                        );
                      }
                    }
                    onChange("local_directory_path", "");
                  } else {
                    onChange(field.name, []);
                    onChange("local_file_path", "");
                    onChange("local_directory_path", "");
                    if ((isGcsNode || isAzureNode) && typeof window !== "undefined" && typeof window.showToast === "function") {
                      window.showToast(
                        "File staging failed. Set Pipeline ID (dag_id) first, then browse files again.",
                        "warning",
                        5000
                      );
                    }
                  }
                } else if (isLocalDeleteMultipleField) {
                  onChange(field.name, []);
                  onChange("delete_local_directory_path", "");
                }
                return;
              }
              onChange(field.name, containerPaths);
              if (isLocalUploadMultipleField) {
                onChange("local_file_path", containerPaths[0] || "");
                onChange("local_directory_path", commonDirectory(containerPaths));
              } else if (isLocalDeleteMultipleField) {
                onChange("delete_local_directory_path", commonDirectory(containerPaths));
              }
            } catch (_error) {
              if (isLocalUploadMultipleField) {
                if (isSftpUploadNode) {
                  const validPathNames = names.filter(looksLikePath);
                  if (validPathNames.length > 0) {
                    onChange(field.name, validPathNames);
                    onChange("local_file_path", validPathNames[0] || "");
                  } else {
                    onChange(field.name, []);
                    onChange("local_file_path", "");
                    if (typeof window !== "undefined" && typeof window.showToast === "function") {
                      window.showToast(
                        "SFTP file staging failed. Set Pipeline ID (dag_id) first, then browse files again.",
                        "warning",
                        5000
                      );
                    }
                  }
                  onChange("local_directory_path", "");
                } else {
                  onChange(field.name, []);
                  onChange("local_file_path", "");
                  onChange("local_directory_path", "");
                  if ((isGcsNode || isAzureNode) && typeof window !== "undefined" && typeof window.showToast === "function") {
                    window.showToast(
                      "File staging failed. Set Pipeline ID (dag_id) first, then browse files again.",
                      "warning",
                      5000
                    );
                  }
                }
              } else if (isLocalDeleteMultipleField) {
                onChange(field.name, []);
                onChange("delete_local_directory_path", "");
              }
            }
          },
        }),
        selected.length
          ? h("small", { className: "field-help" }, "Selected files: " + selected.join(", "))
          : h("small", { className: "field-help" }, "No files selected.")
      );
    }

    if (fieldType === "link_list") {
      const links = Array.isArray(current) ? current : [];

      function updateLink(index, value) {
        const next = links.slice();
        next[index] = value;
        onChange(field.name, next);
      }

      function removeLink(index) {
        const next = links.filter(function (_item, idx) {
          return idx !== index;
        });
        onChange(field.name, next);
      }

      return h(
        "div",
        { className: "link-list-editor" },
        links.length === 0
          ? h("small", { className: "field-help" }, "No links yet. Add at least one link.")
          : null,
        links.map(function (link, idx) {
          return h(
            "div",
            { key: "link_item_" + field.name + "_" + idx, className: "router-rule-row" },
            h("input", {
              type: "text",
              value: String(link || ""),
              placeholder: "https://drive.google.com/file/d/<file_id>/view",
              readOnly: Boolean(readOnly),
              disabled: Boolean(readOnly),
              onChange: function (event) {
                updateLink(idx, event.target.value);
              },
            }),
            h(
              "button",
              {
                type: "button",
                className: "node-action-btn",
                disabled: Boolean(readOnly),
                onClick: function () {
                  removeLink(idx);
                },
              },
              "Remove"
            )
          );
        }),
        h(
          "button",
          {
            type: "button",
            className: "node-action-btn",
            disabled: Boolean(readOnly),
            onClick: function () {
              onChange(field.name, links.concat([""]));
            },
          },
          "Add Link"
        )
      );
    }

    if (
      fieldType === "text" &&
      field.name === "input_source" &&
      formContext &&
      formContext.node &&
      ROUTER_INPUT_NODE_TYPES.has(formContext.node.type)
    ) {
      const suggestions = Array.isArray(formContext.routerInputSourceSuggestions)
        ? formContext.routerInputSourceSuggestions
        : [];
      const upstreamNodeIds = Array.isArray(formContext.routerInputUpstreamNodeIds)
        ? formContext.routerInputUpstreamNodeIds
        : [];

      return h(
        "div",
        { className: "router-input-source-editor" },
        h("input", {
          type: "text",
          value: String(current),
          placeholder: field.placeholder || "xcom_node:node_3.detected_file_name",
          readOnly: Boolean(readOnly),
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange(field.name, event.target.value);
          },
        }),
        suggestions.length
          ? h(
              "select",
              {
                value: "",
                disabled: Boolean(readOnly),
                onChange: function (event) {
                  if (event.target.value) {
                    onChange(field.name, event.target.value);
                  }
                },
              },
              h("option", { value: "" }, "Insert from upstream suggestions..."),
              suggestions.map(function (item, idx) {
                return h("option", { key: "router_input_suggestion_" + idx, value: item.value }, item.label);
              })
            )
          : null,
        h(
          "small",
          { className: "field-help" },
          upstreamNodeIds.length
            ? "Upstream nodes: " + upstreamNodeIds.join(", ") + "."
            : "Connect upstream nodes to get xcom_node suggestions."
        )
      );
    }

    if (fieldType === "dynamic_project_ids") {
      const countRaw = config && config.project_count !== undefined ? config.project_count : 0;
      const count = Math.max(0, parseInt(countRaw, 10) || 0);
      const values = Array.isArray(current) ? current : [];

      if (count <= 0) {
        return h("small", null, "Enter number of projects first.");
      }

      return h(
        "div",
        { className: "dynamic-project-list" },
        Array.from({ length: count }).map(function (_, idx) {
          return h("input", {
            key: "project_id_" + idx,
            type: "text",
            value: values[idx] || "",
            placeholder: "Project ID #" + (idx + 1),
            readOnly: Boolean(readOnly),
            disabled: Boolean(readOnly),
            onChange: function (event) {
              const next = values.slice();
              next[idx] = event.target.value;
              onChange(field.name, next);
            },
          });
        })
      );
    }

    if (fieldType === "router_rules") {
      const rules = Array.isArray(current) ? current : [];
      const targetOptions = (formContext && Array.isArray(formContext.routerTargetNodeIds))
        ? formContext.routerTargetNodeIds
        : [];

      function updateRule(index, patch) {
        const next = rules.map(function (rule, idx) {
          if (idx !== index) {
            return rule;
          }
          return Object.assign({}, rule || {}, patch);
        });
        onChange(field.name, next);
      }

      function removeRule(index) {
        const next = rules.filter(function (_item, idx) {
          return idx !== index;
        });
        onChange(field.name, next);
      }

      return h(
        "div",
        { className: "router-rules-editor" },
        rules.length === 0
          ? h("small", { className: "field-help" }, "No rules yet. Add at least one routing rule.")
          : null,
        rules.map(function (rule, idx) {
          const resolvedRule = rule || {};
          const currentTarget = String(resolvedRule.target_branch_node_id || "");
          const mergedTargetOptions = targetOptions.includes(currentTarget) || !currentTarget
            ? targetOptions
            : targetOptions.concat([currentTarget]);

          return h(
            "div",
            { key: "router_rule_" + idx, className: "router-rule-row" },
            h(
              "select",
              {
                value: String(resolvedRule.match_type || ""),
                disabled: Boolean(readOnly),
                onChange: function (event) {
                  updateRule(idx, { match_type: event.target.value });
                },
              },
              h("option", { value: "" }, "Match type"),
              h("option", { value: "starts_with" }, "starts_with"),
              h("option", { value: "contains" }, "contains"),
              h("option", { value: "equals" }, "equals"),
              h("option", { value: "regex" }, "regex")
            ),
            h("input", {
              type: "text",
              value: String(resolvedRule.value || ""),
              placeholder: "Match value",
              readOnly: Boolean(readOnly),
              disabled: Boolean(readOnly),
              onChange: function (event) {
                updateRule(idx, { value: event.target.value });
              },
            }),
            h(
              "select",
              {
                value: currentTarget,
                disabled: Boolean(readOnly),
                onChange: function (event) {
                  updateRule(idx, { target_branch_node_id: event.target.value });
                },
              },
              h("option", { value: "" }, "Target branch node"),
              mergedTargetOptions.map(function (nodeId) {
                return h("option", { key: "target_" + idx + "_" + nodeId, value: nodeId }, nodeId);
              })
            ),
            h(
              "button",
              {
                type: "button",
                className: "node-action-btn",
                disabled: Boolean(readOnly),
                onClick: function () {
                  removeRule(idx);
                },
              },
              "Remove"
            )
          );
        }),
        h(
          "button",
          {
            type: "button",
            className: "node-action-btn",
            disabled: Boolean(readOnly),
            onClick: function () {
              const next = rules.concat([
                { match_type: "equals", value: "", target_branch_node_id: "" },
              ]);
              onChange(field.name, next);
            },
          },
          "Add Rule"
        )
      );
    }

    return h("input", {
      type: fieldType === "number" ? "number" : "text",
      value: String(current),
      placeholder: field.placeholder || "",
      readOnly: Boolean(readOnly),
      disabled: Boolean(readOnly),
      onChange: function (event) {
        onChange(field.name, event.target.value);
      },
    });
  }

  function isVisible(field, config) {
    if (field.show_if_all && Array.isArray(field.show_if_all)) {
      return field.show_if_all.every(function (condition) {
        const key = Object.keys(condition)[0];
        return config[key] === condition[key];
      });
    }
    if (!field.show_if) {
      return true;
    }
    const key = Object.keys(field.show_if)[0];
    const expected = field.show_if[key];
    return config[key] === expected;
  }

  function getScheduleErrors(pipeline) {
    // Validate only the currently selected mode to avoid irrelevant form errors.
    const mode = String(pipeline.schedule_mode || "").toLowerCase();
    if (!mode) {
      return [];
    }

    if (mode === "preset" && !String(pipeline.schedule_preset || "").trim()) {
      return ["Pick one preset frequency."];
    }

    if (mode === "cron") {
      const cron = String(pipeline.schedule_cron || "").trim();
      const parts = cron ? cron.split(/\s+/) : [];
      if (!cron) {
        return ["Cron expression is required."];
      }
      if (parts.length !== 5 && parts.length !== 6) {
        return ["Cron expression should contain 5 or 6 parts."];
      }
    }

    if (mode === "interval") {
      const value = Number(pipeline.interval_value);
      if (!Number.isFinite(value) || value <= 0) {
        return ["Interval value must be greater than 0."];
      }
      if (!String(pipeline.interval_unit || "").trim()) {
        return ["Select interval unit."];
      }
    }

    if (mode === "event" && !String(pipeline.event_source || "").trim()) {
      return ["Enter a dataset or event source name."];
    }

    if (mode === "custom" && !String(pipeline.custom_schedule_text || "").trim()) {
      return ["Describe your custom scheduling logic."];
    }

    return [];
  }

  function renderScheduleModeFields(pipeline, onChange, readOnly) {
    // Conditional rendering keeps the UI simple for non-technical users.
    const mode = String(pipeline.schedule_mode || "").toLowerCase();
    if (!mode) {
      return h(
        "div",
        { className: "schedule-mode-empty" },
        h("small", { className: "field-help" }, "Select a mode to show the scheduling input.")
      );
    }

    if (mode === "manual") {
      return h(
        "div",
        { className: "schedule-mode-content" },
        h("small", { className: "field-help" }, "Run only when triggered manually.")
      );
    }

    if (mode === "preset") {
      return h(
        "div",
        { className: "schedule-mode-content" },
        h("label", { className: "field-sub-label" }, "Preset Frequency"),
        h(
          "select",
          {
            value: String(pipeline.schedule_preset || ""),
            disabled: Boolean(readOnly),
            onChange: function (event) {
              onChange("schedule_preset", event.target.value);
            },
          },
          SCHEDULE_PRESET_OPTIONS.map(function (option) {
            return h("option", { key: option.value || "empty", value: option.value }, option.label);
          })
        ),
        h("small", { className: "field-help" }, "Choose a common schedule without writing cron.")
      );
    }

    if (mode === "cron") {
      return h(
        "div",
        { className: "schedule-mode-content" },
        h("label", { className: "field-sub-label" }, "Cron Expression"),
        h("input", {
          type: "text",
          value: String(pipeline.schedule_cron || ""),
          placeholder: "0 0 * * *",
          readOnly: Boolean(readOnly),
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange("schedule_cron", event.target.value);
          },
        }),
        h("small", { className: "field-help" }, "Use cron format for advanced scheduling.")
      );
    }

    if (mode === "interval") {
      return h(
        "div",
        { className: "schedule-mode-content" },
        h("label", { className: "field-sub-label" }, "Repeat Every"),
        h(
          "div",
          { className: "field-inline-combo" },
          h("input", {
            type: "number",
            min: "1",
            step: "1",
            value: pipeline.interval_value === null || pipeline.interval_value === undefined ? "" : String(pipeline.interval_value),
            placeholder: "30",
            readOnly: Boolean(readOnly),
            disabled: Boolean(readOnly),
            onChange: function (event) {
              onChange("interval_value", event.target.value);
            },
          }),
          h(
            "select",
            {
              value: String(pipeline.interval_unit || "minutes"),
              disabled: Boolean(readOnly),
              onChange: function (event) {
                onChange("interval_unit", event.target.value);
              },
            },
            INTERVAL_UNITS.map(function (option) {
              return h("option", { key: option.value, value: option.value }, option.label);
            })
          )
        ),
        h("small", { className: "field-help" }, "Example: every 30 minutes.")
      );
    }

    if (mode === "event") {
      return h(
        "div",
        { className: "schedule-mode-content" },
        h("label", { className: "field-sub-label" }, "Dataset / Event Source"),
        h("input", {
          type: "text",
          value: String(pipeline.event_source || ""),
          placeholder: "s3://bucket/file.csv or customer_updates",
          readOnly: Boolean(readOnly),
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange("event_source", event.target.value);
          },
        }),
        h("small", { className: "field-help" }, "Run when an external dataset or event is updated.")
      );
    }

    if (mode === "custom") {
      return h(
        "div",
        { className: "schedule-mode-content" },
        h("label", { className: "field-sub-label" }, "Custom Timetable Logic"),
        h("textarea", {
          value: String(pipeline.custom_schedule_text || ""),
          placeholder: "Describe your custom timetable or advanced scheduling logic.",
          readOnly: Boolean(readOnly),
          disabled: Boolean(readOnly),
          onChange: function (event) {
            onChange("custom_schedule_text", event.target.value);
          },
        }),
        h("small", { className: "field-help" }, "Use this for advanced, non-standard scheduling rules.")
      );
    }

    return null;
  }

  function PipelineSettingsForm(props) {
    const pipeline = props.pipeline;
    const scheduleErrors = getScheduleErrors(pipeline);

    return h(
      "div",
      { className: "form-section" },
      h("h3", null, "Pipeline Settings"),
      renderPipelineField("DAG ID", pipeline.dag_id, function (v) {
        props.onChange("dag_id", v);
      }, { placeholder: "my_pipeline", readOnly: props.readOnly }),
      renderPipelineField("Description", pipeline.description, function (v) {
        props.onChange("description", v);
      }, { asTextarea: true, placeholder: "Business-friendly summary of this pipeline", readOnly: props.readOnly }),
      renderPipelineField("Start Date", pipeline.start_date, function (v) {
        props.onChange("start_date", v);
      }, { type: "date", readOnly: props.readOnly, help: "Used in @dag(start_date=...). Format: YYYY-MM-DD." }),
      h(
        "div",
        { className: "field schedule-field" },
        h("label", null, "Scheduling Mode *"),
        h(
          "select",
          {
            value: String(pipeline.schedule_mode || ""),
            disabled: Boolean(props.readOnly),
            onChange: function (event) {
              props.onChange("schedule_mode", event.target.value);
            },
          },
          SCHEDULE_MODE_OPTIONS.map(function (option) {
            return h("option", { key: option.value || "empty", value: option.value }, option.label);
          })
        ),
        h("small", { className: "field-help" }, "Choose one scheduling style. Only relevant fields are shown."),
        renderScheduleModeFields(pipeline, props.onChange, props.readOnly),
        scheduleErrors.length
          ? h(
              "div",
              { className: "field-error" },
              scheduleErrors.map(function (msg, idx) {
                return h("div", { key: "schedule_error_" + idx }, msg);
              })
            )
          : null
      ),
      h(
        "div",
        { className: "field" },
        h("label", null, "Catchup"),
        h(
          "div",
          { className: "radio-boolean-group" },
          h(
            "label",
            { className: "radio-boolean-item" },
            h("input", {
              type: "radio",
              name: "pipeline_catchup",
              checked: pipeline.catchup === true,
              disabled: Boolean(props.readOnly),
              onChange: function () {
                props.onChange("catchup", true);
              },
            }),
            h("span", null, "True")
          ),
          h(
            "label",
            { className: "radio-boolean-item" },
            h("input", {
              type: "radio",
              name: "pipeline_catchup",
              checked: pipeline.catchup !== true,
              disabled: Boolean(props.readOnly),
              onChange: function () {
                props.onChange("catchup", false);
              },
            }),
            h("span", null, "False")
          )
        ),
        h("small", { className: "field-help" }, "Default is False. Enable only if you want backfill for past runs.")
      ),
      renderPipelineField("Retries", String(pipeline.retries), function (v) {
        props.onChange("retries", v);
      }, { type: "number", min: 0, step: 1, placeholder: "3", readOnly: props.readOnly }),
      renderPipelineField("Retry Delay Minutes", String(pipeline.retry_delay_minutes), function (v) {
        props.onChange("retry_delay_minutes", v);
      }, { type: "number", min: 0, step: 1, placeholder: "5", readOnly: props.readOnly }),
      renderPipelineField("Alert Emails (comma-separated)", Array.isArray(pipeline.alert_emails) ? pipeline.alert_emails.join(", ") : pipeline.alert_emails, function (v) {
        props.onChange("alert_emails", v);
      }, { placeholder: "alerts@example.com, owner@example.com", readOnly: props.readOnly }),
      h(
        "div",
        { className: "field" },
        h("label", null, "Alert Mode"),
        h(
          "select",
          {
            value: pipeline.alert_mode,
            disabled: Boolean(props.readOnly),
            onChange: function (event) {
              props.onChange("alert_mode", event.target.value);
            },
          },
          h("option", { value: "both" }, "Both"),
          h("option", { value: "on_failure" }, "On Failure"),
          h("option", { value: "on_retry" }, "On Retry"),
          h("option", { value: "none" }, "None")
        )
      ),
      renderPipelineField("Tags (comma-separated)", Array.isArray(pipeline.tags) ? pipeline.tags.join(", ") : pipeline.tags, function (v) {
        props.onChange("tags", v);
      }, { placeholder: "demo, orchestration", readOnly: props.readOnly }),
      renderPipelineField("Output DAG Filename", pipeline.output_filename, function (v) {
        props.onChange("output_filename", v);
      }, { placeholder: "my_pipeline.py", readOnly: props.readOnly })
    );
  }

  function renderPipelineField(label, value, onChange, options) {
    const opts = options || {};
    const resolvedValue = value === undefined || value === null ? "" : value;
    return h(
      "div",
      { className: "field", key: label },
      h("label", null, label),
      opts.asTextarea
        ? h("textarea", {
            value: resolvedValue,
            placeholder: opts.placeholder || "",
            readOnly: Boolean(opts.readOnly),
            disabled: Boolean(opts.readOnly),
            onChange: function (event) {
              onChange(event.target.value);
            },
          })
        : h("input", {
            type: opts.type || "text",
            min: opts.min,
            step: opts.step,
            value: resolvedValue,
            placeholder: opts.placeholder || "",
            readOnly: Boolean(opts.readOnly),
            disabled: Boolean(opts.readOnly),
            onChange: function (event) {
              onChange(event.target.value);
            },
          }),
      opts.help ? h("small", { className: "field-help" }, opts.help) : null
    );
  }

  function ModuleConfigForm(props) {
    const node = props.node;
    const schema = props.schema;
    if (!node) {
      return h(
        "div",
        { className: "form-section" },
        h("h3", null, "Node Configuration"),
        h("p", null, "Select a node from the canvas to edit its configuration.")
      );
    }

    if (!schema) {
      return h(
        "div",
        { className: "form-section" },
        h("h3", null, "Node Configuration"),
        h("p", null, "Loading schema for " + node.type + "...")
      );
    }

    const routerTargetNodeIds = node && node.id && Array.isArray(props.allEdges)
      ? props.allEdges
          .filter(function (edge) {
            return edge.source === node.id;
          })
          .map(function (edge) {
            return edge.target;
          })
      : [];
    const routerInputMeta = buildRouterInputSuggestions(node, props.allEdges, props.allNodes);

    return h(
      "div",
      { className: "form-section" },
      h("h3", null, "Node Configuration"),
      h("div", { className: "field" }, h("label", null, "Node Label"), h("input", {
        type: "text",
        value: node.label || "",
        readOnly: Boolean(props.readOnly),
        disabled: Boolean(props.readOnly),
        onChange: function (event) {
          props.onLabelChange(event.target.value);
        },
      })),
      (schema.fields || [])
        .filter(function (field) {
          return isVisible(field, node.config || {});
        })
        .map(function (field) {
          return h(
            "div",
            { className: "field", key: node.id + "_" + field.name },
            h(
              "label",
              null,
              field.label + (field.required ? " *" : "")
            ),
            renderField(
              field,
              (node.config || {})[field.name],
              props.onConfigChange,
              node.config || {},
              {
                node: node,
                pipelineDagId: props.pipelineDagId,
                routerTargetNodeIds: routerTargetNodeIds,
                routerInputSourceSuggestions: routerInputMeta.suggestions,
                routerInputUpstreamNodeIds: routerInputMeta.upstreamNodeIds,
              },
              props.readOnly
            ),
            field.description ? h("small", null, field.description) : null
          );
        })
    );
  }

  window.PipelineForms = {
    PipelineSettingsForm,
    ModuleConfigForm,
  };
})();
