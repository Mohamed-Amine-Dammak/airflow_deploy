(function () {
  const h = React.createElement;
  const useEffect = React.useEffect;
  const useMemo = React.useMemo;
  const useRef = React.useRef;
  const useState = React.useState;

  const defaultPipeline = {
    dag_id: "",
    description: "",
    start_date: "2026-01-01",
    schedule: null,
    schedule_mode: "manual",
    catchup: false,
    schedule_preset: "",
    schedule_cron: "",
    interval_value: "",
    interval_unit: "minutes",
    event_source: "",
    custom_schedule_text: "",
    retries: 3,
    retry_delay_minutes: 5,
    alert_emails: [],
    alert_mode: "both",
    tags: ["demo", "orchestration"],
    output_filename: "",
  };

  const presetBySchedule = {
    "@once": "once",
    "@hourly": "hourly",
    "@daily": "daily",
    "@weekly": "weekly",
    "@monthly": "monthly",
    "@yearly": "yearly",
  };
  const MULTI_OUT_TYPES = new Set(["parallel_fork", "parallel_limit", "conditional_router", "if_else_router", "switch_router", "try_catch"]);
  const MULTI_IN_TYPES = new Set(["parallel_join", "quorum_join"]);
  const DAG_RUN_STATE_LABELS = {
    not_generated: "Not generated",
    generating: "Generating",
    waiting_airflow: "Waiting for Airflow",
    ready_to_run: "Ready to run",
    running: "Running",
    retrying: "Retrying",
    run_started: "Run started",
    run_failed: "Run failed",
  };
  const TERMINAL_RUN_STATES = new Set(["success", "failed", "canceled", "cancelled"]);
  const NODE_STATE_PRIORITY = {
    failed: 6,
    up_for_retry: 5,
    retrying: 5,
    retry: 5,
    running: 4,
    queued: 3,
    success: 2,
    skipped: 1,
  };
  const CONNECTION_TYPE_OPTIONS = [
    { value: "http", label: "HTTP" },
    { value: "wasb", label: "Azure Blob Storage" },
  ];

  function normalizeDagRunStateClass(state) {
    return String(state || "not_generated").replace(/[^a-z0-9_-]+/gi, "_");
  }

  function normalizeTaskState(state) {
    return String(state || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]+/g, "_");
  }

  function hasRetryingTasks(taskList) {
    return (taskList || []).some(function (task) {
      const state = normalizeTaskState(task && task.state);
      return state === "up_for_retry" || state === "retrying" || state === "retry";
    });
  }

  function sanitizeIdentifier(raw, prefix) {
    const value = String(raw || "");
    let clean = value.replace(/[^A-Za-z0-9_]+/g, "_").replace(/_+/g, "_").replace(/^_+|_+$/g, "");
    if (!clean) {
      clean = prefix || "item";
    }
    if (!/^[A-Za-z_]/.test(clean)) {
      clean = (prefix || "item") + "_" + clean;
    }
    return clean.toLowerCase();
  }

  function safeDataformTagTaskId(rawTag) {
    const value = String(rawTag || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/_+/g, "_")
      .replace(/^_+|_+$/g, "");
    return "dataform_" + (value || "tag");
  }

  function ensureNodeTaskMapShape(value) {
    if (!value || typeof value !== "object") {
      return {};
    }

    const map = {};
    Object.keys(value).forEach(function (nodeId) {
      const raw = value[nodeId];
      if (Array.isArray(raw)) {
        map[nodeId] = raw
          .map(function (taskId) {
            return String(taskId || "").trim();
          })
          .filter(Boolean);
      } else if (typeof raw === "string" && raw.trim()) {
        map[nodeId] = [raw.trim()];
      }
    });
    return map;
  }

  function ensureNodePrimaryTaskMapShape(value) {
    if (!value || typeof value !== "object") {
      return {};
    }
    const map = {};
    Object.keys(value).forEach(function (nodeId) {
      const taskId = String(value[nodeId] || "").trim();
      if (taskId) {
        map[nodeId] = taskId;
      }
    });
    return map;
  }

  function createPayloadSignature(payload) {
    try {
      return JSON.stringify(payload || {});
    } catch (_err) {
      return "";
    }
  }

  function computeNextNodeSequence(nodes) {
    let maxSeq = 0;
    (nodes || []).forEach(function (node) {
      const match = String((node && node.id) || "").match(/^node_(\d+)$/i);
      if (!match) {
        return;
      }
      const num = Number(match[1]);
      if (Number.isFinite(num) && num > maxSeq) {
        maxSeq = num;
      }
    });
    return Math.max(1, maxSeq + 1);
  }

  function computeNextGroupSequence(groupBoxes) {
    let maxSeq = 0;
    (groupBoxes || []).forEach(function (box) {
      const match = String((box && box.id) || "").match(/^group_box_(\d+)$/i);
      if (!match) {
        return;
      }
      const num = Number(match[1]);
      if (Number.isFinite(num) && num > maxSeq) {
        maxSeq = num;
      }
    });
    return Math.max(1, maxSeq + 1);
  }

  function fallbackNodeTaskMap(nodes, executionOrder) {
    const orderIndexByNode = {};
    (executionOrder || []).forEach(function (nodeId, index) {
      orderIndexByNode[nodeId] = index + 1;
    });

    const map = {};
    (nodes || []).forEach(function (node, idx) {
      const orderIndex = orderIndexByNode[node.id] || idx + 1;
      const sanitizedNodeId = sanitizeIdentifier(node.id, "node_" + orderIndex);
      const defaultTaskId = node.type + "_" + orderIndex + "_" + sanitizedNodeId;

      if (node.type === "dataform") {
        const rawTags = (node.config && node.config.dataform_tags) || [];
        const tags = Array.isArray(rawTags)
          ? rawTags
          : String(rawTags || "")
              .split(",")
              .map(function (part) {
                return part.trim();
              })
              .filter(Boolean);
        if (tags.length > 0) {
          map[node.id] = tags.map(safeDataformTagTaskId);
          return;
        }
      }

      map[node.id] = [defaultTaskId];
    });
    return map;
  }

  function safeParseExtraToText(extraValue) {
    if (extraValue == null || extraValue === "") {
      return "";
    }
    if (typeof extraValue === "string") {
      return extraValue;
    }
    try {
      return JSON.stringify(extraValue, null, 2);
    } catch (_err) {
      return String(extraValue);
    }
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

  function hydrateScheduleForUi(rawPipeline) {
    const next = Object.assign({}, defaultPipeline, rawPipeline || {});
    next.start_date = String(next.start_date || "2026-01-01");
    next.schedule_mode = String(next.schedule_mode || "").toLowerCase();
    next.schedule_preset = String(next.schedule_preset || "").toLowerCase();
    next.schedule_cron = String(next.schedule_cron || "");
    next.interval_unit = String(next.interval_unit || "minutes").toLowerCase();
    next.event_source = String(next.event_source || "");
    next.custom_schedule_text = String(next.custom_schedule_text || "");
    next.catchup = next.catchup === true || String(next.catchup || "").toLowerCase() === "true";

    // Backward compatibility for older saved files that only had `schedule`.
    if (!next.schedule_mode) {
      const legacy = typeof next.schedule === "string" ? next.schedule.trim() : "";
      if (!legacy) {
        next.schedule_mode = "manual";
      } else if (presetBySchedule[legacy]) {
        next.schedule_mode = "preset";
        next.schedule_preset = next.schedule_preset || presetBySchedule[legacy];
      } else {
        next.schedule_mode = "cron";
        next.schedule_cron = next.schedule_cron || legacy;
      }
    }

    return next;
  }

  function buildAutoLayoutCoordinates(nodes, edges) {
    const indexById = {};
    const nodeById = {};
    const incomingById = {};
    const outgoingById = {};
    const indegreeById = {};
    const laneById = {};
    const levelById = {};
    const occupied = new Set();
    const positionsById = {};

    (nodes || []).forEach(function (node, idx) {
      indexById[node.id] = idx;
      nodeById[node.id] = node;
      incomingById[node.id] = [];
      outgoingById[node.id] = [];
      indegreeById[node.id] = 0;
      levelById[node.id] = 0;
    });

    (edges || []).forEach(function (edge) {
      if (!nodeById[edge.source] || !nodeById[edge.target]) {
        return;
      }
      outgoingById[edge.source].push(edge.target);
      incomingById[edge.target].push(edge.source);
      indegreeById[edge.target] += 1;
    });

    Object.keys(outgoingById).forEach(function (nodeId) {
      outgoingById[nodeId].sort(function (a, b) {
        return (indexById[a] || 0) - (indexById[b] || 0);
      });
    });
    Object.keys(incomingById).forEach(function (nodeId) {
      incomingById[nodeId].sort(function (a, b) {
        return (indexById[a] || 0) - (indexById[b] || 0);
      });
    });

    // Topological-ish order (falls back gracefully if graph has cycles).
    const indegreeWork = Object.assign({}, indegreeById);
    let queue = Object.keys(nodeById)
      .filter(function (nodeId) {
        return indegreeWork[nodeId] === 0;
      })
      .sort(function (a, b) {
        return (indexById[a] || 0) - (indexById[b] || 0);
      });
    const topoOrder = [];

    while (queue.length > 0) {
      const nodeId = queue.shift();
      topoOrder.push(nodeId);
      (outgoingById[nodeId] || []).forEach(function (childId) {
        indegreeWork[childId] -= 1;
        if (indegreeWork[childId] === 0) {
          queue.push(childId);
          queue.sort(function (a, b) {
            return (indexById[a] || 0) - (indexById[b] || 0);
          });
        }
      });
    }

    Object.keys(nodeById)
      .sort(function (a, b) {
        return (indexById[a] || 0) - (indexById[b] || 0);
      })
      .forEach(function (nodeId) {
        if (!topoOrder.includes(nodeId)) {
          topoOrder.push(nodeId);
        }
      });

    topoOrder.forEach(function (nodeId) {
      const parents = incomingById[nodeId] || [];
      if (!parents.length) {
        levelById[nodeId] = Math.max(levelById[nodeId] || 0, 0);
        return;
      }
      let maxParentLevel = 0;
      parents.forEach(function (parentId) {
        maxParentLevel = Math.max(maxParentLevel, (levelById[parentId] || 0) + 1);
      });
      levelById[nodeId] = maxParentLevel;
    });

    let nextLane = 0;
    topoOrder.forEach(function (nodeId) {
      if (laneById[nodeId] != null) {
        return;
      }
      const parents = incomingById[nodeId] || [];
      if (!parents.length) {
        laneById[nodeId] = nextLane;
        nextLane += 1;
        return;
      }

      // Anchor to parent lane; joins prefer top-most parent lane.
      let anchorLane = laneById[parents[0]];
      if (anchorLane == null) {
        anchorLane = 0;
      }
      parents.forEach(function (parentId) {
        const parentLane = laneById[parentId];
        if (parentLane == null) {
          return;
        }
        if (MULTI_IN_TYPES.has((nodeById[nodeId] && nodeById[nodeId].type) || "")) {
          anchorLane = Math.min(anchorLane, parentLane);
        } else {
          anchorLane = parentLane;
        }
      });
      laneById[nodeId] = anchorLane;
    });

    topoOrder.forEach(function (nodeId) {
      const children = outgoingById[nodeId] || [];
      const nodeType = (nodeById[nodeId] && nodeById[nodeId].type) || "";
      const isSplitNode = MULTI_OUT_TYPES.has(nodeType);
      const baseLane = laneById[nodeId] == null ? 0 : laneById[nodeId];

      children.forEach(function (childId, childIdx) {
        const suggestedLane = isSplitNode && childIdx > 0 ? nextLane : baseLane;
        if (isSplitNode && childIdx > 0) {
          nextLane += 1;
        }
        if (laneById[childId] == null) {
          laneById[childId] = suggestedLane;
        } else if (MULTI_IN_TYPES.has((nodeById[childId] && nodeById[childId].type) || "")) {
          laneById[childId] = Math.min(laneById[childId], suggestedLane);
        }
      });
    });

    const byLayoutOrder = topoOrder.slice().sort(function (a, b) {
      const la = levelById[a] || 0;
      const lb = levelById[b] || 0;
      if (la !== lb) {
        return la - lb;
      }
      const laneA = laneById[a] == null ? 0 : laneById[a];
      const laneB = laneById[b] == null ? 0 : laneById[b];
      if (laneA !== laneB) {
        return laneA - laneB;
      }
      return (indexById[a] || 0) - (indexById[b] || 0);
    });

    // Avoid overlap when two nodes land in same level+lane cell.
    byLayoutOrder.forEach(function (nodeId) {
      let lane = laneById[nodeId] == null ? 0 : laneById[nodeId];
      const level = levelById[nodeId] || 0;
      while (occupied.has(level + ":" + lane)) {
        lane += 1;
      }
      laneById[nodeId] = lane;
      occupied.add(level + ":" + lane);
    });

    const xStart = 60;
    const yStart = 90;
    const stepX = 290;
    const stepY = 180;

    Object.keys(nodeById).forEach(function (nodeId) {
      positionsById[nodeId] = {
        x: xStart + (levelById[nodeId] || 0) * stepX,
        y: yStart + (laneById[nodeId] || 0) * stepY,
      };
    });

    return positionsById;
  }

  function hasValidCanvasPosition(node) {
    return (
      node &&
      Number.isFinite(Number(node.x)) &&
      Number.isFinite(Number(node.y))
    );
  }

  function App() {
    const GROUP_BOX_DEFAULT_WIDTH = 380;
    const GROUP_BOX_DEFAULT_HEIGHT = 220;
    const CANVAS_STAGE_WIDTH = 6000;
    const CANVAS_STAGE_HEIGHT = 4000;
    const apiBaseOverride = getApiBaseOverride();
    const isAdminUser = Boolean(window.IS_ADMIN);
    const currentPermissions = Array.isArray(window.CURRENT_PERMISSIONS)
      ? window.CURRENT_PERMISSIONS.map(function (perm) {
          return String(perm || "").trim().toLowerCase();
        }).filter(Boolean)
      : [];
    const permissionSet = new Set(currentPermissions);
    function hasPermission(permission) {
      return isAdminUser || permissionSet.has(String(permission || "").trim().toLowerCase());
    }
    const currentRoles = Array.isArray(window.CURRENT_ROLES)
      ? window.CURRENT_ROLES.map(function (role) {
          return String(role || "").trim().toLowerCase();
        }).filter(Boolean)
      : [];
    const isViewerOnly = (
      currentRoles.includes("viewer") &&
      !currentRoles.some(function (role) {
        return role !== "viewer";
      })
    );
    const canLoadPipelines = hasPermission("pipeline_load");
    const canEditPipeline = hasPermission("pipeline_edit");
    const canValidatePipeline = hasPermission("pipeline_validate");
    const canGenerateDag = hasPermission("dag_generate");
    const canRunDag = hasPermission("dag_run");
    const canRetryDag = hasPermission("dag_retry");
    const canRetryNodeActions = hasPermission("task_retry");
    const canViewLogs = hasPermission("logs_view");
    const canViewConnections = hasPermission("connections_view");
    const canManageConnections = hasPermission("connections_manage");
    const canSavePipeline = hasPermission("pipeline_save");
    const canCreateNewPipeline = hasPermission("pipeline_new");
    const canDragModules = canEditPipeline;
    const canAutoLayout = hasPermission("pipeline_layout");
    const [pipeline, setPipeline] = useState(hydrateScheduleForUi(defaultPipeline));
    const [nodes, setNodes] = useState([]);
    const [edges, setEdges] = useState([]);
    const [groupBoxes, setGroupBoxes] = useState([]);
    const [nextGroupSeq, setNextGroupSeq] = useState(1);
    const [nextSeq, setNextSeq] = useState(1);
    const [selectedNodeId, setSelectedNodeId] = useState(null);
    const [selectedNodeIds, setSelectedNodeIds] = useState([]);
    const [selectedGroupBoxId, setSelectedGroupBoxId] = useState(null);
    const [selectedEdge, setSelectedEdge] = useState(null);
    const [connectingFromId, setConnectingFromId] = useState(null);
    const [moduleSchemas, setModuleSchemas] = useState({});
    const [statusText, setStatusText] = useState("Ready");
    const [statusKind, setStatusKind] = useState("status-ok");
    const [savedFiles, setSavedFiles] = useState([]);
    const [selectedSavedFile, setSelectedSavedFile] = useState("");
    const [pipelineVersions, setPipelineVersions] = useState([]);
    const [versionsLoading, setVersionsLoading] = useState(false);
    const [showDagVersions, setShowDagVersions] = useState(false);
    const [showExecutionHistory, setShowExecutionHistory] = useState(false);
    const [versionExecutionHistory, setVersionExecutionHistory] = useState([]);
    const [versionExecutionHistoryLoading, setVersionExecutionHistoryLoading] = useState(false);
    const [versionExecutionHistoryError, setVersionExecutionHistoryError] = useState("");
    const [currentVersionId, setCurrentVersionId] = useState("");
    const [selectedVersionId, setSelectedVersionId] = useState("");
    const [selectedVersionSummary, setSelectedVersionSummary] = useState(null);
    const [versionSummaryLoading, setVersionSummaryLoading] = useState(false);
    const [versionSummaryError, setVersionSummaryError] = useState("");
    const [moduleSearch, setModuleSearch] = useState("");
    const [isSidebarOpen, setIsSidebarOpen] = useState(true);
    const lastCanvasPointerWorldRef = useRef(null);
    const [executionOrder, setExecutionOrder] = useState([]);
    const [dagPreview, setDagPreview] = useState("");
    const [showPreview, setShowPreview] = useState(false);
    const [isDirty, setIsDirty] = useState(false);
    const [saveState, setSaveState] = useState("saved");
    const [lastGeneratedAt, setLastGeneratedAt] = useState(null);
    const [dagRunState, setDagRunState] = useState("not_generated");
    const [dagRunMeta, setDagRunMeta] = useState(null);
    const [autoRunAfterReadiness, setAutoRunAfterReadiness] = useState(false);
    const [autoRunConf, setAutoRunConf] = useState({});
    const [dagRunStatusText, setDagRunStatusText] = useState(DAG_RUN_STATE_LABELS.not_generated);
    const [readinessPollConfig, setReadinessPollConfig] = useState({
      timeoutSeconds: 600,
      intervalSeconds: 5,
    });
    const [runPollConfig, setRunPollConfig] = useState({
      timeoutSeconds: 1800,
      intervalSeconds: 5,
    });
    const [nodeTaskMap, setNodeTaskMap] = useState({});
    const [nodePrimaryTaskMap, setNodePrimaryTaskMap] = useState({});
    const [lastGeneratedPayloadSignature, setLastGeneratedPayloadSignature] = useState("");
    const [activeRun, setActiveRun] = useState(null);
    const [taskInstances, setTaskInstances] = useState([]);
    const [taskStatusByTaskId, setTaskStatusByTaskId] = useState({});
    const [nodeRetryingById, setNodeRetryingById] = useState({});
    const [logsPanel, setLogsPanel] = useState({
      isOpen: false,
      nodeId: null,
      taskId: "",
      state: "",
      tryNumber: 1,
      startDate: "",
      endDate: "",
      content: "",
      isLoading: false,
      error: "",
      lastFetchedAt: null,
    });
    const [connectionsUi, setConnectionsUi] = useState({
      isOpen: false,
      isLoading: false,
      isSubmitting: false,
      error: "",
      message: "",
      search: "",
      items: [],
      editorOpen: false,
      editorMode: "create",
      editorConnectionId: "",
      form: {
        connection_id: "",
        connection_type: "",
        description: "",
        host: "",
        login: "",
        password: "",
        port: "",
        schema: "",
        account_url: "",
        extra: "",
      },
      formErrors: {},
    });
    const canvasClipboardRef = useRef(null);
    const forceLatestOnNextVersionRefreshRef = useRef(false);
    const suppressPipelineIdVersionEffectRef = useRef(false);

    function buildApiCandidates(path) {
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
          (lastError && lastError.message ? lastError.message : "Network request failed.") +
          " If Flask is running on another URL/port, open builder with ?apiBase=http://HOST:PORT"
      );
    }

    function warnViewerReadOnly(actionLabel) {
      setStatusKind("status-warn");
      setStatusText("Viewer role is read-only. You cannot " + actionLabel + ".");
    }

    function edgeMatches(edge, sourceId, targetId) {
      return edge && edge.source === sourceId && edge.target === targetId;
    }

    function isTypingTarget(target) {
      if (!(target instanceof Element)) {
        return false;
      }
      const tagName = String(target.tagName || "").toUpperCase();
      if (tagName === "INPUT" || tagName === "TEXTAREA" || tagName === "SELECT") {
        return true;
      }
      return target.isContentEditable || Boolean(target.closest("[contenteditable='true']"));
    }

    function setDagLifecycleState(nextState, customText) {
      setDagRunState(nextState);
      setDagRunStatusText(customText || DAG_RUN_STATE_LABELS[nextState] || DAG_RUN_STATE_LABELS.not_generated);
    }

    const selectedNode = useMemo(
      function () {
        return nodes.find(function (item) {
          return item.id === selectedNodeId;
        }) || null;
      },
      [nodes, selectedNodeId]
    );

    const selectedSchema = selectedNode ? moduleSchemas[selectedNode.type] : null;
    const pipelineTitle = String(pipeline.dag_id || "").trim() || "Untitled Pipeline";
    const activeFileLabel = selectedSavedFile || "Unsaved pipeline.json";
    const generatedLabel = lastGeneratedAt
      ? new Date(lastGeneratedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
      : "Not generated yet";
    const runButtonEnabled = canRunDag && canGenerateDag && nodes.length > 0;
    const canRetryNow = canRetryDag && dagRunState === "retrying";
    const saveStateText =
      saveState === "failed" ? "Save failed" : saveState === "unsaved" ? "Unsaved changes" : "Saved";
    const saveStateClass = "save-status-pill state-" + saveState;
    const pipelineStateLabel =
      saveState === "failed" ? "Save Failed" : saveState === "unsaved" ? "Unsaved" : "Saved";
    const pipelineStateTooltip =
      saveState === "failed"
        ? "Pipeline save failed. Check details and try again."
        : saveState === "unsaved"
        ? "Pipeline has unsaved changes."
        : "Pipeline is saved.";
    function PipelineContextIcon() {
      return h(
        "svg",
        {
          viewBox: "0 0 24 24",
          className: "pipeline-context-icon-svg",
          "aria-hidden": "true",
          focusable: "false",
        },
        h("path", {
          d: "M6 4.8A2.2 2.2 0 1 0 6 9.2A2.2 2.2 0 1 0 6 4.8ZM18 4.8A2.2 2.2 0 1 0 18 9.2A2.2 2.2 0 1 0 18 4.8ZM12 16.8A2.2 2.2 0 1 0 12 21.2A2.2 2.2 0 1 0 12 16.8ZM8 6.8H16V8.2H8zM7.5 8.8L11.6 16.2L10.4 16.9L6.3 9.5zM16.5 8.8L17.7 9.5L13.6 16.9L12.4 16.2z",
          fill: "currentColor",
        })
      );
    }
    function SidebarCollapseIcon() {
      return h(
        "svg",
        { viewBox: "0 0 24 24", className: "sidebar-toggle-icon", "aria-hidden": "true", focusable: "false" },
        h("path", {
          d: "M15 6L9 12L15 18",
          fill: "none",
          stroke: "currentColor",
          strokeWidth: "2",
          strokeLinecap: "round",
          strokeLinejoin: "round",
        })
      );
    }
    function SidebarExpandIcon() {
      return h(
        "svg",
        { viewBox: "0 0 24 24", className: "sidebar-toggle-icon", "aria-hidden": "true", focusable: "false" },
        h("path", {
          d: "M9 6L15 12L9 18",
          fill: "none",
          stroke: "currentColor",
          strokeWidth: "2",
          strokeLinecap: "round",
          strokeLinejoin: "round",
        })
      );
    }
    const dagRunStateClass = "dag-run-state state-" + normalizeDagRunStateClass(dagRunState);
    const effectiveNodeTaskMap = useMemo(
      function () {
        const normalized = ensureNodeTaskMapShape(nodeTaskMap);
        if (Object.keys(normalized).length > 0) {
          return normalized;
        }
        return fallbackNodeTaskMap(nodes, executionOrder);
      },
      [nodeTaskMap, nodes, executionOrder]
    );
    const nodeRunStatusMap = useMemo(
      function () {
        const map = {};

        Object.keys(effectiveNodeTaskMap).forEach(function (nodeId) {
          const taskIds = effectiveNodeTaskMap[nodeId] || [];
          const matched = taskIds
            .map(function (taskId) {
              return taskStatusByTaskId[taskId];
            })
            .filter(Boolean);

          if (!matched.length) {
            map[nodeId] = {
              state: "",
              has_task_instance: false,
              task_id: "",
              try_number: 1,
            };
            return;
          }

          matched.sort(function (a, b) {
            const pa = NODE_STATE_PRIORITY[normalizeTaskState(a.state)] || 0;
            const pb = NODE_STATE_PRIORITY[normalizeTaskState(b.state)] || 0;
            if (pb !== pa) {
              return pb - pa;
            }
            return (Number(b.try_number) || 1) - (Number(a.try_number) || 1);
          });

          const chosen = matched[0];
          map[nodeId] = {
            state: chosen.state || "",
            has_task_instance: true,
            task_id: chosen.task_id || "",
            try_number: chosen.try_number || 1,
            start_date: chosen.start_date || "",
            end_date: chosen.end_date || "",
            task_ids: taskIds,
          };
        });

        return map;
      },
      [effectiveNodeTaskMap, taskStatusByTaskId]
    );
    const currentRunLabel = activeRun
      ? "Current run: " +
        (activeRun.dag_run_id || "-") +
        " (" +
        (String(activeRun.state || "unknown").toLowerCase() || "unknown") +
        ")"
      : "Current run: none";
    const selectedVersion = useMemo(
      function () {
        return (
          (pipelineVersions || []).find(function (item) {
            return String(item.version_id || "") === String(selectedVersionId || "");
          }) || null
        );
      },
      [pipelineVersions, selectedVersionId]
    );
    const groupedVersionExecutionHistory = useMemo(
      function () {
        const groups = {};
        (versionExecutionHistory || []).forEach(function (run) {
          const stamp = String(run.start_date || run.logical_date || "").trim();
          const key = stamp && stamp.indexOf("T") >= 0 ? stamp.split("T")[0] : (stamp || "Unknown Date");
          if (!groups[key]) {
            groups[key] = [];
          }
          groups[key].push(run);
        });
        return Object.keys(groups)
          .sort(function (a, b) {
            return a < b ? 1 : a > b ? -1 : 0;
          })
          .map(function (dateKey) {
            return { date: dateKey, runs: groups[dateKey] };
          });
      },
      [versionExecutionHistory]
    );
    const filteredConnections = useMemo(
      function () {
        const q = String(connectionsUi.search || "").trim().toLowerCase();
        if (!q) {
          return connectionsUi.items || [];
        }
        return (connectionsUi.items || []).filter(function (item) {
          return (
            String(item.connection_id || "").toLowerCase().includes(q) ||
            String(item.conn_type || "").toLowerCase().includes(q) ||
            String(item.description || "").toLowerCase().includes(q)
          );
        });
      },
      [connectionsUi.search, connectionsUi.items]
    );
    const filteredPaletteItems = useMemo(
      function () {
        const q = String(moduleSearch || "").trim().toLowerCase();
        if (!q) {
          return window.PipelineBuilderModules.PALETTE_ITEMS;
        }
        return window.PipelineBuilderModules.PALETTE_ITEMS.filter(function (item) {
          return (
            item.display_name.toLowerCase().includes(q) ||
            item.type.toLowerCase().includes(q) ||
            item.description.toLowerCase().includes(q)
          );
        });
      },
      [moduleSearch]
    );

    async function ensureSchema(moduleType) {
      if (moduleSchemas[moduleType]) {
        return;
      }
      const resp = await fetchApi("/api/module-schema/" + moduleType);
      const data = await resp.json();
      if (!data.success) {
        throw new Error(data.error || "Failed to fetch schema");
      }
      setModuleSchemas(function (prev) {
        return Object.assign({}, prev, { [moduleType]: data.schema });
      });
    }

    async function refreshSavedFiles() {
      if (!canLoadPipelines) {
        setSavedFiles([]);
        setSelectedSavedFile("");
        return;
      }
      try {
        const resp = await fetchApi("/api/load-pipeline");
        const data = await resp.json();
        if (data.success) {
          setSavedFiles(data.files || []);
          if (!selectedSavedFile && data.files && data.files.length > 0) {
            setSelectedSavedFile(data.files[0]);
          }
        }
      } catch (error) {
        setStatusKind("status-error");
        setStatusText(error && error.message ? error.message : "Unable to load saved pipeline files.");
      }
    }

    async function refreshPipelineVersions(preferredPipelineId, preferredVersionId, forceSelectLatest) {
      const pipelineId = String(preferredPipelineId || (pipeline && pipeline.dag_id) || "").trim();
      if (!pipelineId || !canLoadPipelines) {
        setPipelineVersions([]);
        setCurrentVersionId("");
        setSelectedVersionId("");
        setSelectedVersionSummary(null);
        return { pipelineId: pipelineId, versions: [], currentVersionId: "", selectedVersionId: "" };
      }
      let selectedVersionToUse = "";
      setVersionsLoading(true);
      try {
        const resp = await fetchApi("/api/pipeline-versions/" + encodeURIComponent(pipelineId));
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Failed to load versions.");
        }
        const versions = Array.isArray(data.versions) ? data.versions : [];
        const currentId = String(data.current_version_id || "");
        setPipelineVersions(versions);
        setCurrentVersionId(currentId);

        const preferred = String(preferredVersionId || "").trim();
        const hasPreferred = preferred && versions.some(function (item) { return item.version_id === preferred; });
        const hasSelected = selectedVersionId && versions.some(function (item) { return item.version_id === selectedVersionId; });
        if (forceSelectLatest === true && versions.length > 0) {
          selectedVersionToUse = String((versions[0] && versions[0].version_id) || "");
          setSelectedVersionId(selectedVersionToUse);
        } else if (hasPreferred) {
          selectedVersionToUse = preferred;
          setSelectedVersionId(preferred);
        } else if (!hasSelected) {
          selectedVersionToUse = currentId || (versions[0] && versions[0].version_id) || "";
          setSelectedVersionId(selectedVersionToUse);
        } else {
          selectedVersionToUse = String(selectedVersionId || "");
        }
        return {
          pipelineId: pipelineId,
          versions: versions,
          currentVersionId: currentId,
          selectedVersionId: selectedVersionToUse,
        };
      } catch (error) {
        setStatusKind("status-error");
        setStatusText(error && error.message ? error.message : "Unable to load pipeline versions.");
        return { pipelineId: pipelineId, versions: [], currentVersionId: "", selectedVersionId: "" };
      } finally {
        setVersionsLoading(false);
      }
    }

    async function applySelectedVersionToCanvas(version, summary) {
      if (!version || typeof version !== "object") {
        return;
      }
      const source = version.source_definition && typeof version.source_definition === "object"
        ? version.source_definition
        : null;
      if (!source) {
        return;
      }

      const loadedPipeline = hydrateScheduleForUi(source.pipeline || defaultPipeline);
      const loadedNodes = (source.nodes || []).map(function (node) {
        const normalizedConfig = Object.assign({}, node.config || {});
        let normalizedType = node.type;

        if (String(node.type || "").toLowerCase() === "sftp") {
          const action = String(normalizedConfig.action || "upload").toLowerCase();
          normalizedType = action === "sensor" ? "sftp_sensor" : "sftp_upload";
          delete normalizedConfig.action;
        }
        if (normalizedType === "dataform" && !normalizedConfig.project_mode) {
          normalizedConfig.project_mode = "single";
        }

        const logoFromType =
          window.PipelineBuilderModules && window.PipelineBuilderModules.getLogoForType
            ? window.PipelineBuilderModules.getLogoForType(normalizedType)
            : "";

        return Object.assign({}, node, {
          type: normalizedType,
          config: normalizedConfig,
          logo: logoFromType,
        });
      });

      const loadedEdges = source.edges || [];
      const rawGroupBoxes = Array.isArray(source.group_boxes)
        ? source.group_boxes
        : Array.isArray(source.groupBoxes)
        ? source.groupBoxes
        : [];
      const loadedGroupBoxes = rawGroupBoxes.map(function (box, idx) {
        const parsedX = Number(box.x);
        const parsedY = Number(box.y);
        const parsedWidth = Number(box.width);
        const parsedHeight = Number(box.height);
        return {
          id: String(box.id || "group_box_" + (idx + 1)),
          title: String(box.title || "Group " + (idx + 1)),
          x: Number.isFinite(parsedX) ? parsedX : 100,
          y: Number.isFinite(parsedY) ? parsedY : 100,
          width: Math.max(180, Number.isFinite(parsedWidth) ? parsedWidth : 380),
          height: Math.max(120, Number.isFinite(parsedHeight) ? parsedHeight : 220),
          color: String(box.color || "#2f6ea2"),
        };
      });

      const hasAnyInvalidPosition = loadedNodes.some(function (node) {
        return !hasValidCanvasPosition(node);
      });
      const layoutPositions = hasAnyInvalidPosition ? buildAutoLayoutCoordinates(loadedNodes, loadedEdges) : null;
      const normalizedNodes = loadedNodes.map(function (node) {
        const layoutPos = layoutPositions ? layoutPositions[node.id] : null;
        const safeX = hasValidCanvasPosition(node) ? Number(node.x) : layoutPos ? layoutPos.x : 40;
        const safeY = hasValidCanvasPosition(node) ? Number(node.y) : layoutPos ? layoutPos.y : 40;
        return Object.assign({}, node, { x: safeX, y: safeY });
      });

      setPipeline(loadedPipeline);
      setNodes(normalizedNodes);
      setEdges(loadedEdges);
      setGroupBoxes(loadedGroupBoxes);
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedGroupBoxId(null);
      setSelectedEdge(null);
      setConnectingFromId(null);
      setExecutionOrder([]);
      setNextSeq(computeNextNodeSequence(normalizedNodes));
      setNextGroupSeq(computeNextGroupSequence(loadedGroupBoxes));
      setIsDirty(false);
      setSaveState("saved");
      setDagPreview("");
      setShowPreview(false);
      setDagRunMeta({
        dag_id: String(version.airflow_dag_id || ""),
        airflow_path: String(version.generated_airflow_path || ""),
      });
      setNodeTaskMap(ensureNodeTaskMapShape(version.node_task_map));
      setNodePrimaryTaskMap(ensureNodePrimaryTaskMapShape(version.node_primary_task_map));
      setNodeRetryingById({});
      setLogsPanel({
        isOpen: false,
        nodeId: null,
        taskId: "",
        state: "",
        tryNumber: 1,
        startDate: "",
        endDate: "",
        content: "",
        isLoading: false,
        error: "",
        lastFetchedAt: null,
      });

      const hasRun = Boolean(summary && summary.has_ever_run && summary.last_run);
      if (hasRun) {
        const lastRun = summary.last_run || {};
        setActiveRun({
          dag_id: String(version.airflow_dag_id || ""),
          dag_run_id: String(lastRun.dag_run_id || ""),
          state: String(lastRun.state || ""),
          start_date: lastRun.start_date || "",
          end_date: lastRun.end_date || "",
          started_at: Date.now(),
        });
        setTaskInstances(Array.isArray(summary.tasks) ? summary.tasks : []);
        setTaskStatusByTaskId(
          summary.task_status_by_id && typeof summary.task_status_by_id === "object"
            ? summary.task_status_by_id
            : {}
        );
      } else {
        setActiveRun(null);
        setTaskInstances([]);
        setTaskStatusByTaskId({});
      }

      const uniqueTypes = Array.from(
        new Set(
          normalizedNodes.map(function (node) {
            return node.type;
          })
        )
      );
      for (let i = 0; i < uniqueTypes.length; i += 1) {
        await ensureSchema(uniqueTypes[i]);
      }
    }

    async function refreshSelectedVersionSummary(preferredVersionId, preferredPipelineId) {
      const pipelineId = String(preferredPipelineId || (pipeline && pipeline.dag_id) || "").trim();
      const versionId = String(preferredVersionId || selectedVersionId || "").trim();
      if (!pipelineId || !versionId) {
        setSelectedVersionSummary(null);
        setVersionSummaryError("");
        return;
      }
      setVersionSummaryLoading(true);
      setVersionSummaryError("");
      try {
        const resp = await fetchApi(
          "/api/pipeline-versions/" +
            encodeURIComponent(pipelineId) +
            "/" +
            encodeURIComponent(versionId) +
            "/last-execution"
        );
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Failed to load version execution summary.");
        }
        setSelectedVersionSummary(data.summary || null);
        await applySelectedVersionToCanvas(data.version || null, data.summary || null);
      } catch (error) {
        setSelectedVersionSummary(null);
        // Don't block editing UX when Airflow auth/session is unavailable.
        // Version selection should still load canvas metadata even without run history.
        setVersionSummaryError("");
      } finally {
        setVersionSummaryLoading(false);
      }
    }

    async function handleSetCurrentVersion() {
      if (!canGenerateDag) {
        warnViewerReadOnly("switch DAG versions");
        return;
      }
      const pipelineId = String((pipeline && pipeline.dag_id) || "").trim();
      const versionId = String(selectedVersionId || "").trim();
      if (!pipelineId || !versionId) {
        return;
      }
      if (versionId === currentVersionId) {
        setStatusKind("status-ok");
        setStatusText("Selected version is already current.");
        return;
      }
      const proceed = window.confirm("Switch current DAG version to " + versionId + "?");
      if (!proceed) {
        return;
      }
      try {
        const resp = await fetchApi("/api/pipeline-versions/" + encodeURIComponent(pipelineId) + "/current", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ version_id: versionId }),
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Failed to switch current version.");
        }
        setStatusKind("status-ok");
        setStatusText("Current DAG version switched to " + versionId + ".");
        await refreshPipelineVersions(pipelineId, versionId);
      } catch (error) {
        setStatusKind("status-error");
        setStatusText(error && error.message ? error.message : "Failed to switch current version.");
      }
    }

    async function handleClearVersions() {
      if (!canGenerateDag) {
        warnViewerReadOnly("clear DAG versions");
        return;
      }
      const pipelineId = String((pipeline && pipeline.dag_id) || "").trim();
      if (!pipelineId) {
        setStatusKind("status-warn");
        setStatusText("Pipeline DAG ID is required to clear versions.");
        return;
      }
      const proceed = window.confirm(
        "Clear all DAG versions for '" + pipelineId + "'? This deletes versioned DAG files from Airflow DAG folder."
      );
      if (!proceed) {
        return;
      }

      setVersionsLoading(true);
      try {
        const resp = await fetchApi("/api/pipeline-versions/" + encodeURIComponent(pipelineId), {
          method: "DELETE",
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Failed to clear versions.");
        }

        setPipelineVersions([]);
        setCurrentVersionId("");
        setSelectedVersionId("");
        setSelectedVersionSummary(null);
        setVersionSummaryError("");
        setVersionExecutionHistory([]);
        setVersionExecutionHistoryError("");
        setShowExecutionHistory(false);

        const removed = Number(data.versions_removed) || 0;
        const deletedFiles = Number(data.deleted_files_count) || 0;
        const deleteErrors = Array.isArray(data.delete_errors) ? data.delete_errors : [];
        if (deleteErrors.length > 0) {
          setStatusKind("status-warn");
          setStatusText(
            "Versions cleared (" +
              removed +
              "). Deleted files: " +
              deletedFiles +
              ". Some files could not be deleted: " +
              deleteErrors.join(" | ")
          );
        } else {
          setStatusKind("status-ok");
          setStatusText("Versions cleared (" + removed + "). Deleted files: " + deletedFiles + ".");
        }
      } catch (error) {
        setStatusKind("status-error");
        setStatusText(error && error.message ? error.message : "Failed to clear versions.");
      } finally {
        setVersionsLoading(false);
      }
    }

    async function loadSelectedVersionExecutionHistory() {
      const pipelineId = String((pipeline && pipeline.dag_id) || "").trim();
      const versionId = String(selectedVersionId || "").trim();
      if (!pipelineId || !versionId) {
        setVersionExecutionHistory([]);
        setVersionExecutionHistoryError("");
        return;
      }
      setVersionExecutionHistoryLoading(true);
      setVersionExecutionHistoryError("");
      try {
        const resp = await fetchApi(
          "/api/pipeline-versions/" +
            encodeURIComponent(pipelineId) +
            "/" +
            encodeURIComponent(versionId) +
            "/execution-history?limit=80"
        );
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Failed to load execution history.");
        }
        setVersionExecutionHistory(Array.isArray(data.runs) ? data.runs : []);
      } catch (error) {
        setVersionExecutionHistory([]);
        setVersionExecutionHistoryError(error && error.message ? error.message : "Failed to load execution history.");
      } finally {
        setVersionExecutionHistoryLoading(false);
      }
    }

    async function handleLoadHistoricalRun(runItem) {
      const version = selectedVersion;
      if (!version || !runItem) {
        return;
      }
      const dagId = String(version.airflow_dag_id || "").trim();
      const dagRunId = String(runItem.dag_run_id || "").trim();
      if (!dagId || !dagRunId) {
        return;
      }
      try {
        const taskList = await fetchRunTaskInstances(dagId, dagRunId);
        const statusByTaskId = {};
        taskList.forEach(function (task) {
          const taskId = String(task.task_id || "").trim();
          if (!taskId) {
            return;
          }
          statusByTaskId[taskId] = {
            task_id: taskId,
            state: task.state || "",
            try_number: task.try_number || 1,
            start_date: task.start_date || "",
            end_date: task.end_date || "",
          };
        });
        setActiveRun({
          dag_id: dagId,
          dag_run_id: dagRunId,
          state: runItem.state || "",
          start_date: runItem.start_date || "",
          end_date: runItem.end_date || "",
          started_at: Date.now(),
        });
        setTaskInstances(taskList);
        setTaskStatusByTaskId(statusByTaskId);
        setStatusKind("status-ok");
        setStatusText("Loaded execution " + dagRunId + " for " + (selectedVersionId || "selected version") + ".");
      } catch (error) {
        setStatusKind("status-error");
        setStatusText(error && error.message ? error.message : "Failed to load execution details.");
      }
    }

    function closeHeaderDropdowns() {
      const openMenus = document.querySelectorAll(".topbar .toolbar-menu[open]");
      openMenus.forEach(function (menu) {
        menu.removeAttribute("open");
      });
    }

    useEffect(function () {
      refreshSavedFiles();
    }, [canLoadPipelines]);

    useEffect(
      function () {
        if (suppressPipelineIdVersionEffectRef.current === true) {
          return;
        }
        const pipelineId = String((pipeline && pipeline.dag_id) || "").trim();
        refreshPipelineVersions(
          pipelineId,
          "",
          forceLatestOnNextVersionRefreshRef.current === true
        );
        forceLatestOnNextVersionRefreshRef.current = false;
      },
      [canLoadPipelines, pipeline.dag_id]
    );

    useEffect(
      function () {
        refreshSelectedVersionSummary(selectedVersionId, pipeline && pipeline.dag_id);
      },
      [selectedVersionId, pipeline && pipeline.dag_id]
    );

    useEffect(
      function () {
        if (!showExecutionHistory) {
          return;
        }
        loadSelectedVersionExecutionHistory();
      },
      [showExecutionHistory, selectedVersionId, pipeline && pipeline.dag_id]
    );

    useEffect(
      function () {
        if (canEditPipeline) {
          return;
        }
        setStatusKind("status-ok");
        setStatusText("Read-only mode enabled for your role. You can inspect pipelines based on your privileges.");
      },
      [canEditPipeline]
    );

    useEffect(
      function () {
        if (!showDagVersions) {
          return;
        }
        function onEscape(event) {
          if (event.key === "Escape") {
            setShowDagVersions(false);
          }
        }
        document.addEventListener("keydown", onEscape);
        return function () {
          document.removeEventListener("keydown", onEscape);
        };
      },
      [showDagVersions]
    );

    useEffect(function () {
      function onDocumentPointerDown(event) {
        const target = event.target;
        if (target instanceof Element && target.closest(".topbar .toolbar-menu")) {
          return;
        }
        closeHeaderDropdowns();
      }

      function onDocumentKeyDown(event) {
        if (event.key === "Escape") {
          closeHeaderDropdowns();
        }
      }

      document.addEventListener("pointerdown", onDocumentPointerDown);
      document.addEventListener("keydown", onDocumentKeyDown);
      return function () {
        document.removeEventListener("pointerdown", onDocumentPointerDown);
        document.removeEventListener("keydown", onDocumentKeyDown);
      };
    }, []);

    useEffect(
      function () {
        function onDeleteKeyDown(event) {
          if (!canEditPipeline) {
            return;
          }
          if (event.defaultPrevented) {
            return;
          }
          if (event.key !== "Delete" && event.key !== "Backspace") {
            return;
          }
          if (isTypingTarget(event.target)) {
            return;
          }
          if (
            !selectedNodeId &&
            !selectedEdge &&
            (!selectedNodeIds || selectedNodeIds.length === 0) &&
            !selectedGroupBoxId
          ) {
            return;
          }

          event.preventDefault();
          if (selectedEdge) {
            handleDeleteEdge(selectedEdge.source, selectedEdge.target);
            return;
          }
          if (selectedNodeIds && selectedNodeIds.length > 1) {
            handleDeleteNodes(selectedNodeIds);
            return;
          }
          if (selectedGroupBoxId) {
            handleDeleteGroupBox(selectedGroupBoxId);
            return;
          }
          if (selectedNodeId) {
            handleDeleteNode(selectedNodeId);
          }
        }

        document.addEventListener("keydown", onDeleteKeyDown);
        return function () {
          document.removeEventListener("keydown", onDeleteKeyDown);
        };
      },
      [canEditPipeline, selectedNodeId, selectedNodeIds, selectedGroupBoxId, selectedEdge, edges]
    );

    useEffect(
      function () {
        async function onCopyPasteKeyDown(event) {
          if (!canEditPipeline) {
            return;
          }
          if (event.defaultPrevented) {
            return;
          }
          if (isTypingTarget(event.target)) {
            return;
          }
          if (!event.ctrlKey && !event.metaKey) {
            return;
          }

          const key = String(event.key || "").toLowerCase();
          if (key === "c") {
            const selectedIds = (selectedNodeIds && selectedNodeIds.length ? selectedNodeIds : selectedNodeId ? [selectedNodeId] : []).filter(Boolean);
            if (!selectedIds.length) {
              return;
            }

            const selectedSet = new Set(selectedIds);
            const selectedNodes = nodes.filter(function (node) {
              return selectedSet.has(node.id);
            });
            if (!selectedNodes.length) {
              return;
            }

            const selectedEdges = edges.filter(function (edge) {
              return selectedSet.has(edge.source) && selectedSet.has(edge.target);
            });

            canvasClipboardRef.current = {
              nodes: selectedNodes.map(function (node) {
                return {
                  id: node.id,
                  type: node.type,
                  label: node.label,
                  x: Number(node.x) || 0,
                  y: Number(node.y) || 0,
                  logo: node.logo || "",
                  config: cloneConfig(node.config),
                };
              }),
              edges: selectedEdges.map(function (edge) {
                return { source: edge.source, target: edge.target };
              }),
              pasteCount: 0,
            };

            event.preventDefault();
            setStatusKind("status-ok");
            setStatusText("Copied " + selectedNodes.length + " module(s).");
            return;
          }

          if (key !== "v") {
            return;
          }

          const clip = canvasClipboardRef.current;
          if (!clip || !Array.isArray(clip.nodes) || clip.nodes.length === 0) {
            return;
          }

          event.preventDefault();

          const nextPasteCount = (Number(clip.pasteCount) || 0) + 1;
          const offset = 34 * nextPasteCount;
          let seq = nextSeq;
          const idMap = {};
          const clonedNodes = clip.nodes.map(function (node) {
            const newId = "node_" + seq;
            seq += 1;
            idMap[node.id] = newId;
            return {
              id: newId,
              type: node.type,
              label: node.label,
              x: (Number(node.x) || 0) + offset,
              y: (Number(node.y) || 0) + offset,
              logo: node.logo || "",
              config: cloneConfig(node.config),
            };
          });

          const clonedEdges = (clip.edges || [])
            .map(function (edge) {
              const sourceId = idMap[edge.source];
              const targetId = idMap[edge.target];
              if (!sourceId || !targetId || sourceId === targetId) {
                return null;
              }
              return { source: sourceId, target: targetId };
            })
            .filter(Boolean);

          const typesToEnsure = Array.from(
            new Set(
              clonedNodes.map(function (node) {
                return node.type;
              })
            )
          );
          for (let i = 0; i < typesToEnsure.length; i += 1) {
            await ensureSchema(typesToEnsure[i]);
          }

          setNodes(function (prev) {
            return prev.concat(clonedNodes);
          });
          setEdges(function (prev) {
            return prev.concat(clonedEdges);
          });
          setNextSeq(seq);
          setSelectedNodeIds(
            clonedNodes.map(function (node) {
              return node.id;
            })
          );
          setSelectedNodeId(clonedNodes[0] ? clonedNodes[0].id : null);
          setSelectedEdge(null);
          setIsDirty(true);
          setSaveState("unsaved");
          setStatusKind("status-ok");
          setStatusText("Pasted " + clonedNodes.length + " module(s) with " + clonedEdges.length + " connection(s).");
          canvasClipboardRef.current = Object.assign({}, clip, { pasteCount: nextPasteCount });
        }

        document.addEventListener("keydown", onCopyPasteKeyDown);
        return function () {
          document.removeEventListener("keydown", onCopyPasteKeyDown);
        };
      },
      [canEditPipeline, selectedNodeId, selectedNodeIds, nodes, edges, nextSeq]
    );

    async function handleCanvasDrop(payload) {
      if (!canEditPipeline) {
        return;
      }
      if (payload.kind !== "palette") {
        return;
      }

      const node = window.PipelineBuilderModules.createNodeFromPalette(payload.item, nextSeq, {
        x: payload.x,
        y: payload.y,
      });
      await ensureSchema(node.type);
      setNodes(function (prev) {
        return prev.concat([node]);
      });
      setNextSeq(function (prev) {
        return prev + 1;
      });
      setSelectedNodeId(node.id);
      setSelectedNodeIds([node.id]);
      setSelectedGroupBoxId(null);
      setSelectedEdge(null);
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-ok");
      setStatusText("Added node " + node.label);
    }

    function handleNodeMoved(nodeId, x, y) {
      if (!canEditPipeline) {
        return;
      }
      setNodes(function (prev) {
        return prev.map(function (node) {
          if (node.id !== nodeId) {
            return node;
          }
          return Object.assign({}, node, { x: x, y: y });
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function handleCanvasPointerWorldChange(position) {
      if (!position) {
        return;
      }
      const x = Number(position.x);
      const y = Number(position.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) {
        return;
      }
      lastCanvasPointerWorldRef.current = { x: x, y: y };
    }

    function handleAddGroupBox() {
      if (!canEditPipeline) {
        warnViewerReadOnly("add group boxes");
        return;
      }
      const id = "group_box_" + nextGroupSeq;
      const pointer = lastCanvasPointerWorldRef.current;
      const maxX = Math.max(8, CANVAS_STAGE_WIDTH - GROUP_BOX_DEFAULT_WIDTH - 8);
      const maxY = Math.max(8, CANVAS_STAGE_HEIGHT - GROUP_BOX_DEFAULT_HEIGHT - 8);
      const nextX = pointer
        ? Math.max(8, Math.min(maxX, Number(pointer.x) - GROUP_BOX_DEFAULT_WIDTH / 2))
        : 120;
      const nextY = pointer
        ? Math.max(8, Math.min(maxY, Number(pointer.y) - GROUP_BOX_DEFAULT_HEIGHT / 2))
        : 120;
      const groupBox = {
        id: id,
        title: "Group " + nextGroupSeq,
        x: nextX,
        y: nextY,
        width: GROUP_BOX_DEFAULT_WIDTH,
        height: GROUP_BOX_DEFAULT_HEIGHT,
        color: "#2f6ea2",
      };
      setGroupBoxes(function (prev) {
        return prev.concat([groupBox]);
      });
      setNextGroupSeq(function (prev) {
        return prev + 1;
      });
      setSelectedGroupBoxId(id);
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedEdge(null);
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-ok");
      setStatusText("Added Group Box '" + groupBox.title + "'.");
    }

    function handleGroupBoxMoved(groupId, x, y) {
      if (!canEditPipeline) {
        return;
      }
      setGroupBoxes(function (prev) {
        return prev.map(function (box) {
          if (box.id !== groupId) {
            return box;
          }
          return Object.assign({}, box, { x: x, y: y });
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function handleGroupBoxResized(groupId, width, height) {
      if (!canEditPipeline) {
        return;
      }
      setGroupBoxes(function (prev) {
        return prev.map(function (box) {
          if (box.id !== groupId) {
            return box;
          }
          return Object.assign({}, box, {
            width: Math.max(180, Number(width) || 180),
            height: Math.max(120, Number(height) || 120),
          });
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function handleGroupBoxChange(groupId, field, value) {
      if (!canEditPipeline) {
        return;
      }
      setGroupBoxes(function (prev) {
        return prev.map(function (box) {
          if (box.id !== groupId) {
            return box;
          }
          if (field === "title") {
            return Object.assign({}, box, { title: String(value || "") });
          }
          if (field === "color") {
            return Object.assign({}, box, { color: String(value || "#2f6ea2") });
          }
          return box;
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function handleDeleteGroupBox(groupId) {
      if (!canEditPipeline) {
        warnViewerReadOnly("delete group boxes");
        return;
      }
      setGroupBoxes(function (prev) {
        return prev.filter(function (box) {
          return box.id !== groupId;
        });
      });
      setSelectedGroupBoxId(function (prev) {
        return prev === groupId ? null : prev;
      });
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-warn");
      setStatusText("Deleted Group Box '" + groupId + "'.");
    }

    function cloneConfig(value) {
      try {
        return JSON.parse(JSON.stringify(value == null ? {} : value));
      } catch (_err) {
        return {};
      }
    }

    async function handleDuplicateNode(nodeId) {
      if (!canEditPipeline) {
        warnViewerReadOnly("duplicate modules");
        return;
      }
      const sourceNode = nodes.find(function (node) {
        return node.id === nodeId;
      });
      if (!sourceNode) {
        return;
      }
      await ensureSchema(sourceNode.type);
      const duplicated = {
        id: "node_" + nextSeq,
        type: sourceNode.type,
        label: sourceNode.label,
        x: (Number(sourceNode.x) || 0) + 36,
        y: (Number(sourceNode.y) || 0) + 36,
        logo: sourceNode.logo || "",
        config: cloneConfig(sourceNode.config),
      };
      setNodes(function (prev) {
        return prev.concat([duplicated]);
      });
      setNextSeq(function (prev) {
        return prev + 1;
      });
      setSelectedNodeId(duplicated.id);
      setSelectedNodeIds([duplicated.id]);
      setSelectedGroupBoxId(null);
      setSelectedEdge(null);
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-ok");
      setStatusText("Duplicated module '" + sourceNode.label + "'.");
    }

    function handleDeleteNode(nodeId) {
      if (!canEditPipeline) {
        warnViewerReadOnly("delete modules");
        return;
      }
      setNodes(function (prev) {
        return prev.filter(function (node) {
          return node.id !== nodeId;
        });
      });
      setEdges(function (prev) {
        return prev.filter(function (edge) {
          return edge.source !== nodeId && edge.target !== nodeId;
        });
      });
      if (selectedNodeId === nodeId) {
        setSelectedNodeId(null);
      }
      setSelectedNodeIds(function (prev) {
        return prev.filter(function (id) {
          return id !== nodeId;
        });
      });
      setSelectedEdge(function (prev) {
        if (prev && (prev.source === nodeId || prev.target === nodeId)) {
          return null;
        }
        return prev;
      });
      if (connectingFromId === nodeId) {
        setConnectingFromId(null);
      }
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-warn");
      setStatusText("Deleted node " + nodeId);
    }

    function handleDeleteNodes(nodeIds) {
      const ids = Array.from(new Set((nodeIds || []).map(function (id) {
        return String(id || "").trim();
      }).filter(Boolean)));
      if (!ids.length) {
        return;
      }
      if (!canEditPipeline) {
        warnViewerReadOnly("delete modules");
        return;
      }
      const idSet = new Set(ids);
      setNodes(function (prev) {
        return prev.filter(function (node) {
          return !idSet.has(node.id);
        });
      });
      setEdges(function (prev) {
        return prev.filter(function (edge) {
          return !idSet.has(edge.source) && !idSet.has(edge.target);
        });
      });
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedEdge(function (prev) {
        if (prev && (idSet.has(prev.source) || idSet.has(prev.target))) {
          return null;
        }
        return prev;
      });
      setConnectingFromId(function (prev) {
        return prev && idSet.has(prev) ? null : prev;
      });
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-warn");
      setStatusText("Deleted " + ids.length + " module(s).");
    }

    function handleDeleteEdge(sourceId, targetId) {
      if (!canEditPipeline) {
        warnViewerReadOnly("delete connections");
        return;
      }
      const exists = edges.some(function (edge) {
        return edgeMatches(edge, sourceId, targetId);
      });
      if (!exists) {
        setSelectedEdge(null);
        setStatusKind("status-warn");
        setStatusText("Connection already removed.");
        return;
      }

      setEdges(function (prev) {
        return prev.filter(function (edge) {
          return !edgeMatches(edge, sourceId, targetId);
        });
      });
      setSelectedEdge(null);
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-warn");
      setStatusText("Deleted connection " + sourceId + " -> " + targetId);
    }

    function handleSelectEdge(edge) {
      if (!edge || !edge.source || !edge.target) {
        setSelectedEdge(null);
        return;
      }
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedGroupBoxId(null);
      setConnectingFromId(null);
      setSelectedEdge({ source: edge.source, target: edge.target });
    }

    function handleCanvasClearSelection() {
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedGroupBoxId(null);
      setSelectedEdge(null);
    }

    function handleSelectGroupBox(groupId) {
      setSelectedGroupBoxId(groupId || null);
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedEdge(null);
    }

    function connectNodes(sourceId, targetId) {
      if (!canEditPipeline) {
        warnViewerReadOnly("connect modules");
        return false;
      }
      if (sourceId === targetId) {
        setStatusKind("status-error");
        setStatusText("Cannot connect a node to itself.");
        return false;
      }

      const sourceNode = nodes.find(function (node) {
        return node.id === sourceId;
      });
      const targetNode = nodes.find(function (node) {
        return node.id === targetId;
      });
      if (!sourceNode || !targetNode) {
        setStatusKind("status-error");
        setStatusText("Cannot connect unknown nodes.");
        return false;
      }

      const duplicateEdge = edges.find(function (edge) {
        return edge.source === sourceId && edge.target === targetId;
      });
      if (duplicateEdge) {
        setStatusKind("status-warn");
        setStatusText("This connection already exists.");
        return false;
      }

      const sourceOutCount = edges.filter(function (edge) {
        return edge.source === sourceId;
      }).length;
      const targetInCount = edges.filter(function (edge) {
        return edge.target === targetId;
      }).length;

      if (!MULTI_OUT_TYPES.has(sourceNode.type) && sourceOutCount >= 1) {
        setStatusKind("status-error");
        setStatusText("This source already has an outgoing connection.");
        return false;
      }

      if (!MULTI_IN_TYPES.has(targetNode.type) && targetInCount >= 1) {
        setStatusKind("status-error");
        setStatusText("This target already has an incoming connection.");
        return false;
      }

      setEdges(function (prev) {
        return prev.concat([{ source: sourceId, target: targetId }]);
      });
      setSelectedEdge(null);
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-ok");
      setStatusText("Connected " + sourceId + " -> " + targetId);
      return true;
    }

    function handleConnectorClick(nodeId, side) {
      if (!canEditPipeline) {
        warnViewerReadOnly("connect modules");
        return;
      }
      if (side === "out") {
        if (connectingFromId === nodeId) {
          setConnectingFromId(null);
          setStatusKind("status-warn");
          setStatusText("Connection source cancelled.");
          return;
        }

        setConnectingFromId(nodeId);
        setStatusKind("status-ok");
        setStatusText("Source selected: " + nodeId + ". Click target node left edge.");
        return;
      }

      if (!connectingFromId) {
        setStatusKind("status-warn");
        setStatusText("Select a source output edge first.");
        return;
      }

      connectNodes(connectingFromId, nodeId);
      setConnectingFromId(null);
    }

    async function handleNodeClick(nodeId, options) {
      if (canEditPipeline && connectingFromId && connectingFromId !== nodeId) {
        connectNodes(connectingFromId, nodeId);
        setConnectingFromId(null);
        return;
      }

      const node = nodes.find(function (item) {
        return item.id === nodeId;
      });
      if (node) {
        await ensureSchema(node.type);
      }
      const toggleSelect = Boolean(options && options.toggle);
      setSelectedEdge(null);
      setSelectedGroupBoxId(null);
      if (toggleSelect) {
        const alreadySelected = selectedNodeIds.includes(nodeId);
        if (alreadySelected) {
          const nextSelected = selectedNodeIds.filter(function (id) {
            return id !== nodeId;
          });
          setSelectedNodeIds(nextSelected);
          setSelectedNodeId(nextSelected.length ? nextSelected[nextSelected.length - 1] : null);
          return;
        }
        setSelectedNodeIds(selectedNodeIds.concat([nodeId]));
        setSelectedNodeId(nodeId);
        return;
      }
      setSelectedNodeIds([nodeId]);
      setSelectedNodeId(nodeId);
    }

    function handlePipelineFieldChange(field, value) {
      if (!canEditPipeline) {
        return;
      }
      setPipeline(function (prev) {
        const next = Object.assign({}, prev);
        if (field === "retries" || field === "retry_delay_minutes" || field === "interval_value") {
          next[field] = value;
        } else {
          next[field] = value;
        }
        if (field === "dag_id" && (!next.output_filename || next.output_filename === prev.output_filename)) {
          next.output_filename = (value || "pipeline") + ".py";
        }
        return next;
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function handleNodeLabelChange(value) {
      if (!canEditPipeline) {
        return;
      }
      if (!selectedNodeId) {
        return;
      }
      setNodes(function (prev) {
        return prev.map(function (node) {
          if (node.id !== selectedNodeId) {
            return node;
          }
          return Object.assign({}, node, { label: value });
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function handleNodeConfigChange(field, value) {
      if (!canEditPipeline) {
        return;
      }
      if (!selectedNodeId) {
        return;
      }
      setNodes(function (prev) {
        return prev.map(function (node) {
          if (node.id !== selectedNodeId) {
            return node;
          }
          const nextConfig = Object.assign({}, node.config, { [field]: value });

          if (node.type === "dataform") {
            if (field === "project_mode") {
              if (value === "single") {
                nextConfig.project_count = "";
                nextConfig.project_ids = [];
              } else if (value === "multiple") {
                if (!nextConfig.project_count || Number(nextConfig.project_count) < 2) {
                  nextConfig.project_count = 2;
                }
                if (!Array.isArray(nextConfig.project_ids)) {
                  nextConfig.project_ids = [];
                }
              }
            }

            if (field === "project_count") {
              const count = Math.max(0, parseInt(value, 10) || 0);
              nextConfig.project_count = value;
              if (!Array.isArray(nextConfig.project_ids)) {
                nextConfig.project_ids = [];
              }
              if (count > 0) {
                const resized = Array.from({ length: count }).map(function (_, idx) {
                  return nextConfig.project_ids[idx] || "";
                });
                nextConfig.project_ids = resized;
              }
            }

            if (field === "project_ids") {
              const ids = Array.isArray(value) ? value : [];
              nextConfig.project_ids = ids;
              if (nextConfig.project_mode === "multiple" && ids.length > 0) {
                nextConfig.project_id = ids[0];
              }
            }
          }

          return Object.assign({}, node, { config: nextConfig });
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
    }

    function buildPayload() {
      return {
        pipeline: Object.assign({}, pipeline),
        nodes: nodes.map(function (node) {
          return {
            id: node.id,
            type: node.type,
            label: node.label,
            x: node.x,
            y: node.y,
            config: node.config,
          };
        }),
        edges: edges,
        group_boxes: groupBoxes.map(function (box) {
          return {
            id: box.id,
            title: box.title,
            x: Number(box.x) || 0,
            y: Number(box.y) || 0,
            width: Number(box.width) || 320,
            height: Number(box.height) || 180,
            color: box.color || "#2f6ea2",
          };
        }),
      };
    }

    async function parseApiJson(response) {
      const text = await response.text();
      try {
        return JSON.parse(text);
      } catch (_err) {
        const preview = String(text || "").slice(0, 220).replace(/\s+/g, " ").trim();
        throw new Error("Invalid server response (" + response.status + "). " + (preview || "No response body."));
      }
    }

    async function checkDagReadiness(meta) {
      if (!meta || !meta.dag_id) {
        return { ready: false };
      }

      const resp = await fetchApi("/api/dag-readiness", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dag_id: meta.dag_id,
          airflow_path: meta.airflow_path,
          auto_unpause: true,
        }),
      });
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to check DAG readiness.");
      }
      return data;
    }

    async function fetchRunStatus(dagId, dagRunId) {
      const resp = await fetchApi(
        "/api/runs/" + encodeURIComponent(dagId) + "/" + encodeURIComponent(dagRunId) + "/status"
      );
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to fetch DAG run status.");
      }
      return data;
    }

    async function fetchRunTaskInstances(dagId, dagRunId) {
      const resp = await fetchApi(
        "/api/runs/" + encodeURIComponent(dagId) + "/" + encodeURIComponent(dagRunId) + "/tasks"
      );
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to fetch task instances.");
      }
      return Array.isArray(data.task_instances) ? data.task_instances : [];
    }

    async function fetchTaskLogs(dagId, dagRunId, taskId, tryNumber) {
      const safeTryNumber = Math.max(1, Number(tryNumber) || 1);
      const resp = await fetchApi(
        "/api/runs/" +
          encodeURIComponent(dagId) +
          "/" +
          encodeURIComponent(dagRunId) +
          "/tasks/" +
          encodeURIComponent(taskId) +
          "/logs?try_number=" +
          safeTryNumber
      );
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to fetch task logs.");
      }
      return data;
    }

    async function triggerTaskRetry(dagId, dagRunId, taskId) {
      const resp = await fetchApi(
        "/api/runs/" +
          encodeURIComponent(dagId) +
          "/" +
          encodeURIComponent(dagRunId) +
          "/tasks/" +
          encodeURIComponent(taskId) +
          "/retry",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to retry task.");
      }
      return data;
    }

    function createEmptyConnectionForm() {
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
        extra: "",
      };
    }

    function normalizeConnectionTypeForUi(rawType) {
      const value = String(rawType || "").trim().toLowerCase();
      if (value === "http") {
        return "http";
      }
      if (value === "wasb" || value === "azure_blob_storage" || value === "azure_blob") {
        return "wasb";
      }
      return "";
    }

    function toConnectionForm(connection) {
      const form = createEmptyConnectionForm();
      const normalizedType = normalizeConnectionTypeForUi(connection && connection.conn_type);
      form.connection_id = String((connection && connection.connection_id) || "");
      form.connection_type = normalizedType;
      form.description = String((connection && connection.description) || "");
      form.host = String((connection && connection.host) || "");
      form.login = String((connection && connection.login) || "");
      form.schema = String((connection && connection.schema) || "");
      form.port = connection && connection.port != null ? String(connection.port) : "";
      form.extra = safeParseExtraToText(connection && connection.extra);
      if (normalizedType === "wasb") {
        form.account_url = String((connection && connection.host) || "");
      }
      return form;
    }

    function validateConnectionForm(form, mode) {
      const errors = {};
      const connectionId = String(form.connection_id || "").trim();
      const connectionType = normalizeConnectionTypeForUi(form.connection_type);
      const host = String(form.host || "").trim();
      const accountUrl = String(form.account_url || "").trim();
      const portRaw = String(form.port || "").trim();
      const extraRaw = String(form.extra || "").trim();

      if (mode === "create" && !connectionId) {
        errors.connection_id = "Connection ID is required.";
      }
      if (!connectionType) {
        errors.connection_type = "Connection type is required.";
      }
      if (connectionType === "http" && !host) {
        errors.host = "Host is required for HTTP connections.";
      }
      if (connectionType === "wasb" && !accountUrl) {
        errors.account_url = "Account URL is required for Azure Blob Storage connections.";
      }

      if (portRaw) {
        const portNum = Number(portRaw);
        if (!Number.isInteger(portNum) || portNum <= 0) {
          errors.port = "Port must be a valid integer greater than 0.";
        }
      }

      if (extraRaw) {
        try {
          const parsed = JSON.parse(extraRaw);
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            errors.extra = "Extra must be a JSON object.";
          }
        } catch (_err) {
          errors.extra = "Extra must be valid JSON.";
        }
      }
      return errors;
    }

    function buildConnectionPayload(form, mode) {
      const payload = {
        connection_type: normalizeConnectionTypeForUi(form.connection_type),
        description: String(form.description || "").trim(),
        login: String(form.login || "").trim(),
        password: String(form.password || ""),
        schema: String(form.schema || "").trim(),
        extra: String(form.extra || "").trim(),
      };

      if (mode === "create") {
        payload.connection_id = String(form.connection_id || "").trim();
      }

      if (payload.connection_type === "http") {
        payload.host = String(form.host || "").trim();
        payload.port = String(form.port || "").trim();
      } else if (payload.connection_type === "wasb") {
        payload.account_url = String(form.account_url || "").trim();
        payload.host = payload.account_url;
        payload.port = "";
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

    async function fetchConnectionsList() {
      const resp = await fetchApi("/api/airflow/connections");
      const data = await parseApiJson(resp);
      if (!resp.ok || !data.success) {
        throw new Error((data && data.error) || "Failed to load Airflow connections.");
      }
      return Array.isArray(data.connections) ? data.connections : [];
    }

    async function refreshConnectionsList() {
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, { isLoading: true, error: "", message: "" });
      });
      try {
        const items = await fetchConnectionsList();
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isLoading: false,
            items: items,
            error: "",
          });
        });
      } catch (error) {
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isLoading: false,
            error: error && error.message ? error.message : "Unable to load Airflow connections.",
          });
        });
      }
    }

    async function openConnectionsManager() {
      if (!canManageConnections) {
        warnViewerReadOnly("manage connections");
        return;
      }
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, {
          isOpen: true,
          isLoading: true,
          error: "",
          message: "",
          editorOpen: false,
          formErrors: {},
        });
      });
      try {
        const items = await fetchConnectionsList();
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isLoading: false,
            items: items,
            error: "",
          });
        });
      } catch (error) {
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isLoading: false,
            error: error && error.message ? error.message : "Unable to load Airflow connections.",
          });
        });
      }
    }

    function closeConnectionsManager() {
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, {
          isOpen: false,
          editorOpen: false,
          formErrors: {},
          error: "",
          message: "",
        });
      });
    }

    function openCreateConnectionEditor() {
      if (!canManageConnections) {
        warnViewerReadOnly("create connections");
        return;
      }
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, {
          editorOpen: true,
          editorMode: "create",
          editorConnectionId: "",
          form: createEmptyConnectionForm(),
          formErrors: {},
          message: "",
        });
      });
    }

    function openEditConnectionEditor(connection) {
      if (!canManageConnections) {
        warnViewerReadOnly("edit connections");
        return;
      }
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, {
          editorOpen: true,
          editorMode: "edit",
          editorConnectionId: String((connection && connection.connection_id) || ""),
          form: toConnectionForm(connection || {}),
          formErrors: {},
          message: "",
        });
      });
    }

    function closeConnectionEditor() {
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, {
          editorOpen: false,
          formErrors: {},
          isSubmitting: false,
        });
      });
    }

    function handleConnectionSearchChange(value) {
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, { search: value });
      });
    }

    function handleConnectionFormChange(field, value) {
      setConnectionsUi(function (prev) {
        const nextForm = Object.assign({}, prev.form, { [field]: value });
        if (field === "connection_type") {
          const normalizedType = normalizeConnectionTypeForUi(value);
          nextForm.connection_type = normalizedType;
          if (normalizedType === "http") {
            nextForm.account_url = "";
          } else if (normalizedType === "wasb") {
            nextForm.host = "";
            nextForm.port = "";
            nextForm.schema = "";
          }
        }

        const nextErrors = Object.assign({}, prev.formErrors);
        if (nextErrors[field]) {
          delete nextErrors[field];
        }
        return Object.assign({}, prev, {
          form: nextForm,
          formErrors: nextErrors,
          message: "",
        });
      });
    }

    async function submitConnectionForm() {
      if (!canManageConnections) {
        warnViewerReadOnly("save connections");
        return;
      }
      const mode = connectionsUi.editorMode;
      const form = connectionsUi.form;
      const errors = validateConnectionForm(form, mode);
      if (Object.keys(errors).length > 0) {
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, { formErrors: errors });
        });
        return;
      }

      const payload = buildConnectionPayload(form, mode);
      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, { isSubmitting: true, error: "", message: "", formErrors: {} });
      });

      try {
        let resp;
        if (mode === "create") {
          resp = await fetchApi("/api/airflow/connections", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
        } else {
          resp = await fetchApi(
            "/api/airflow/connections/" + encodeURIComponent(String(connectionsUi.editorConnectionId || "")),
            {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            }
          );
        }

        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          const firstError =
            data && Array.isArray(data.errors) && data.errors.length > 0 ? data.errors[0] : data && data.error;
          throw new Error(firstError || "Unable to save connection.");
        }

        const items = await fetchConnectionsList();
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isSubmitting: false,
            items: items,
            editorOpen: false,
            formErrors: {},
            message: mode === "create" ? "Connection created successfully." : "Connection updated successfully.",
          });
        });
      } catch (error) {
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isSubmitting: false,
            error: error && error.message ? error.message : "Unable to save connection.",
          });
        });
      }
    }

    async function deleteConnectionById(connectionId) {
      if (!canManageConnections) {
        warnViewerReadOnly("delete connections");
        return;
      }
      const safeId = String(connectionId || "").trim();
      if (!safeId) {
        return;
      }
      const confirmed = window.confirm("Delete connection '" + safeId + "'?");
      if (!confirmed) {
        return;
      }

      setConnectionsUi(function (prev) {
        return Object.assign({}, prev, { isLoading: true, error: "", message: "" });
      });
      try {
        const resp = await fetchApi("/api/airflow/connections/" + encodeURIComponent(safeId), {
          method: "DELETE",
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Unable to delete connection.");
        }
        const items = await fetchConnectionsList();
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isLoading: false,
            items: items,
            message: "Connection deleted: " + safeId,
            error: "",
          });
        });
      } catch (error) {
        setConnectionsUi(function (prev) {
          return Object.assign({}, prev, {
            isLoading: false,
            error: error && error.message ? error.message : "Unable to delete connection.",
          });
        });
      }
    }

    async function openNodeLogs(nodeId) {
      if (!canViewLogs) {
        warnViewerReadOnly("view task logs");
        return;
      }
      if (!activeRun || !activeRun.dag_id || !activeRun.dag_run_id) {
        setStatusKind("status-warn");
        setStatusText("Start a DAG run before opening logs.");
        return;
      }

      const nodeStatus = nodeRunStatusMap[nodeId];
      if (!nodeStatus || !nodeStatus.has_task_instance || !nodeStatus.task_id) {
        setStatusKind("status-warn");
        setStatusText("No Airflow task instance found yet for this node.");
        return;
      }

      setLogsPanel(function () {
        return {
          isOpen: true,
          nodeId: nodeId,
          taskId: nodeStatus.task_id,
          state: nodeStatus.state || "",
          tryNumber: nodeStatus.try_number || 1,
          startDate: nodeStatus.start_date || "",
          endDate: nodeStatus.end_date || "",
          content: "",
          isLoading: true,
          error: "",
          lastFetchedAt: null,
        };
      });

      try {
        const logData = await fetchTaskLogs(
          activeRun.dag_id,
          activeRun.dag_run_id,
          nodeStatus.task_id,
          nodeStatus.try_number || 1
        );
        setLogsPanel(function (prev) {
          if (!prev.isOpen || prev.nodeId !== nodeId) {
            return prev;
          }
          return Object.assign({}, prev, {
            content: String(logData.content || ""),
            isLoading: false,
            error: "",
            lastFetchedAt: Date.now(),
          });
        });
      } catch (error) {
        setLogsPanel(function (prev) {
          if (!prev.isOpen || prev.nodeId !== nodeId) {
            return prev;
          }
          return Object.assign({}, prev, {
            isLoading: false,
            error: error && error.message ? error.message : "Unable to load logs.",
            lastFetchedAt: Date.now(),
          });
        });
      }
    }

    async function refreshCurrentLogs() {
      if (!logsPanel.isOpen || !activeRun || !activeRun.dag_id || !activeRun.dag_run_id || !logsPanel.taskId) {
        return;
      }

      setLogsPanel(function (prev) {
        return Object.assign({}, prev, { isLoading: true, error: "" });
      });

      try {
        const logData = await fetchTaskLogs(
          activeRun.dag_id,
          activeRun.dag_run_id,
          logsPanel.taskId,
          logsPanel.tryNumber || 1
        );
        setLogsPanel(function (prev) {
          if (!prev.isOpen) {
            return prev;
          }
          return Object.assign({}, prev, {
            content: String(logData.content || ""),
            isLoading: false,
            error: "",
            lastFetchedAt: Date.now(),
          });
        });
      } catch (error) {
        setLogsPanel(function (prev) {
          if (!prev.isOpen) {
            return prev;
          }
          return Object.assign({}, prev, {
            isLoading: false,
            error: error && error.message ? error.message : "Unable to refresh logs.",
            lastFetchedAt: Date.now(),
          });
        });
      }
    }

    useEffect(
      function () {
        if (dagRunState !== "waiting_airflow" || !dagRunMeta || !dagRunMeta.dag_id) {
          return;
        }

        let cancelled = false;
        const startedAt = Date.now();
        const timeoutSeconds = Math.max(30, Number(readinessPollConfig.timeoutSeconds) || 600);
        const intervalSeconds = Math.max(2, Number(readinessPollConfig.intervalSeconds) || 5);
        const timeoutMs = timeoutSeconds * 1000;
        const pollMs = intervalSeconds * 1000;

        async function pollOnce() {
          if (cancelled) {
            return;
          }
          try {
            const readiness = await checkDagReadiness(dagRunMeta);
            if (cancelled) {
              return;
            }
            if (readiness.ready) {
              setDagLifecycleState("ready_to_run");
              if (autoRunAfterReadiness) {
                setStatusKind("status-warn");
                setStatusText("DAG is ready in Airflow. Triggering run...");
                setAutoRunAfterReadiness(false);
                await triggerDagRunForDagId(dagRunMeta.dag_id, autoRunConf);
                setAutoRunConf({});
              } else {
                setStatusKind("status-ok");
                setStatusText("DAG is ready in Airflow.");
              }
              return;
            }
            if (Date.now() - startedAt >= timeoutMs) {
              setDagLifecycleState("run_failed", "Run failed");
              setStatusKind("status-error");
              setStatusText("DAG did not become ready in Airflow before timeout.");
              setAutoRunAfterReadiness(false);
              setAutoRunConf({});
              return;
            }
            setDagLifecycleState("waiting_airflow");
            setStatusKind("status-warn");
            setStatusText("Waiting for Airflow DAG registration...");
          } catch (error) {
            if (cancelled) {
              return;
            }
            if (Date.now() - startedAt >= timeoutMs) {
              setDagLifecycleState("run_failed", "Run failed");
              setStatusKind("status-error");
              setStatusText(
                "Airflow readiness checks failed before timeout: " +
                  (error && error.message ? error.message : "Unknown error.")
              );
              setAutoRunAfterReadiness(false);
              setAutoRunConf({});
            } else {
              setStatusKind("status-warn");
              setStatusText(
                "Airflow readiness check issue, retrying: " +
                  (error && error.message ? error.message : "Unknown error.")
              );
            }
          }
        }

        pollOnce();
        const timer = window.setInterval(pollOnce, pollMs);

        return function () {
          cancelled = true;
          window.clearInterval(timer);
        };
      },
      [dagRunState, dagRunMeta, readinessPollConfig, autoRunAfterReadiness, autoRunConf]
    );

    useEffect(
      function () {
        if (!activeRun || !activeRun.dag_id || !activeRun.dag_run_id) {
          return;
        }

        let cancelled = false;
        const startedAt = Date.now();
        const timeoutSeconds = Math.max(60, Number(runPollConfig.timeoutSeconds) || 1800);
        const intervalSeconds = Math.max(2, Number(runPollConfig.intervalSeconds) || 5);
        const timeoutMs = timeoutSeconds * 1000;
        const pollMs = intervalSeconds * 1000;

        async function pollRun() {
          if (cancelled) {
            return;
          }

          try {
            const statusData = await fetchRunStatus(activeRun.dag_id, activeRun.dag_run_id);
            const taskList = await fetchRunTaskInstances(activeRun.dag_id, activeRun.dag_run_id);
            if (cancelled) {
              return;
            }

            setActiveRun(function (prev) {
              if (!prev || prev.dag_run_id !== statusData.dag_run_id || prev.dag_id !== statusData.dag_id) {
                return prev;
              }
              return Object.assign({}, prev, {
                state: statusData.state || prev.state,
                start_date: statusData.start_date || prev.start_date || "",
                end_date: statusData.end_date || prev.end_date || "",
              });
            });

            setTaskInstances(taskList);
            const statusByTaskId = {};
            taskList.forEach(function (task) {
              const taskId = String(task.task_id || "").trim();
              if (!taskId) {
                return;
              }
              statusByTaskId[taskId] = {
                task_id: taskId,
                state: task.state || "",
                try_number: task.try_number || 1,
                start_date: task.start_date || "",
                end_date: task.end_date || "",
              };
            });
            setTaskStatusByTaskId(statusByTaskId);

            const runState = normalizeTaskState(statusData.state);
            if (runState === "success") {
              setDagLifecycleState("run_started", "Run succeeded");
              setStatusKind("status-ok");
              setStatusText("DAG run completed successfully.");
              return true;
            }
            if (runState === "failed" || runState === "canceled" || runState === "cancelled") {
              setDagLifecycleState("run_failed", "Run failed");
              setStatusKind("status-error");
              setStatusText("DAG run ended with state: " + String(statusData.state || "failed"));
              return true;
            }

            const retryingNow = hasRetryingTasks(taskList);
            if (retryingNow) {
              setDagLifecycleState("retrying", "Retrying");
            } else {
              setDagLifecycleState("running", "Running (" + (statusData.state || "unknown") + ")");
            }

            if (Date.now() - startedAt >= timeoutMs) {
              setStatusKind("status-warn");
              setStatusText("Run monitoring timed out. You can keep checking logs manually.");
              return true;
            }
          } catch (error) {
            if (cancelled) {
              return false;
            }
            setStatusKind("status-warn");
            setStatusText(
              "Run monitoring issue: " + (error && error.message ? error.message : "Unknown error")
            );
          }
          return false;
        }

        let timer = null;
        pollRun().then(function (done) {
          if (!done && !cancelled) {
            timer = window.setInterval(function () {
              pollRun().then(function (finished) {
                if (finished && timer) {
                  window.clearInterval(timer);
                  timer = null;
                }
              });
            }, pollMs);
          }
        });

        return function () {
          cancelled = true;
          if (timer) {
            window.clearInterval(timer);
          }
        };
      },
      [activeRun ? activeRun.dag_id + ":" + activeRun.dag_run_id : "", runPollConfig]
    );

    useEffect(
      function () {
        if (!logsPanel.isOpen || !logsPanel.nodeId) {
          return;
        }
        const nodeStatus = nodeRunStatusMap[logsPanel.nodeId];
        if (!nodeStatus || !nodeStatus.has_task_instance) {
          return;
        }
        if (
          logsPanel.taskId !== nodeStatus.task_id ||
          Number(logsPanel.tryNumber || 1) !== Number(nodeStatus.try_number || 1) ||
          logsPanel.state !== (nodeStatus.state || "")
        ) {
          setLogsPanel(function (prev) {
            if (!prev.isOpen || prev.nodeId !== logsPanel.nodeId) {
              return prev;
            }
            return Object.assign({}, prev, {
              taskId: nodeStatus.task_id || prev.taskId,
              tryNumber: nodeStatus.try_number || prev.tryNumber || 1,
              state: nodeStatus.state || "",
              startDate: nodeStatus.start_date || "",
              endDate: nodeStatus.end_date || "",
            });
          });
        }
      },
      [logsPanel.isOpen, logsPanel.nodeId, logsPanel.taskId, logsPanel.tryNumber, logsPanel.state, nodeRunStatusMap]
    );

    useEffect(
      function () {
        if (!logsPanel.isOpen) {
          return;
        }
        const state = normalizeTaskState(logsPanel.state);
        if (state !== "running" && state !== "queued" && state !== "up_for_retry" && state !== "retrying") {
          return;
        }
        const timer = window.setInterval(function () {
          refreshCurrentLogs();
        }, 5000);
        return function () {
          window.clearInterval(timer);
        };
      },
      [logsPanel.isOpen, logsPanel.state, logsPanel.taskId, logsPanel.tryNumber, activeRun ? activeRun.dag_run_id : ""]
    );

    async function handleValidate() {
      if (!canValidatePipeline) {
        warnViewerReadOnly("validate pipelines");
        return;
      }
      setStatusKind("status-warn");
      setStatusText("Validating pipeline...");
      try {
        const resp = await fetchApi("/api/validate-pipeline", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(buildPayload()),
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          const msg = (data && (data.error || (data.errors || []).join(" | "))) || "Validation failed.";
          setStatusKind("status-error");
          setStatusText(msg);
          return;
        }

        setExecutionOrder(data.execution_order || []);
        if (data.valid) {
          setStatusKind("status-ok");
          setStatusText("Validation successful. Execution order: " + (data.execution_order || []).join(" -> "));
        } else {
          setStatusKind("status-error");
          setStatusText("Validation errors: " + (data.errors || []).join(" | "));
        }
      } catch (error) {
        setStatusKind("status-error");
        setStatusText("Validation request failed: " + (error && error.message ? error.message : "Unknown error."));
      }
    }

    async function handleGenerate() {
      if (!canGenerateDag) {
        warnViewerReadOnly("generate DAGs");
        return;
      }
      if (!nodes.length) {
        setStatusKind("status-warn");
        setStatusText("Add at least one module before generating a DAG.");
        return;
      }

      setDagLifecycleState("generating");
      setAutoRunAfterReadiness(false);
      setAutoRunConf({});
      setStatusKind("status-warn");
      setStatusText("Generating DAG...");
      try {
        const payload = buildPayload();
        const resp = await fetchApi("/api/generate-dag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          setStatusKind("status-error");
          const errors = data && data.errors && data.errors.length ? data.errors.join(" | ") : data && data.error;
          setStatusText(errors || "DAG generation failed.");
          setDagLifecycleState("run_failed", "Run failed");
          return;
        }
        setExecutionOrder(data.execution_order || []);
        setDagPreview(data.dag_preview || "");
        setShowPreview(true);
        setLastGeneratedAt(Date.now());
        setLastGeneratedPayloadSignature(createPayloadSignature(payload));
        setNodeTaskMap(ensureNodeTaskMapShape(data.node_task_map));
        setNodePrimaryTaskMap(ensureNodePrimaryTaskMapShape(data.node_primary_task_map));
        setActiveRun(null);
        setTaskInstances([]);
        setTaskStatusByTaskId({});
        setNodeRetryingById({});
        setLogsPanel({
          isOpen: false,
          nodeId: null,
          taskId: "",
          state: "",
          tryNumber: 1,
          startDate: "",
          endDate: "",
          content: "",
          isLoading: false,
          error: "",
          lastFetchedAt: null,
        });

        const readinessMeta = {
          dag_id: data.dag_id || pipeline.dag_id,
          airflow_path: data.airflow_path || "",
        };
        const skipRegistrationWait = data.registration_wait_required === false;
        setDagRunMeta(readinessMeta);

        if (data.version_reused === true || data.version_updated_in_place === true || skipRegistrationWait) {
          setDagLifecycleState("ready_to_run");
        } else if (data.readiness && data.readiness.ready) {
          setDagLifecycleState("ready_to_run");
        } else {
          setDagLifecycleState("waiting_airflow");
        }

        setReadinessPollConfig({
          timeoutSeconds: Number(data.readiness_poll_timeout_seconds) || 600,
          intervalSeconds: Number(data.readiness_poll_interval_seconds) || 5,
        });

        if (data.version_reused === true) {
          setStatusKind("status-ok");
          setStatusText(
            "DAG reused existing version (" +
              (data.version_id || "-") +
              "). Ready to run without registration wait."
          );
        } else if (data.version_updated_in_place === true) {
          setStatusKind("status-ok");
          setStatusText(
            "Current DAG version (" +
              (data.version_id || "-") +
              ") updated in place. Ready to run without registration wait."
          );
        } else if (skipRegistrationWait) {
          setStatusKind("status-ok");
          setStatusText("DAG updated in existing output file. Ready to run without registration wait.");
        } else if (data.warnings && data.warnings.length > 0) {
          setStatusKind("status-warn");
          setStatusText(
            "DAG generated: " +
              data.generated_filename +
              ". Warning: " +
              data.warnings.join(" | ")
          );
        } else {
          setStatusKind("status-ok");
          setStatusText(
            "DAG generated and copied successfully. Local: " + data.local_path + " | Airflow: " + data.airflow_path
          );
        }
        await refreshPipelineVersions(pipeline.dag_id, data.version_id || "");
      } catch (error) {
        setStatusKind("status-error");
        setStatusText("Generate request failed: " + (error && error.message ? error.message : "Unknown error."));
        setDagLifecycleState("run_failed", "Run failed");
      }
    }

    async function triggerDagRunForDagId(dagId, runConf) {
      const safeDagId = String(dagId || "").trim();
      const safeRunConf = runConf && typeof runConf === "object" ? runConf : {};
      if (!safeDagId) {
        setDagLifecycleState("run_failed", "Run failed");
        setStatusKind("status-error");
        setStatusText("Unable to run DAG because dag_id is missing.");
        setActiveRun(null);
        return false;
      }

      setDagLifecycleState("running");
      setStatusKind("status-warn");
      setStatusText("Triggering DAG run...");
      setTaskInstances([]);
      setTaskStatusByTaskId({});
      setNodeRetryingById({});
      setLogsPanel({
        isOpen: false,
        nodeId: null,
        taskId: "",
        state: "",
        tryNumber: 1,
        startDate: "",
        endDate: "",
        content: "",
        isLoading: false,
        error: "",
        lastFetchedAt: null,
      });

      try {
        const resp = await fetchApi("/api/run-dag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            dag_id: safeDagId,
            conf: safeRunConf,
          }),
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          const errorMsg = (data && data.error) || "Run trigger failed.";
          setDagLifecycleState("run_failed", "Run failed");
          setStatusKind("status-error");
          setStatusText(errorMsg);
          setActiveRun(null);
          return false;
        }

        const runId = data.dag_run_id ? " (dag_run_id: " + data.dag_run_id + ")" : "";
        setDagLifecycleState("running", "Running");
        setRunPollConfig({
          timeoutSeconds: Number(data.run_poll_timeout_seconds) || 1800,
          intervalSeconds: Number(data.run_poll_interval_seconds) || 5,
        });
        setActiveRun({
          dag_id: data.dag_id || safeDagId,
          dag_run_id: data.dag_run_id || "",
          state: data.state || "queued",
          start_date: data.start_date || "",
          end_date: data.end_date || "",
          started_at: Date.now(),
        });
        setStatusKind("status-ok");
        if (!data.dag_run_id) {
          setStatusKind("status-warn");
          setStatusText("DAG run was triggered but dag_run_id is missing; live monitoring is unavailable.");
        } else {
          setStatusText("DAG run started for " + (data.dag_id || safeDagId) + runId);
        }
        return true;
      } catch (error) {
        setDagLifecycleState("run_failed", "Run failed");
        setStatusKind("status-error");
        setStatusText("Run DAG request failed: " + (error && error.message ? error.message : "Unknown error."));
        setActiveRun(null);
        return false;
      }
    }

    async function handleRunPipeline() {
      if (!canGenerateDag || !canRunDag) {
        warnViewerReadOnly("generate and run DAGs");
        return;
      }
      if (!nodes.length) {
        setStatusKind("status-warn");
        setStatusText("Add at least one module before running.");
        return;
      }

      setAutoRunAfterReadiness(true);
      setAutoRunConf({});
      setDagLifecycleState("generating");
      setStatusKind("status-warn");
      setStatusText("Generating DAG...");

      try {
        const payload = buildPayload();
        const resp = await fetchApi("/api/generate-dag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          setStatusKind("status-error");
          const errors = data && data.errors && data.errors.length ? data.errors.join(" | ") : data && data.error;
          setStatusText(errors || "DAG generation failed.");
          setDagLifecycleState("run_failed", "Run failed");
          setAutoRunAfterReadiness(false);
          setAutoRunConf({});
          return;
        }

        setExecutionOrder(data.execution_order || []);
        setDagPreview(data.dag_preview || "");
        setShowPreview(true);
        setLastGeneratedAt(Date.now());
        setLastGeneratedPayloadSignature(createPayloadSignature(payload));
        setNodeTaskMap(ensureNodeTaskMapShape(data.node_task_map));
        setNodePrimaryTaskMap(ensureNodePrimaryTaskMapShape(data.node_primary_task_map));
        setActiveRun(null);
        setTaskInstances([]);
        setTaskStatusByTaskId({});
        setNodeRetryingById({});
        setLogsPanel({
          isOpen: false,
          nodeId: null,
          taskId: "",
          state: "",
          tryNumber: 1,
          startDate: "",
          endDate: "",
          content: "",
          isLoading: false,
          error: "",
          lastFetchedAt: null,
        });

        const readinessMeta = {
          dag_id: data.dag_id || pipeline.dag_id,
          airflow_path: data.airflow_path || "",
        };
        const skipRegistrationWait = data.registration_wait_required === false;
        setDagRunMeta(readinessMeta);
        setReadinessPollConfig({
          timeoutSeconds: Number(data.readiness_poll_timeout_seconds) || 600,
          intervalSeconds: Number(data.readiness_poll_interval_seconds) || 5,
        });

        if (data.version_reused === true || data.version_updated_in_place === true || skipRegistrationWait) {
          setDagLifecycleState("ready_to_run");
          setStatusKind("status-warn");
          setStatusText(
            data.version_reused === true
              ? "DAG version reused. Triggering run..."
              : data.version_updated_in_place === true
              ? "Current DAG version updated. Triggering run..."
              : "DAG output file already exists in Airflow. Triggering run..."
          );
          setAutoRunAfterReadiness(false);
          setAutoRunConf({});
          await triggerDagRunForDagId(readinessMeta.dag_id);
          return;
        }

        if (data.readiness && data.readiness.ready) {
          setDagLifecycleState("ready_to_run");
          setStatusKind("status-warn");
          setStatusText("DAG is ready in Airflow. Triggering run...");
          setAutoRunAfterReadiness(false);
          setAutoRunConf({});
          await triggerDagRunForDagId(readinessMeta.dag_id);
          return;
        }

        setDagLifecycleState("waiting_airflow");
        if (data.warnings && data.warnings.length > 0) {
          setStatusKind("status-warn");
          setStatusText(
            "DAG generated. Waiting for Airflow registration. Warnings: " +
              data.warnings.join(" | ")
          );
        } else {
          setStatusKind("status-warn");
          setStatusText("DAG generated. Waiting for Airflow registration...");
        }
        await refreshPipelineVersions(pipeline.dag_id, data.version_id || "");
      } catch (error) {
        setAutoRunAfterReadiness(false);
        setAutoRunConf({});
        setStatusKind("status-error");
        setStatusText("Run request failed: " + (error && error.message ? error.message : "Unknown error."));
        setDagLifecycleState("run_failed", "Run failed");
      }
    }

    async function handleRetryDag() {
      if (!canRetryDag) {
        warnViewerReadOnly("retry DAGs");
        return;
      }
      if (!dagRunMeta || !dagRunMeta.dag_id) {
        setStatusKind("status-warn");
        setStatusText("Generate a DAG and wait until it is ready before retrying.");
        return;
      }

      const proceed = window.confirm(
        "This will trigger a new DAG run while the current run is in retry state. Continue?"
      );
      if (!proceed) {
        return;
      }

      await triggerDagRunForDagId(dagRunMeta && dagRunMeta.dag_id);
    }

    async function handleStopRetry() {
      if (!canRetryDag) {
        warnViewerReadOnly("stop DAG retries");
        return;
      }
      if (!activeRun || !activeRun.dag_id || !activeRun.dag_run_id) {
        setStatusKind("status-warn");
        setStatusText("No active DAG run to stop.");
        return;
      }
      const proceed = window.confirm(
        "Stop current DAG run " + activeRun.dag_run_id + " ? This will mark it failed in Airflow."
      );
      if (!proceed) {
        return;
      }
      try {
        const resp = await fetchApi(
          "/api/runs/" +
            encodeURIComponent(activeRun.dag_id) +
            "/" +
            encodeURIComponent(activeRun.dag_run_id) +
            "/stop",
          {
            method: "POST",
          }
        );
        const data = await parseApiJson(resp);
        if (!resp.ok || !data.success) {
          throw new Error((data && data.error) || "Failed to stop DAG run.");
        }
        setDagLifecycleState("run_failed", "Stopped");
        setStatusKind("status-warn");
        setStatusText("DAG run stopped: " + activeRun.dag_run_id);
        setActiveRun(function (prev) {
          if (!prev) {
            return prev;
          }
          return Object.assign({}, prev, {
            state: data.state || "failed",
            end_date: new Date().toISOString(),
          });
        });
      } catch (error) {
        setStatusKind("status-error");
        setStatusText("Stop run failed: " + (error && error.message ? error.message : "Unknown error."));
      }
    }

    async function handleRetryNode(nodeId) {
      if (!canRetryNodeActions) {
        warnViewerReadOnly("retry modules");
        return;
      }
      if (!activeRun || !activeRun.dag_id || !activeRun.dag_run_id) {
        setStatusKind("status-warn");
        setStatusText("Start a DAG run before retrying a module.");
        return;
      }

      const nodeStatus = nodeRunStatusMap[nodeId];
      if (!nodeStatus || !nodeStatus.has_task_instance || !nodeStatus.task_id) {
        setStatusKind("status-warn");
        setStatusText("No Airflow task instance found for this module yet.");
        return;
      }

      const normalizedState = normalizeTaskState(nodeStatus.state);
      if (
        normalizedState !== "failed" &&
        normalizedState !== "up_for_retry" &&
        normalizedState !== "retrying" &&
        normalizedState !== "retry"
      ) {
        setStatusKind("status-warn");
        setStatusText("Retry is available only for failed or retrying modules.");
        return;
      }

      if (nodeRetryingById[nodeId]) {
        return;
      }

      const nodeLabel =
        (nodes.find(function (node) {
          return node.id === nodeId;
        }) || {}).label || nodeId;
      const currentPayload = buildPayload();
      const currentPayloadSignature = createPayloadSignature(currentPayload);
      const hasConfigChangedSinceGeneration =
        Boolean(lastGeneratedPayloadSignature) &&
        Boolean(currentPayloadSignature) &&
        currentPayloadSignature !== lastGeneratedPayloadSignature;

      setNodeRetryingById(function (prev) {
        return Object.assign({}, prev, { [nodeId]: true });
      });
      if (hasConfigChangedSinceGeneration) {
        if (!canGenerateDag) {
          setStatusKind("status-warn");
          setStatusText(
            "Pipeline config changed since last generation. You need DAG generate permission to apply new config before retry."
          );
          return;
        }

        const proceedWithNewConfig = window.confirm(
          "Pipeline configuration has changed since the last DAG generation. Generate updated DAG and retry this module in the same run?"
        );
        if (!proceedWithNewConfig) {
          return;
        }

        setDagLifecycleState("generating");
        setStatusKind("status-warn");
        setStatusText("Configuration changed. Generating updated DAG before retry...");

        const generateResp = await fetchApi("/api/generate-dag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(currentPayload),
        });
        const generateData = await parseApiJson(generateResp);
        if (!generateResp.ok || !generateData.success) {
          const errors =
            generateData && generateData.errors && generateData.errors.length
              ? generateData.errors.join(" | ")
              : generateData && generateData.error;
          setStatusKind("status-error");
          setStatusText(errors || "DAG generation failed.");
          setDagLifecycleState("run_failed", "Run failed");
          return;
        }

        setExecutionOrder(generateData.execution_order || []);
        setDagPreview(generateData.dag_preview || "");
        setShowPreview(true);
        setLastGeneratedAt(Date.now());
        setLastGeneratedPayloadSignature(currentPayloadSignature);
        setNodeTaskMap(ensureNodeTaskMapShape(generateData.node_task_map));
        setNodePrimaryTaskMap(ensureNodePrimaryTaskMapShape(generateData.node_primary_task_map));
        setDagRunMeta({
          dag_id: generateData.dag_id || pipeline.dag_id,
          airflow_path: generateData.airflow_path || "",
        });
        setReadinessPollConfig({
          timeoutSeconds: Number(generateData.readiness_poll_timeout_seconds) || 600,
          intervalSeconds: Number(generateData.readiness_poll_interval_seconds) || 5,
        });
      }

      setDagLifecycleState("retrying", "Retrying");
      setStatusKind("status-warn");
      setStatusText("Retrying module '" + nodeLabel + "'...");

      try {
        const retryData = await triggerTaskRetry(activeRun.dag_id, activeRun.dag_run_id, nodeStatus.task_id);
        const statusData = await fetchRunStatus(activeRun.dag_id, activeRun.dag_run_id);
        const taskList = await fetchRunTaskInstances(activeRun.dag_id, activeRun.dag_run_id);

        setActiveRun(function (prev) {
          if (!prev || prev.dag_run_id !== statusData.dag_run_id || prev.dag_id !== statusData.dag_id) {
            return prev;
          }
          return Object.assign({}, prev, {
            state: statusData.state || prev.state,
            start_date: statusData.start_date || prev.start_date || "",
            end_date: statusData.end_date || prev.end_date || "",
          });
        });

        setTaskInstances(taskList);
        const statusByTaskId = {};
        taskList.forEach(function (task) {
          const taskId = String(task.task_id || "").trim();
          if (!taskId) {
            return;
          }
          statusByTaskId[taskId] = {
            task_id: taskId,
            state: task.state || "",
            try_number: task.try_number || 1,
            start_date: task.start_date || "",
            end_date: task.end_date || "",
          };
        });
        setTaskStatusByTaskId(statusByTaskId);

        setStatusKind("status-ok");
        setStatusText(
          "Retry requested for '" +
            nodeLabel +
            "' (task_id: " +
            nodeStatus.task_id +
            ", strategy: " +
            (retryData.strategy || "airflow") +
            ")."
        );
      } catch (error) {
        setStatusKind("status-error");
        setStatusText("Node retry failed: " + (error && error.message ? error.message : "Unknown error."));
      } finally {
        setNodeRetryingById(function (prev) {
          const next = Object.assign({}, prev);
          delete next[nodeId];
          return next;
        });
      }
    }

    async function handleSavePipeline() {
      if (!canSavePipeline) {
        warnViewerReadOnly("save pipeline JSON");
        return;
      }
      const suggested = pipeline.dag_id ? pipeline.dag_id + ".json" : "pipeline.json";
      const fileName = window.prompt("Filename to save pipeline JSON:", suggested);
      if (!fileName) {
        return;
      }
      try {
        const resp = await fetchApi("/api/save-pipeline", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ filename: fileName, pipeline_data: buildPayload() }),
        });
        const data = await resp.json();
        if (!data.success) {
          setSaveState("failed");
          setStatusKind("status-error");
          setStatusText(data.error || "Failed to save pipeline.");
          return;
        }
        await refreshSavedFiles();
        setSelectedSavedFile(data.filename);
        setIsDirty(false);
        setSaveState("saved");
        setStatusKind("status-ok");
        setStatusText("Saved pipeline to " + data.filename);
      } catch (error) {
        setSaveState("failed");
        setStatusKind("status-error");
        setStatusText("Save request failed: " + (error && error.message ? error.message : "Unknown error."));
      }
    }

    async function handleLoadPipeline() {
      if (!canLoadPipelines) {
        warnViewerReadOnly("load saved pipelines");
        return;
      }
      if (!selectedSavedFile) {
        setStatusKind("status-warn");
        setStatusText("Select a saved pipeline file first.");
        return;
      }
      const resp = await fetchApi("/api/load-pipeline", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ filename: selectedSavedFile }),
      });
      const data = await resp.json();
      if (!data.success) {
        setStatusKind("status-error");
        setStatusText(data.error || "Failed to load pipeline.");
        return;
      }

      const loaded = data.pipeline_data || {};
      const loadedPipeline = hydrateScheduleForUi(loaded.pipeline || defaultPipeline);
      const loadedNodes = (loaded.nodes || []).map(function (node) {
        const normalizedConfig = Object.assign({}, node.config || {});
        let normalizedType = node.type;

        // Backward compatibility: old saved pipelines stored SFTP as one type + action.
        if (String(node.type || "").toLowerCase() === "sftp") {
          const action = String(normalizedConfig.action || "upload").toLowerCase();
          normalizedType = action === "sensor" ? "sftp_sensor" : "sftp_upload";
          delete normalizedConfig.action;
        }

        if (normalizedType === "dataform" && !normalizedConfig.project_mode) {
          normalizedConfig.project_mode = "single";
        }
        const logoFromType =
          window.PipelineBuilderModules && window.PipelineBuilderModules.getLogoForType
            ? window.PipelineBuilderModules.getLogoForType(normalizedType)
            : "";
        return Object.assign({}, node, {
          type: normalizedType,
          config: normalizedConfig,
          logo: logoFromType,
        });
      });
      const loadedEdges = loaded.edges || [];
      const rawGroupBoxes = Array.isArray(loaded.group_boxes)
        ? loaded.group_boxes
        : Array.isArray(loaded.groupBoxes)
        ? loaded.groupBoxes
        : [];
      const loadedGroupBoxes = rawGroupBoxes.map(function (box, idx) {
            const parsedX = Number(box.x);
            const parsedY = Number(box.y);
            const parsedWidth = Number(box.width);
            const parsedHeight = Number(box.height);
            return {
              id: String(box.id || "group_box_" + (idx + 1)),
              title: String(box.title || "Group " + (idx + 1)),
              x: Number.isFinite(parsedX) ? parsedX : 100,
              y: Number.isFinite(parsedY) ? parsedY : 100,
              width: Math.max(180, Number.isFinite(parsedWidth) ? parsedWidth : 380),
              height: Math.max(120, Number.isFinite(parsedHeight) ? parsedHeight : 220),
              color: String(box.color || "#2f6ea2"),
            };
          });
      const hasAnyInvalidPosition = loadedNodes.some(function (node) {
        return !hasValidCanvasPosition(node);
      });
      const layoutPositions = hasAnyInvalidPosition ? buildAutoLayoutCoordinates(loadedNodes, loadedEdges) : null;
      const normalizedNodes = loadedNodes.map(function (node) {
        const layoutPos = layoutPositions ? layoutPositions[node.id] : null;
        const safeX = hasValidCanvasPosition(node) ? Number(node.x) : layoutPos ? layoutPos.x : 40;
        const safeY = hasValidCanvasPosition(node) ? Number(node.y) : layoutPos ? layoutPos.y : 40;
        return Object.assign({}, node, { x: safeX, y: safeY });
      });

      forceLatestOnNextVersionRefreshRef.current = true;
      suppressPipelineIdVersionEffectRef.current = true;
      setSelectedVersionId("");
      setSelectedVersionSummary(null);
      setVersionSummaryError("");
      setPipeline(loadedPipeline);
      setNodes([]);
      setEdges([]);
      setGroupBoxes([]);
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedGroupBoxId(null);
      setSelectedEdge(null);
      setConnectingFromId(null);
      setExecutionOrder([]);
      setStatusKind("status-ok");
      setStatusText("Loaded pipeline from " + data.filename + ". Loading latest version...");
      setNextSeq(1);
      setNextGroupSeq(1);
      setIsDirty(false);
      setSaveState("saved");
      setLastGeneratedAt(null);
      setLastGeneratedPayloadSignature("");
      setDagRunMeta(null);
      setAutoRunAfterReadiness(false);
      setAutoRunConf({});
      setDagLifecycleState("not_generated");
      setNodeTaskMap({});
      setNodePrimaryTaskMap({});
      setActiveRun(null);
      setTaskInstances([]);
      setTaskStatusByTaskId({});
      setNodeRetryingById({});
      setLogsPanel({
        isOpen: false,
        nodeId: null,
        taskId: "",
        state: "",
        tryNumber: 1,
        startDate: "",
        endDate: "",
        content: "",
        isLoading: false,
        error: "",
        lastFetchedAt: null,
      });

      try {
        const versionResult = await refreshPipelineVersions(loadedPipeline.dag_id || "", "", true);
        const selectedVersionFromResult = String((versionResult && versionResult.selectedVersionId) || "").trim();
        if (selectedVersionFromResult) {
          await refreshSelectedVersionSummary(selectedVersionFromResult, loadedPipeline.dag_id || "");
          setStatusKind("status-ok");
          setStatusText("Loaded latest DAG version from " + data.filename + ".");
        } else {
          setNodes(normalizedNodes);
          setEdges(loadedEdges);
          setGroupBoxes(loadedGroupBoxes);
          setNextSeq(computeNextNodeSequence(normalizedNodes));
          setNextGroupSeq(computeNextGroupSequence(loadedGroupBoxes));
          const uniqueTypes = Array.from(
            new Set(
              loadedNodes.map(function (node) {
                return node.type;
              })
            )
          );
          for (let i = 0; i < uniqueTypes.length; i += 1) {
            await ensureSchema(uniqueTypes[i]);
          }
          setStatusKind("status-ok");
          setStatusText("Loaded pipeline from " + data.filename);
        }
      } finally {
        suppressPipelineIdVersionEffectRef.current = false;
      }
    }

    function handleNewPipeline() {
      if (!canCreateNewPipeline) {
        warnViewerReadOnly("create new pipelines");
        return;
      }
      setPipeline(hydrateScheduleForUi(defaultPipeline));
      setNodes([]);
      setEdges([]);
      setGroupBoxes([]);
      setExecutionOrder([]);
      setDagPreview("");
      setShowPreview(false);
      setSelectedNodeId(null);
      setSelectedNodeIds([]);
      setSelectedGroupBoxId(null);
      setSelectedEdge(null);
      setConnectingFromId(null);
      setNextSeq(1);
      setNextGroupSeq(1);
      setIsDirty(false);
      setSaveState("saved");
      setLastGeneratedAt(null);
      setLastGeneratedPayloadSignature("");
      setDagRunMeta(null);
      setAutoRunAfterReadiness(false);
      setAutoRunConf({});
      setDagLifecycleState("not_generated");
      setNodeTaskMap({});
      setNodePrimaryTaskMap({});
      setActiveRun(null);
      setTaskInstances([]);
      setTaskStatusByTaskId({});
      setNodeRetryingById({});
      setLogsPanel({
        isOpen: false,
        nodeId: null,
        taskId: "",
        state: "",
        tryNumber: 1,
        startDate: "",
        endDate: "",
        content: "",
        isLoading: false,
        error: "",
        lastFetchedAt: null,
      });
      setStatusKind("status-ok");
      setStatusText("Started a new pipeline.");
      refreshPipelineVersions(defaultPipeline.dag_id || "");
    }

    function handleAutoLayout() {
      if (!canAutoLayout) {
        warnViewerReadOnly("auto-layout modules");
        return;
      }
      if (!nodes.length) {
        setStatusKind("status-warn");
        setStatusText("Add nodes first, then use auto-layout.");
        return;
      }

      const positionsById = buildAutoLayoutCoordinates(nodes, edges);

      setNodes(function (prev) {
        return prev.map(function (node) {
          const pos = positionsById[node.id];
          if (!pos) {
            return node;
          }
          return Object.assign({}, node, { x: pos.x, y: pos.y });
        });
      });
      setIsDirty(true);
      setSaveState("unsaved");
      setStatusKind("status-ok");
      setStatusText("Nodes auto-arranged (including parallel branches).");
    }

    return h(
      "div",
      { className: "app-shell" },
      h(
        "header",
        { className: "topbar" },
        h(
          "div",
          { className: "topbar-brand" },
          h(
            "div",
            { className: "topbar-title-row" },
            h("div", { className: "topbar-title" }, "Airflow Pipeline Builder"),
            h("span", { className: "topbar-version" }, "v1")
          ),
          h("div", { className: "topbar-subtitle" }, "Orchestration workspace")
        ),
        h(
          "div",
          { className: "topbar-main" },
          h(
            "div",
            {
              className: "topbar-context",
              "aria-live": "polite",
              tabIndex: 0,
            },
            h(
              "div",
              { className: "context-main" },
              h(
                "div",
                { className: "context-title-wrap" },
                h("span", { className: "context-icon", "aria-hidden": "true" }, h(PipelineContextIcon)),
                h("span", { className: "context-label" }, "Pipeline"),
                h("strong", { className: "context-value", title: pipelineTitle }, pipelineTitle)
              ),
              h(
                "span",
                {
                  className: saveStateClass,
                  title: pipelineStateTooltip,
                  role: "status",
                  "aria-label": "Save status: " + pipelineStateLabel + ". " + saveStateText,
                },
                h("span", { className: "save-status-dot-indicator", "aria-hidden": "true" }, ""),
                h("span", { className: "save-status-pill-text" }, pipelineStateLabel)
              )
            ),
            h(
              "div",
              { className: "context-meta-reveal" },
              h(
                "div",
                { className: "context-meta-row" },
                h("span", { className: "context-meta-label" }, "File"),
                h("span", { className: "context-meta-value context-meta-value-file", title: activeFileLabel }, activeFileLabel)
              ),
              h(
                "div",
                { className: "context-meta-row" },
                h("span", { className: "context-meta-label" }, "Last generated"),
                h("span", { className: "context-meta-value" }, generatedLabel)
              )
            )
          ),
          h(
            "div",
            { className: "topbar-primary-actions", role: "toolbar", "aria-label": "Primary pipeline actions" },
            h(
              "button",
              {
                className: "btn btn-primary btn-run-dag",
                onClick: handleRunPipeline,
                disabled:
                  !runButtonEnabled ||
                  dagRunState === "running" ||
                  dagRunState === "retrying" ||
                  dagRunState === "generating" ||
                  dagRunState === "waiting_airflow",
                "aria-label": "Run Pipeline",
              },
              dagRunState === "generating"
                ? "Generating..."
                : dagRunState === "waiting_airflow"
                ? "Registering..."
                : dagRunState === "retrying"
                ? "Retrying..."
                : dagRunState === "running"
                ? "Running..."
                : "Run Pipeline"
            ),
            dagRunState === "retrying" && activeRun && activeRun.dag_run_id
              ? h(
                  "button",
                  {
                    className: "btn btn-secondary",
                    onClick: handleStopRetry,
                    disabled: !canRetryDag,
                    "aria-label": "Stop Retry Run",
                  },
                  "Stop Retry"
                )
              : null,
            h("span", { className: dagRunStateClass }, dagRunStatusText),
            h("span", { className: "run-summary-chip" }, currentRunLabel)
          ),
          h(
            "div",
            { className: "topbar-secondary-actions", role: "toolbar", "aria-label": "Secondary actions" },
            h(
              "button",
              {
                className: "btn btn-secondary btn-versions-trigger",
                type: "button",
                title: "DAG Versions",
                onClick: function () {
                  closeHeaderDropdowns();
                  setShowDagVersions(true);
                },
              },
              "Versions"
            ),
            h(
              "details",
              { className: "toolbar-menu" },
              h(
                "summary",
                {
                  className: "btn btn-secondary menu-trigger",
                  role: "button",
                  "aria-haspopup": "menu",
                  "aria-label": "Open actions menu",
                },
                "Actions"
              ),
              h(
                "div",
                { className: "menu-panel", role: "menu" },
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleValidate();
                    },
                    disabled: !canValidatePipeline,
                  },
                  "Validate"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      setShowPreview(!showPreview);
                    },
                  },
                  showPreview ? "Hide Preview" : "Preview"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleRetryDag();
                    },
                    disabled: !canRetryNow || !canRetryDag,
                  },
                  "Retry DAG"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleStopRetry();
                    },
                    disabled: !(dagRunState === "retrying" && activeRun && activeRun.dag_run_id) || !canRetryDag,
                  },
                  "Stop Retry Run"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleAutoLayout();
                    },
                    disabled: !canAutoLayout,
                  },
                  "Auto Layout"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleAddGroupBox();
                    },
                    disabled: !canEditPipeline,
                  },
                  "Add Group Box"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      refreshSavedFiles();
                    },
                  },
                  "Refresh Files"
                ),
                h(
                  canViewConnections ? "a" : "span",
                  {
                    className: "menu-item",
                    href: canViewConnections ? "/airflow-connections" : undefined,
                    role: "menuitem",
                    onClick: canViewConnections ? closeHeaderDropdowns : undefined,
                  },
                  !canViewConnections
                    ? "Airflow Connections (No access)"
                    : (canManageConnections ? "Airflow Connections" : "Airflow Connections (Read-only)")
                ),
                h(
                  "a",
                  {
                    className: "menu-item",
                    href: "/requests",
                    role: "menuitem",
                    onClick: closeHeaderDropdowns,
                  },
                  "Requests"
                ),
                isAdminUser
                  ? h(
                      "a",
                      {
                        className: "menu-item",
                        href: "/admin-dashboard",
                        role: "menuitem",
                        onClick: closeHeaderDropdowns,
                      },
                      "Admin Dashboard"
                    )
                  : null,
                h(
                  "details",
                  { className: "menu-subgroup" },
                  h(
                    "summary",
                    {
                      className: "menu-item menu-subgroup-trigger",
                      role: "button",
                      "aria-haspopup": "menu",
                    },
                    "Releases"
                  ),
                  h(
                    "div",
                    { className: "menu-subgroup-list", role: "group", "aria-label": "Release notes" },
                    h(
                      "a",
                      {
                        className: "menu-item",
                        href: "/release-notes/v0.2",
                        role: "menuitem",
                        onClick: closeHeaderDropdowns,
                      },
                      "Release Notes v0.2"
                    ),
                    h(
                      "a",
                      {
                        className: "menu-item",
                        href: "/release-notes/v0.1",
                        role: "menuitem",
                        onClick: closeHeaderDropdowns,
                      },
                      "Release Notes v0.1"
                    ),
                    h(
                      "a",
                      {
                        className: "menu-item",
                        href: "/release-notes/v0",
                        role: "menuitem",
                        onClick: closeHeaderDropdowns,
                      },
                      "Release Notes v0"
                    )
                  )
                )
              )
            ),
            h(
              "details",
              { className: "toolbar-menu toolbar-menu-file" },
              h(
                "summary",
                {
                  className: "btn btn-secondary menu-trigger",
                  role: "button",
                  "aria-haspopup": "menu",
                  "aria-label": "Open file menu",
                },
                "File"
              ),
              h(
                "div",
                { className: "menu-panel", role: "menu" },
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleNewPipeline();
                    },
                    disabled: !canCreateNewPipeline,
                  },
                  "New"
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleSavePipeline();
                    },
                    disabled: !canSavePipeline,
                  },
                  "Save JSON"
                ),
                h("div", { className: "menu-divider", "aria-hidden": "true" }),
                h("div", { className: "menu-label" }, "Saved files"),
                h(
                  "select",
                  {
                    className: "file-select file-select-menu",
                    value: selectedSavedFile,
                    disabled: !canLoadPipelines,
                    "aria-label": "Saved pipeline files",
                    onChange: function (event) {
                      setSelectedSavedFile(event.target.value);
                      closeHeaderDropdowns();
                    },
                  },
                  h("option", { value: "" }, "Select file"),
                  savedFiles.map(function (file) {
                    return h("option", { key: file, value: file }, file);
                  })
                ),
                h(
                  "button",
                  {
                    className: "menu-item",
                    type: "button",
                    role: "menuitem",
                    onClick: function () {
                      closeHeaderDropdowns();
                      handleLoadPipeline();
                    },
                    disabled: !canLoadPipelines || !selectedSavedFile,
                  },
                  "Load"
                )
              )
            ),
            h(
              "a",
              {
                className: "btn btn-secondary logout-link",
                href: "/logout",
                "aria-label": "Logout",
              },
              "Logout"
            )
          )
        )
      ),
      h(
        "section",
        { className: "workspace" + (isSidebarOpen ? "" : " workspace--sidebar-collapsed") },
        h(
          "aside",
          { className: "panel panel-left" + (isSidebarOpen ? "" : " panel-left-collapsed") },
          h(
            "div",
            { className: "sidebar-panel-head" },
            h("h2", null, "Available Modules"),
            h(
              "button",
              {
                type: "button",
                className: "sidebar-toggle-btn",
                title: "Collapse modules panel",
                "aria-label": "Collapse modules panel",
                onClick: function () {
                  setIsSidebarOpen(false);
                },
              },
              h(SidebarCollapseIcon)
            )
          ),
          isSidebarOpen
            ? h(
                React.Fragment,
                null,
                h(
                  "div",
                  { className: "sidebar-toolbar" },
                  h("input", {
                    type: "text",
                    className: "module-search",
                    placeholder: "Search modules...",
                    value: moduleSearch,
                    onChange: function (event) {
                      setModuleSearch(event.target.value);
                    },
                  }),
                  h(
                    "small",
                    null,
                    canDragModules
                      ? "Drag modules to canvas. Use control-flow nodes for parallelism and branching."
                      : "Viewer mode: modules are visible, drag-and-drop is disabled."
                  )
                ),
                h(
                  "div",
                  { className: "sidebar-list" },
                  filteredPaletteItems.length === 0
                    ? h("div", { className: "module-empty" }, "No module matched your search.")
                    : filteredPaletteItems.map(function (item) {
                        return h(
                          "div",
                          {
                            key: item.palette_id,
                            className: "module-item" + (canDragModules ? "" : " module-item-readonly"),
                            draggable: canDragModules,
                            onDragStart: function (event) {
                              if (!canDragModules) {
                                event.preventDefault();
                                return;
                              }
                              event.dataTransfer.setData("application/x-palette-item", JSON.stringify(item));
                              event.dataTransfer.effectAllowed = "copy";
                            },
                          },
                          h(
                            "div",
                            { className: "module-row" },
                            item.logo
                              ? h("img", {
                                  className: "module-logo",
                                  src: item.logo,
                                  alt: item.display_name + " logo",
                                  draggable: false,
                                })
                              : null,
                            h("strong", null, item.display_name)
                          ),
                          h("small", null, item.description)
                        );
                      })
                )
              )
            : null
        ),
        h(
          "div",
          { className: "canvas-area" },
          !isSidebarOpen
            ? h(
                "button",
                {
                  type: "button",
                  className: "sidebar-reopen-btn",
                  title: "Expand modules panel",
                  "aria-label": "Expand modules panel",
                  onClick: function () {
                    setIsSidebarOpen(true);
                  },
                },
                h(SidebarExpandIcon)
              )
            : null,
          h(window.PipelineCanvas, {
            nodes: nodes,
            edges: edges,
            groupBoxes: groupBoxes,
            selectedNodeId: selectedNodeId,
            selectedNodeIds: selectedNodeIds,
            selectedGroupBoxId: selectedGroupBoxId,
            selectedEdge: selectedEdge,
            connectingFromId: connectingFromId,
            executionOrder: executionOrder,
            activeRun: activeRun,
            nodeRunStatusMap: nodeRunStatusMap,
            nodeRetryingById: nodeRetryingById,
            readOnly: !canEditPipeline,
            canRetryNodeAction: canRetryNodeActions,
            canViewLogs: canViewLogs,
            onSelectNode: handleNodeClick,
            onSelectGroupBox: handleSelectGroupBox,
            onDuplicateNode: handleDuplicateNode,
            onSelectEdge: handleSelectEdge,
            onClearSelection: handleCanvasClearSelection,
            onAddGroupBox: handleAddGroupBox,
            onMoveGroupBox: handleGroupBoxMoved,
            onResizeGroupBox: handleGroupBoxResized,
            onChangeGroupBox: handleGroupBoxChange,
            onDeleteGroupBox: handleDeleteGroupBox,
            onDeleteNode: handleDeleteNode,
            onConnectNodes: connectNodes,
            onConnectorClick: handleConnectorClick,
            onCanvasDrop: handleCanvasDrop,
            onNodeMoved: handleNodeMoved,
            onOpenNodeLogs: openNodeLogs,
            onRetryNode: handleRetryNode,
            onCanvasPointerWorldChange: handleCanvasPointerWorldChange,
          })
        ),
        h(
          "aside",
          { className: "panel panel-right" },
          h(window.PipelineForms.PipelineSettingsForm, {
            pipeline: pipeline,
            readOnly: !canEditPipeline,
            onChange: handlePipelineFieldChange,
          }),
          h(window.PipelineForms.ModuleConfigForm, {
            node: selectedNode,
            schema: selectedSchema,
            allNodes: nodes,
            allEdges: edges,
            pipelineDagId: pipeline && pipeline.dag_id ? pipeline.dag_id : "",
            readOnly: !canEditPipeline,
            onConfigChange: handleNodeConfigChange,
            onLabelChange: handleNodeLabelChange,
          })
        )
      ),
      showDagVersions
        ? h(
            "div",
            {
              className: "dag-versions-overlay",
              role: "dialog",
              "aria-modal": "true",
              onClick: function (event) {
                if (event.target === event.currentTarget) {
                  setShowDagVersions(false);
                }
              },
            },
            h(
              "div",
              { className: "dag-versions-modal" },
              h(
                "div",
                { className: "dag-versions-head" },
                h(
                  "div",
                  null,
                  h("h2", null, "DAG Versions"),
                  h("small", null, "Select a DAG version and decide which one to keep as current.")
                ),
                h(
                  "button",
                  {
                    className: "btn btn-secondary",
                    type: "button",
                    onClick: function () {
                      setShowDagVersions(false);
                    },
                  },
                  "Close Versions"
                )
              ),
              h("div", { className: "field" }, h("label", null, "Logical Pipeline ID"), h("input", {
                type: "text",
                value: String((pipeline && pipeline.dag_id) || ""),
                readOnly: true,
                disabled: true,
              })),
              versionsLoading
                ? h("p", null, "Loading versions...")
                : !pipelineVersions.length
                ? h("p", null, "No DAG version generated yet for this pipeline.")
                : h(
                    "div",
                    null,
                    h(
                      "div",
                      { className: "field" },
                      h("label", null, "Available Versions"),
                      h(
                        "select",
                        {
                          value: String(selectedVersionId || ""),
                          onChange: function (event) {
                            setSelectedVersionId(event.target.value);
                          },
                        },
                        pipelineVersions.map(function (item) {
                          const suffix = item.is_current ? " (current)" : "";
                          const label = String(item.version_id || "") + " | dag_id=" + String(item.airflow_dag_id || "") + suffix;
                          return h("option", { key: item.version_id, value: item.version_id }, label);
                        })
                      )
                    ),
                    h(
                      "div",
                      { className: "field version-meta-line" },
                      h(
                        "span",
                        { className: "version-chip" },
                        h("strong", null, "Current"),
                        " ",
                        h("span", null, currentVersionId || "none")
                      ),
                      h(
                        "span",
                        { className: "version-chip" },
                        h("strong", null, "Selected"),
                        " ",
                        h("span", null, selectedVersionId || "none")
                      )
                    ),
                    h(
                      "div",
                      { className: "dag-versions-actions" },
                      h(
                        "button",
                        {
                          className: "btn btn-primary",
                          type: "button",
                          onClick: handleSetCurrentVersion,
                          disabled: !selectedVersionId || selectedVersionId === currentVersionId || !canGenerateDag,
                        },
                        "Use This Version"
                      ),
                      h(
                        "button",
                        {
                          className: "btn btn-secondary",
                          type: "button",
                          onClick: handleClearVersions,
                          disabled: versionsLoading || !canGenerateDag || pipelineVersions.length === 0,
                        },
                        "Clear Versions"
                      ),
                      h(
                        "button",
                        {
                          className: "btn btn-secondary",
                          type: "button",
                          onClick: function () {
                            setShowExecutionHistory(function (prev) {
                              return !prev;
                            });
                          },
                        },
                        showExecutionHistory ? "Hide Execution History" : "Execution History"
                      ),
                      h(
                        "button",
                        {
                          className: "btn btn-secondary",
                          type: "button",
                          onClick: function () {
                            setShowDagVersions(false);
                          },
                        },
                        "Keep Selected Version"
                      )
                    ),
                    selectedVersion
                      ? h(
                          "div",
                          { className: "version-card" },
                          h("div", { className: "version-row" }, h("span", { className: "version-key" }, "Airflow DAG ID"), h("span", { className: "version-val" }, String(selectedVersion.airflow_dag_id || "-"))),
                          h("div", { className: "version-row" }, h("span", { className: "version-key" }, "Created At"), h("span", { className: "version-val" }, String(selectedVersion.created_at || "-"))),
                          h("div", { className: "version-row" }, h("span", { className: "version-key" }, "Created By"), h("span", { className: "version-val" }, String(selectedVersion.created_by || "-"))),
                          h("div", { className: "version-row" }, h("span", { className: "version-key" }, "Status"), h("span", { className: "version-val status-" + String(selectedVersion.status || "").toLowerCase() }, String(selectedVersion.status || "-")))
                        )
                      : null,
                    versionSummaryLoading
                      ? h("p", null, "Loading selected version in canvas...")
                    : versionSummaryError
                      ? h("p", { className: "field-error" }, versionSummaryError)
                      : null,
                    showExecutionHistory
                      ? h(
                          "div",
                          { className: "version-history-wrap" },
                          h("h4", null, "Execution History"),
                          versionExecutionHistoryLoading
                            ? h("p", null, "Loading execution history...")
                            : versionExecutionHistoryError
                            ? h("p", { className: "field-error" }, versionExecutionHistoryError)
                            : groupedVersionExecutionHistory.length === 0
                            ? h("p", null, "No executions found for this version.")
                            : h(
                                "div",
                                { className: "version-history-groups" },
                                groupedVersionExecutionHistory.map(function (group) {
                                  return h(
                                    "div",
                                    { className: "version-history-group", key: "group_" + group.date },
                                    h(
                                      "div",
                                      { className: "version-history-date" },
                                      h("span", null, group.date)
                                    ),
                                    h(
                                      "div",
                                      { className: "version-history-runs" },
                                      group.runs.map(function (runItem) {
                                        const runState = normalizeTaskState(runItem.state || "unknown");
                                        return h(
                                          "button",
                                          {
                                            key: String((runItem.dag_run_id || "") + "|" + (runItem.start_date || runItem.logical_date || "")),
                                            className: "version-history-run state-" + runState,
                                            type: "button",
                                            onClick: function () {
                                              handleLoadHistoricalRun(runItem);
                                            },
                                          },
                                          h("strong", null, String(runItem.start_date || runItem.logical_date || runItem.dag_run_id || "Execution")),
                                          h("small", null, "run_id: " + String(runItem.dag_run_id || "-")),
                                          h("small", null, "state: " + String(runItem.state || "unknown")),
                                          h("small", null, "end: " + String(runItem.end_date || "-"))
                                        );
                                      })
                                    )
                                  );
                                })
                              )
                        )
                      : null
                  )
            )
          )
        : null,
      logsPanel.isOpen
        ? h(
            "section",
            { className: "logs-drawer" },
            h(
              "div",
              { className: "logs-drawer-head" },
              h(
                "div",
                { className: "logs-meta" },
                h("strong", null, "Task Logs"),
                h(
                  "span",
                  null,
                  "node=" +
                    (logsPanel.nodeId || "-") +
                    " | task_id=" +
                    (logsPanel.taskId || "-") +
                    " | state=" +
                    String(logsPanel.state || "unknown").toLowerCase() +
                    " | try=" +
                    String(logsPanel.tryNumber || 1)
                ),
                logsPanel.startDate || logsPanel.endDate
                  ? h(
                      "span",
                      null,
                      "start=" + (logsPanel.startDate || "-") + " | end=" + (logsPanel.endDate || "-")
                    )
                  : null
              ),
              h(
                "div",
                { className: "logs-actions" },
                h(
                  "button",
                  {
                    className: "btn btn-secondary",
                    type: "button",
                    onClick: refreshCurrentLogs,
                    disabled: logsPanel.isLoading,
                  },
                  logsPanel.isLoading ? "Refreshing..." : "Refresh Logs"
                ),
                h(
                  "button",
                  {
                    className: "btn btn-secondary",
                    type: "button",
                    onClick: function () {
                      setLogsPanel({
                        isOpen: false,
                        nodeId: null,
                        taskId: "",
                        state: "",
                        tryNumber: 1,
                        startDate: "",
                        endDate: "",
                        content: "",
                        isLoading: false,
                        error: "",
                        lastFetchedAt: null,
                      });
                    },
                  },
                  "Close"
                )
              )
            ),
            logsPanel.error
              ? h("div", { className: "logs-error" }, logsPanel.error)
              : h(
                  "pre",
                  { className: "logs-content" },
                  logsPanel.content || "[No logs yet for this task. Trigger refresh to check again.]"
                ),
            logsPanel.lastFetchedAt
              ? h(
                  "div",
                  { className: "logs-footnote" },
                  "Last updated: " +
                    new Date(logsPanel.lastFetchedAt).toLocaleTimeString([], {
                      hour: "2-digit",
                      minute: "2-digit",
                      second: "2-digit",
                    })
                )
              : null
          )
        : null,
      connectionsUi.isOpen
        ? h(
            "div",
            { className: "connections-overlay", role: "dialog", "aria-modal": "true" },
            h(
              "div",
              { className: "connections-modal" },
              h(
                "div",
                { className: "connections-head" },
                h(
                  "div",
                  null,
                  h("h2", null, "Airflow Connections"),
                  h("small", null, "Manage HTTP and Azure Blob Storage connections inside the builder.")
                ),
                h(
                  "button",
                  {
                    className: "btn btn-secondary",
                    type: "button",
                    onClick: closeConnectionsManager,
                  },
                  "Close"
                )
              ),
              h(
                "div",
                { className: "connections-toolbar" },
                h("input", {
                  className: "module-search",
                  type: "text",
                  placeholder: "Search by connection id, type, or description...",
                  value: connectionsUi.search,
                  onChange: function (event) {
                    handleConnectionSearchChange(event.target.value);
                  },
                }),
                h(
                  "button",
                  {
                    className: "btn btn-secondary",
                    type: "button",
                    onClick: refreshConnectionsList,
                    disabled: connectionsUi.isLoading,
                  },
                  connectionsUi.isLoading ? "Loading..." : "Refresh"
                ),
                h(
                  "button",
                  {
                    className: "btn btn-primary",
                    type: "button",
                    onClick: openCreateConnectionEditor,
                    disabled: !canManageConnections,
                  },
                  canManageConnections ? "Create Connection" : "Read-only"
                )
              ),
              connectionsUi.error ? h("div", { className: "logs-error" }, connectionsUi.error) : null,
              connectionsUi.message ? h("div", { className: "connections-message" }, connectionsUi.message) : null,
              h(
                "div",
                { className: "connections-table-wrap" },
                h(
                  "table",
                  { className: "connections-table" },
                  h(
                    "thead",
                    null,
                    h(
                      "tr",
                      null,
                      h("th", null, "Connection ID"),
                      h("th", null, "Type"),
                      h("th", null, "Description"),
                      h("th", null, canManageConnections ? "Actions" : "Access")
                    )
                  ),
                  h(
                    "tbody",
                    null,
                    filteredConnections.length === 0
                      ? h(
                          "tr",
                          null,
                          h(
                            "td",
                            { colSpan: 4, className: "connections-empty-row" },
                            connectionsUi.isLoading
                              ? "Loading connections..."
                              : (canManageConnections
                                  ? "No connections found. Create your first connection."
                                  : "No connections found.")
                          )
                        )
                      : filteredConnections.map(function (item) {
                          const connType = normalizeConnectionTypeForUi(item.conn_type);
                          const connTypeLabel = connType === "http" ? "HTTP" : "Azure Blob Storage";
                          return h(
                            "tr",
                            { key: item.connection_id },
                            h("td", null, item.connection_id || "-"),
                            h("td", null, connTypeLabel),
                            h("td", null, item.description || "-"),
                            h(
                              "td",
                              { className: "connections-actions-cell" },
                              canManageConnections
                                ? h(
                                    React.Fragment,
                                    null,
                                    h(
                                      "button",
                                      {
                                        className: "btn btn-secondary",
                                        type: "button",
                                        onClick: function () {
                                          openEditConnectionEditor(item);
                                        },
                                      },
                                      "Edit"
                                    ),
                                    h(
                                      "button",
                                      {
                                        className: "btn btn-secondary",
                                        type: "button",
                                        onClick: function () {
                                          deleteConnectionById(item.connection_id);
                                        },
                                      },
                                      "Delete"
                                    )
                                  )
                                : h("span", { className: "admin-empty" }, "Read-only")
                            )
                          );
                        })
                  )
                )
              ),
              connectionsUi.editorOpen && canManageConnections
                ? h(
                    "div",
                    { className: "connections-editor" },
                    h(
                      "div",
                      { className: "connections-editor-head" },
                      h(
                        "strong",
                        null,
                        connectionsUi.editorMode === "create" ? "Create connection" : "Modify connection"
                      ),
                      h(
                        "button",
                        {
                          className: "btn btn-secondary",
                          type: "button",
                          onClick: closeConnectionEditor,
                          disabled: connectionsUi.isSubmitting,
                        },
                        "Close"
                      )
                    ),
                    h(
                      "div",
                      { className: "connections-editor-fields" },
                      h(
                        "div",
                        { className: "field" },
                        h("label", null, "Connection ID *"),
                        h("input", {
                          type: "text",
                          value: connectionsUi.form.connection_id,
                          readOnly: connectionsUi.editorMode === "edit",
                          placeholder: "e.g. n8n_local or wasb_default",
                          onChange: function (event) {
                            handleConnectionFormChange("connection_id", event.target.value);
                          },
                        }),
                        connectionsUi.formErrors.connection_id
                          ? h("div", { className: "field-error" }, connectionsUi.formErrors.connection_id)
                          : null
                      ),
                      h(
                        "div",
                        { className: "field" },
                        h("label", null, "Connection Type *"),
                        h(
                          "select",
                          {
                            value: connectionsUi.form.connection_type,
                            disabled: connectionsUi.editorMode === "edit",
                            onChange: function (event) {
                              handleConnectionFormChange("connection_type", event.target.value);
                            },
                          },
                          h("option", { value: "" }, "Select a connection type"),
                          CONNECTION_TYPE_OPTIONS.map(function (option) {
                            return h("option", { key: option.value, value: option.value }, option.label);
                          })
                        ),
                        connectionsUi.formErrors.connection_type
                          ? h("div", { className: "field-error" }, connectionsUi.formErrors.connection_type)
                          : null
                      ),
                      h(
                        "div",
                        { className: "field" },
                        h("label", null, "Description"),
                        h("input", {
                          type: "text",
                          value: connectionsUi.form.description,
                          placeholder: "Optional connection description",
                          onChange: function (event) {
                            handleConnectionFormChange("description", event.target.value);
                          },
                        })
                      ),
                      connectionsUi.form.connection_type === "http"
                        ? h(
                            React.Fragment,
                            null,
                            h(
                              "div",
                              { className: "field" },
                              h("label", null, "Host *"),
                              h("input", {
                                type: "text",
                                value: connectionsUi.form.host,
                                placeholder: "https://your-host.example.com",
                                onChange: function (event) {
                                  handleConnectionFormChange("host", event.target.value);
                                },
                              }),
                              connectionsUi.formErrors.host
                                ? h("div", { className: "field-error" }, connectionsUi.formErrors.host)
                                : null
                            ),
                            h(
                              "div",
                              { className: "field-inline-combo" },
                              h(
                                "div",
                                { className: "field" },
                                h("label", null, "Login"),
                                h("input", {
                                  type: "text",
                                  value: connectionsUi.form.login,
                                  onChange: function (event) {
                                    handleConnectionFormChange("login", event.target.value);
                                  },
                                })
                              ),
                              h(
                                "div",
                                { className: "field" },
                                h("label", null, "Password"),
                                h("input", {
                                  type: "password",
                                  value: connectionsUi.form.password,
                                  onChange: function (event) {
                                    handleConnectionFormChange("password", event.target.value);
                                  },
                                })
                              )
                            ),
                            h(
                              "div",
                              { className: "field-inline-combo" },
                              h(
                                "div",
                                { className: "field" },
                                h("label", null, "Port"),
                                h("input", {
                                  type: "number",
                                  min: "1",
                                  value: connectionsUi.form.port,
                                  placeholder: "443",
                                  onChange: function (event) {
                                    handleConnectionFormChange("port", event.target.value);
                                  },
                                }),
                                connectionsUi.formErrors.port
                                  ? h("div", { className: "field-error" }, connectionsUi.formErrors.port)
                                  : null
                              ),
                              h(
                                "div",
                                { className: "field" },
                                h("label", null, "Schema"),
                                h("input", {
                                  type: "text",
                                  value: connectionsUi.form.schema,
                                  placeholder: "https",
                                  onChange: function (event) {
                                    handleConnectionFormChange("schema", event.target.value);
                                  },
                                })
                              )
                            )
                          )
                        : null,
                      connectionsUi.form.connection_type === "wasb"
                        ? h(
                            React.Fragment,
                            null,
                            h(
                              "div",
                              { className: "field" },
                              h("label", null, "Account URL (Active Directory Auth) *"),
                              h("input", {
                                type: "text",
                                value: connectionsUi.form.account_url,
                                placeholder: "https://youraccount.blob.core.windows.net",
                                onChange: function (event) {
                                  handleConnectionFormChange("account_url", event.target.value);
                                },
                              }),
                              connectionsUi.formErrors.account_url
                                ? h("div", { className: "field-error" }, connectionsUi.formErrors.account_url)
                                : null
                            ),
                            h(
                              "div",
                              { className: "field-inline-combo" },
                              h(
                                "div",
                                { className: "field" },
                                h("label", null, "Blob Storage Login (optional)"),
                                h("input", {
                                  type: "text",
                                  value: connectionsUi.form.login,
                                  placeholder: "Optional",
                                  onChange: function (event) {
                                    handleConnectionFormChange("login", event.target.value);
                                  },
                                })
                              ),
                              h(
                                "div",
                                { className: "field" },
                                h("label", null, "Blob Storage Key (optional)"),
                                h("input", {
                                  type: "password",
                                  value: connectionsUi.form.password,
                                  placeholder: "Optional",
                                  onChange: function (event) {
                                    handleConnectionFormChange("password", event.target.value);
                                  },
                                })
                              )
                            )
                          )
                        : null,
                      h(
                        "div",
                        { className: "field" },
                        h("label", null, "Extra JSON (optional)"),
                        h("textarea", {
                          value: connectionsUi.form.extra,
                          placeholder: "{\"key\": \"value\"}",
                          onChange: function (event) {
                            handleConnectionFormChange("extra", event.target.value);
                          },
                        }),
                        connectionsUi.formErrors.extra
                          ? h("div", { className: "field-error" }, connectionsUi.formErrors.extra)
                          : null
                      )
                    ),
                    h(
                      "div",
                      { className: "connections-editor-actions" },
                      h(
                        "button",
                        {
                          className: "btn btn-secondary",
                          type: "button",
                          onClick: closeConnectionEditor,
                          disabled: connectionsUi.isSubmitting,
                        },
                        "Cancel"
                      ),
                      h(
                        "button",
                        {
                          className: "btn btn-primary",
                          type: "button",
                          onClick: submitConnectionForm,
                          disabled: connectionsUi.isSubmitting,
                        },
                        connectionsUi.isSubmitting ? "Saving..." : "Save Connection"
                      )
                    )
                  )
                : null
            )
          )
        : null,
      h(
        "div",
        { className: "status-bar" },
        h("span", { className: statusKind }, statusText),
        h("span", null, "Nodes: " + nodes.length + " | Edges: " + edges.length)
      ),
      showPreview
        ? h(
            "section",
            { className: "preview-panel" },
            h(
              "div",
              { className: "preview-panel-head" },
              h("strong", null, "Generated DAG Preview"),
              h(
                "button",
                {
                  className: "btn btn-secondary preview-close-btn",
                  type: "button",
                  onClick: function () {
                    setShowPreview(false);
                  },
                  "aria-label": "Close DAG preview",
                  title: "Close preview",
                },
                "Close"
              )
            ),
            h("pre", null, dagPreview || "# No DAG preview available yet.")
          )
        : null
    );
  }

  const root = ReactDOM.createRoot(document.getElementById("app-root"));
  root.render(h(App));
})();
