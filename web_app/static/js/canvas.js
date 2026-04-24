(function () {
  const h = React.createElement;
  const useEffect = React.useEffect;
  const useMemo = React.useMemo;
  const useRef = React.useRef;
  const useState = React.useState;

  const NODE_WIDTH = 220;
  const NODE_HEIGHT = 88;
  const STAGE_WIDTH = 6000;
  const STAGE_HEIGHT = 4000;
  const MIN_SCALE = 0.45;
  const MAX_SCALE = 1.8;
  const GROUP_BOX_MIN_WIDTH = 180;
  const GROUP_BOX_MIN_HEIGHT = 120;
  const MULTI_OUT_TYPES = new Set(["parallel_fork", "parallel_limit", "conditional_router", "if_else_router", "switch_router", "try_catch"]);
  const MULTI_IN_TYPES = new Set(["parallel_join", "quorum_join"]);

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function buildOrderLookup(executionOrder) {
    const lookup = {};
    (executionOrder || []).forEach((nodeId, idx) => {
      lookup[nodeId] = idx + 1;
    });
    return lookup;
  }

  function resolveNodeLogo(node) {
    if (node.logo) {
      return node.logo;
    }
    if (window.PipelineBuilderModules && window.PipelineBuilderModules.getLogoForType) {
      return window.PipelineBuilderModules.getLogoForType(node.type);
    }
    return "";
  }

  function getConnectorPoint(node, side) {
    return {
      x: side === "out" ? node.x + NODE_WIDTH : node.x,
      y: node.y + NODE_HEIGHT / 2,
    };
  }

  function makeConnectorPath(start, end) {
    const dx = end.x - start.x;
    const dy = end.y - start.y;
    const distance = Math.hypot(dx, dy);

    // Balanced control points for short and long links.
    let handle = clamp(distance * 0.34, 34, 170);
    if (dx < 0) {
      handle = clamp(Math.abs(dx) * 0.28 + 92, 92, 210);
    }

    const c1x = start.x + handle;
    const c2x = end.x - handle;
    const verticalBias = clamp(Math.abs(dy) * 0.14, 0, 28);
    const c1y = start.y + Math.sign(dy || 1) * verticalBias;
    const c2y = end.y - Math.sign(dy || 1) * verticalBias;

    return (
      "M " +
      start.x +
      " " +
      start.y +
      " C " +
      c1x +
      " " +
      c1y +
      ", " +
      c2x +
      " " +
      c2y +
      ", " +
      end.x +
      " " +
      end.y
    );
  }

  function normalizeRunState(value) {
    const normalized = String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]+/g, "_");
    if (normalized === "up_for_retry" || normalized === "retrying") {
      return "retry";
    }
    return normalized;
  }

  function toRunBadgeLabel(value) {
    const state = normalizeRunState(value);
    if (!state) {
      return "";
    }
    if (state === "retry") {
      return "retry";
    }
    return state.replace(/_/g, " ");
  }

  function colorToTint(color, alpha) {
    const text = String(color || "").trim();
    const match = text.match(/^#([0-9a-f]{6})$/i);
    if (!match) {
      return "rgba(47, 110, 162, " + String(alpha || 0.1) + ")";
    }
    const hex = match[1];
    const r = parseInt(hex.slice(0, 2), 16);
    const g = parseInt(hex.slice(2, 4), 16);
    const b = parseInt(hex.slice(4, 6), 16);
    return "rgba(" + r + ", " + g + ", " + b + ", " + String(alpha || 0.1) + ")";
  }

  function TrashIcon() {
    return h(
      "svg",
      {
        className: "trash-icon",
        viewBox: "0 0 24 24",
        ariaHidden: "true",
        focusable: "false",
      },
      h("path", {
        d: "M9 3h6l1 2h4v2H4V5h4l1-2zm-1 6h2v9H8V9zm6 0h2v9h-2V9zM6 9h2v9H6V9zm2 12h8a2 2 0 0 0 2-2V7H6v12a2 2 0 0 0 2 2z",
        fill: "currentColor",
      })
    );
  }

  function LogsIcon() {
    return h(
      "svg",
      {
        className: "trash-icon",
        viewBox: "0 0 24 24",
        ariaHidden: "true",
        focusable: "false",
      },
      h("path", {
        d: "M5 4h10l4 4v12H5V4zm10 1.5V9h3.5L15 5.5zM8 12h8v1.7H8V12zm0 3.2h8V17H8v-1.8zm0-6.4h5.2v1.7H8V8.8z",
        fill: "currentColor",
      })
    );
  }

  function RetryIcon() {
    return h(
      "svg",
      {
        className: "trash-icon",
        viewBox: "0 0 24 24",
        ariaHidden: "true",
        focusable: "false",
      },
      h("path", {
        d: "M12 4a8 8 0 1 1-7.7 10.2h2.2A6 6 0 1 0 8.3 8.3L11 11H4V4l2.9 2.9A7.9 7.9 0 0 1 12 4z",
        fill: "currentColor",
      })
    );
  }

  function GroupBoxItem(props) {
    const groupBox = props.groupBox;
    const boxColor = String(groupBox.color || "#2f6ea2");
    const [isEditingTitle, setIsEditingTitle] = useState(false);
    const [draftTitle, setDraftTitle] = useState(String(groupBox.title || ""));

    useEffect(
      function () {
        if (!isEditingTitle) {
          setDraftTitle(String(groupBox.title || ""));
        }
      },
      [groupBox.title, isEditingTitle]
    );

    function commitTitle() {
      setIsEditingTitle(false);
      if (props.onChange) {
        props.onChange(groupBox.id, "title", draftTitle);
      }
    }

    function cancelTitleEdit() {
      setDraftTitle(String(groupBox.title || ""));
      setIsEditingTitle(false);
    }

    function shouldStartDrag(event) {
      if (props.readOnly) {
        return false;
      }
      const target = event.target;
      if (!(target instanceof Element)) {
        return true;
      }
      if (target.closest(".group-box-delete-btn")) {
        return false;
      }
      if (target.closest(".group-box-color-input")) {
        return false;
      }
      if (target.closest(".group-box-resize-handle")) {
        return false;
      }
      if (target.closest(".group-box-title-editor")) {
        return false;
      }
      return true;
    }

    function handleBoxPointerDown(event) {
      event.stopPropagation();
      if (props.onSelect) {
        props.onSelect(groupBox.id);
      }
      if (shouldStartDrag(event) && props.onBeginDrag) {
        props.onBeginDrag(event, groupBox);
      }
    }

    return h(
      "div",
      {
        className: "group-box" + (props.isSelected ? " selected" : ""),
        style: {
          left: (Number(groupBox.x) || 0) + "px",
          top: (Number(groupBox.y) || 0) + "px",
          width: Math.max(GROUP_BOX_MIN_WIDTH, Number(groupBox.width) || GROUP_BOX_MIN_WIDTH) + "px",
          height: Math.max(GROUP_BOX_MIN_HEIGHT, Number(groupBox.height) || GROUP_BOX_MIN_HEIGHT) + "px",
          borderColor: boxColor,
          backgroundColor: colorToTint(boxColor, 0.12),
        },
        onPointerDown: handleBoxPointerDown,
      },
      h(
        "div",
        { className: "group-box-head" },
        h(
          "div",
          { className: "group-box-title-wrap" },
          isEditingTitle && !props.readOnly
            ? h("input", {
                className: "group-box-title-editor",
                type: "text",
                value: draftTitle,
                placeholder: "Group title",
                autoFocus: true,
                onPointerDown: function (event) {
                  event.stopPropagation();
                },
                onChange: function (event) {
                  setDraftTitle(event.target.value);
                },
                onBlur: function () {
                  commitTitle();
                },
                onKeyDown: function (event) {
                  if (event.key === "Enter") {
                    event.preventDefault();
                    commitTitle();
                    return;
                  }
                  if (event.key === "Escape") {
                    event.preventDefault();
                    cancelTitleEdit();
                  }
                },
              })
            : h(
                "span",
                {
                  className: "group-box-title-text",
                  title: "Double-click to edit",
                  onDoubleClick: function (event) {
                    if (props.readOnly) {
                      return;
                    }
                    event.preventDefault();
                    event.stopPropagation();
                    setIsEditingTitle(true);
                  },
                },
                String(groupBox.title || "Group")
              )
        ),
        h(
          "div",
          { className: "group-box-head-actions" },
          h("input", {
            className: "group-box-color-input",
            type: "color",
            value: boxColor,
            disabled: Boolean(props.readOnly),
            onPointerDown: function (event) {
              event.stopPropagation();
            },
            onChange: function (event) {
              if (props.onChange) {
                props.onChange(groupBox.id, "color", event.target.value);
              }
            },
          }),
          !props.readOnly
            ? h(
                "button",
                {
                  type: "button",
                  className: "group-box-delete-btn",
                  "aria-label": "Delete group box",
                  title: "Delete group box",
                  onPointerDown: function (event) {
                    event.stopPropagation();
                  },
                  onClick: function (event) {
                    event.stopPropagation();
                    if (props.onDelete) {
                      props.onDelete(groupBox.id);
                    }
                  },
                },
                h(TrashIcon)
              )
            : null
        )
      ),
      !props.readOnly
        ? h("button", {
            type: "button",
            className: "group-box-resize-handle",
            onPointerDown: function (event) {
              props.onBeginResize(event, groupBox);
            },
            title: "Resize group box",
          })
        : null
    );
  }

  function PipelineCanvas(props) {
    const canvasRef = useRef(null);
    const viewportRef = useRef({ x: 120, y: 70, scale: 1 });
    const onNodeMovedRef = useRef(props.onNodeMoved);
    const onConnectNodesRef = useRef(props.onConnectNodes);
    const movedFlagRef = useRef(false);

    const [viewport, setViewport] = useState(viewportRef.current);
    const [dragState, setDragState] = useState(null);
    const [groupDragState, setGroupDragState] = useState(null);
    const [groupResizeState, setGroupResizeState] = useState(null);
    const [panState, setPanState] = useState(null);
    const [draggingNodeId, setDraggingNodeId] = useState(null);
    const [connectionDrag, setConnectionDrag] = useState(null);
    const [hoveredEdgeKey, setHoveredEdgeKey] = useState(null);

    const nodeById = useMemo(function () {
      const map = {};
      props.nodes.forEach(function (node) {
        map[node.id] = node;
      });
      return map;
    }, [props.nodes]);

    const orderLookup = useMemo(function () {
      return buildOrderLookup(props.executionOrder);
    }, [props.executionOrder]);

    const incomingCountByTarget = useMemo(function () {
      const map = {};
      props.edges.forEach(function (edge) {
        map[edge.target] = (map[edge.target] || 0) + 1;
      });
      return map;
    }, [props.edges]);

    const outgoingCountBySource = useMemo(function () {
      const map = {};
      props.edges.forEach(function (edge) {
        map[edge.source] = (map[edge.source] || 0) + 1;
      });
      return map;
    }, [props.edges]);

    useEffect(
      function () {
        viewportRef.current = viewport;
      },
      [viewport]
    );

    useEffect(
      function () {
        onNodeMovedRef.current = props.onNodeMoved;
      },
      [props.onNodeMoved]
    );

    useEffect(
      function () {
        onConnectNodesRef.current = props.onConnectNodes;
      },
      [props.onConnectNodes]
    );

    // Convert viewport/screen coordinates into world coordinates after pan+zoom.
    function toWorld(clientX, clientY) {
      const rect = canvasRef.current ? canvasRef.current.getBoundingClientRect() : null;
      const vp = viewportRef.current;
      if (!rect) {
        return { x: 0, y: 0 };
      }

      const localX = clientX - rect.left;
      const localY = clientY - rect.top;
      return {
        x: (localX - vp.x) / vp.scale,
        y: (localY - vp.y) / vp.scale,
      };
    }

    function isValidTarget(sourceId, targetId) {
      if (props.readOnly) {
        return false;
      }
      if (!sourceId || !targetId || sourceId === targetId) {
        return false;
      }

      const sourceNode = nodeById[sourceId];
      const targetNode = nodeById[targetId];
      if (!sourceNode || !targetNode) {
        return false;
      }

      const hasDuplicate = props.edges.some(function (edge) {
        return edge.source === sourceId && edge.target === targetId;
      });
      if (hasDuplicate) {
        return false;
      }

      const sourceOutCount = outgoingCountBySource[sourceId] || 0;
      const targetInCount = incomingCountByTarget[targetId] || 0;

      if (!MULTI_OUT_TYPES.has(sourceNode.type) && sourceOutCount >= 1) {
        return false;
      }
      if (!MULTI_IN_TYPES.has(targetNode.type) && targetInCount >= 1) {
        return false;
      }
      return true;
    }

    function getHoveredInputNodeId(clientX, clientY) {
      const element = document.elementFromPoint(clientX, clientY);
      if (!element) {
        return null;
      }
      const connector = element.closest(".connector-zone-in");
      if (!connector) {
        return null;
      }
      return connector.getAttribute("data-node-id");
    }

    function handleCanvasDrop(event) {
      event.preventDefault();
      if (props.readOnly) {
        return;
      }

      const paletteRaw = event.dataTransfer.getData("application/x-palette-item");
      if (!paletteRaw) {
        return;
      }

      const paletteItem = JSON.parse(paletteRaw);
      const point = toWorld(event.clientX, event.clientY);
      props.onCanvasDrop({
        kind: "palette",
        item: paletteItem,
        x: Math.max(8, point.x - NODE_WIDTH / 2),
        y: Math.max(8, point.y - 36),
      });
    }

    function handleCanvasWheel(event) {
      event.preventDefault();
      const rect = canvasRef.current ? canvasRef.current.getBoundingClientRect() : null;
      if (!rect) {
        return;
      }

      const mouseX = event.clientX - rect.left;
      const mouseY = event.clientY - rect.top;
      const vp = viewportRef.current;

      const worldX = (mouseX - vp.x) / vp.scale;
      const worldY = (mouseY - vp.y) / vp.scale;
      const zoomFactor = event.deltaY < 0 ? 1.1 : 0.9;
      const nextScale = clamp(vp.scale * zoomFactor, MIN_SCALE, MAX_SCALE);

      // Keep the point under cursor stable while zooming.
      setViewport({
        x: mouseX - worldX * nextScale,
        y: mouseY - worldY * nextScale,
        scale: nextScale,
      });
    }

    function handleCanvasPointerDown(event) {
      if (event.button !== 0) {
        return;
      }

      if (event.target.closest(".node-card") || event.target.closest(".group-box") || event.target.closest(".zoom-controls")) {
        return;
      }

      if (props.onClearSelection) {
        props.onClearSelection();
      }

      event.preventDefault();
      const vp = viewportRef.current;
      setPanState({
        startX: event.clientX,
        startY: event.clientY,
        originX: vp.x,
        originY: vp.y,
      });
    }

    function beginNodeDrag(event, node) {
      if (props.readOnly) {
        return;
      }
      if (event.button !== 0) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();

      const world = toWorld(event.clientX, event.clientY);
      movedFlagRef.current = false;
      setDraggingNodeId(node.id);
      setDragState({
        nodeId: node.id,
        offsetX: world.x - node.x,
        offsetY: world.y - node.y,
      });
    }

    function beginGroupBoxDrag(event, groupBox) {
      if (props.readOnly) {
        return;
      }
      if (event.button !== 0) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const world = toWorld(event.clientX, event.clientY);
      setGroupDragState({
        groupId: groupBox.id,
        offsetX: world.x - (Number(groupBox.x) || 0),
        offsetY: world.y - (Number(groupBox.y) || 0),
      });
      if (props.onSelectGroupBox) {
        props.onSelectGroupBox(groupBox.id);
      }
    }

    function beginGroupBoxResize(event, groupBox) {
      if (props.readOnly) {
        return;
      }
      if (event.button !== 0) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const world = toWorld(event.clientX, event.clientY);
      setGroupResizeState({
        groupId: groupBox.id,
        originX: world.x,
        originY: world.y,
        originWidth: Number(groupBox.width) || GROUP_BOX_MIN_WIDTH,
        originHeight: Number(groupBox.height) || GROUP_BOX_MIN_HEIGHT,
      });
      if (props.onSelectGroupBox) {
        props.onSelectGroupBox(groupBox.id);
      }
    }

    function beginConnectionDrag(event, sourceNodeId) {
      if (props.readOnly) {
        return;
      }
      if (event.button !== 0 && event.pointerType !== "touch") {
        return;
      }
      event.preventDefault();
      event.stopPropagation();

      const world = toWorld(event.clientX, event.clientY);
      setConnectionDrag({
        sourceId: sourceNodeId,
        cursorX: world.x,
        cursorY: world.y,
        hoveredTargetId: null,
      });
    }

    function handleNodeClick(event, nodeId) {
      if (movedFlagRef.current) {
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      const toggle = Boolean(event && (event.ctrlKey || event.metaKey || event.shiftKey));
      props.onSelectNode(nodeId, { toggle: toggle });
    }

    function handleNodeContextMenu(event, nodeId) {
      if (props.readOnly) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      if (props.onDuplicateNode) {
        props.onDuplicateNode(nodeId);
      }
    }

    function setZoomAtCenter(nextScale) {
      const rect = canvasRef.current ? canvasRef.current.getBoundingClientRect() : null;
      if (!rect) {
        return;
      }

      const vp = viewportRef.current;
      const centerX = rect.width / 2;
      const centerY = rect.height / 2;
      const worldX = (centerX - vp.x) / vp.scale;
      const worldY = (centerY - vp.y) / vp.scale;

      setViewport({
        x: centerX - worldX * nextScale,
        y: centerY - worldY * nextScale,
        scale: nextScale,
      });
    }

    useEffect(
      function () {
        function onPointerMove(event) {
          // Live node dragging with immediate position updates.
          if (dragState) {
            const world = toWorld(event.clientX, event.clientY);
            const nextX = Math.max(8, world.x - dragState.offsetX);
            const nextY = Math.max(8, world.y - dragState.offsetY);
            movedFlagRef.current = true;
            onNodeMovedRef.current(dragState.nodeId, nextX, nextY);
            return;
          }

          if (groupDragState) {
            const world = toWorld(event.clientX, event.clientY);
            const nextX = Math.max(8, world.x - groupDragState.offsetX);
            const nextY = Math.max(8, world.y - groupDragState.offsetY);
            if (props.onMoveGroupBox) {
              props.onMoveGroupBox(groupDragState.groupId, nextX, nextY);
            }
            return;
          }

          if (groupResizeState) {
            const world = toWorld(event.clientX, event.clientY);
            const dx = world.x - groupResizeState.originX;
            const dy = world.y - groupResizeState.originY;
            const nextWidth = Math.max(GROUP_BOX_MIN_WIDTH, groupResizeState.originWidth + dx);
            const nextHeight = Math.max(GROUP_BOX_MIN_HEIGHT, groupResizeState.originHeight + dy);
            if (props.onResizeGroupBox) {
              props.onResizeGroupBox(groupResizeState.groupId, nextWidth, nextHeight);
            }
            return;
          }

          // Live connector preview drag (mouse/touch via Pointer Events).
          if (connectionDrag) {
            const world = toWorld(event.clientX, event.clientY);
            const hoveredTargetId = getHoveredInputNodeId(event.clientX, event.clientY);
            setConnectionDrag(function (prev) {
              if (!prev) {
                return prev;
              }

              const validHovered = isValidTarget(prev.sourceId, hoveredTargetId) ? hoveredTargetId : null;
              if (
                prev.cursorX === world.x &&
                prev.cursorY === world.y &&
                prev.hoveredTargetId === validHovered
              ) {
                return prev;
              }

              return {
                sourceId: prev.sourceId,
                cursorX: world.x,
                cursorY: world.y,
                hoveredTargetId: validHovered,
              };
            });
            return;
          }

          if (panState) {
            const dx = event.clientX - panState.startX;
            const dy = event.clientY - panState.startY;
            setViewport({
              x: panState.originX + dx,
              y: panState.originY + dy,
              scale: viewportRef.current.scale,
            });
          }
        }

        function onPointerUp() {
          if (dragState) {
            window.setTimeout(function () {
              movedFlagRef.current = false;
            }, 0);
          }

          if (connectionDrag) {
            const sourceId = connectionDrag.sourceId;
            const targetId = connectionDrag.hoveredTargetId;
            if (targetId && isValidTarget(sourceId, targetId) && onConnectNodesRef.current) {
              onConnectNodesRef.current(sourceId, targetId);
            }
          }

          setDragState(null);
          setGroupDragState(null);
          setGroupResizeState(null);
          setPanState(null);
          setDraggingNodeId(null);
          setConnectionDrag(null);
        }

        if (dragState || groupDragState || groupResizeState || panState || connectionDrag) {
          window.addEventListener("pointermove", onPointerMove);
          window.addEventListener("pointerup", onPointerUp);
          window.addEventListener("pointercancel", onPointerUp);
        }

        return function () {
          window.removeEventListener("pointermove", onPointerMove);
          window.removeEventListener("pointerup", onPointerUp);
          window.removeEventListener("pointercancel", onPointerUp);
        };
      },
      [
        dragState,
        groupDragState,
        groupResizeState,
        panState,
        connectionDrag,
        outgoingCountBySource,
        incomingCountByTarget,
        nodeById,
        props.edges,
        props.onMoveGroupBox,
        props.onResizeGroupBox,
      ]
    );

    const previewPathData = useMemo(function () {
      if (!connectionDrag) {
        return null;
      }

      const sourceNode = nodeById[connectionDrag.sourceId];
      if (!sourceNode) {
        return null;
      }

      const start = getConnectorPoint(sourceNode, "out");
      let end = { x: connectionDrag.cursorX, y: connectionDrag.cursorY };

      if (connectionDrag.hoveredTargetId && nodeById[connectionDrag.hoveredTargetId]) {
        end = getConnectorPoint(nodeById[connectionDrag.hoveredTargetId], "in");
      }

      return makeConnectorPath(start, end);
    }, [connectionDrag, nodeById]);

    const validTargetLookup = useMemo(function () {
      if (!connectionDrag) {
        return {};
      }

      const map = {};
      props.nodes.forEach(function (node) {
        map[node.id] = isValidTarget(connectionDrag.sourceId, node.id);
      });
      return map;
    }, [connectionDrag, props.nodes, outgoingCountBySource, incomingCountByTarget, nodeById, props.edges]);

    return h(
      "div",
      {
        ref: canvasRef,
        className:
          "canvas-wrap" +
          (panState ? " canvas-panning" : "") +
          (dragState ? " canvas-dragging-node" : "") +
          (connectionDrag ? " canvas-connecting" : "") +
          (props.readOnly ? " canvas-readonly" : ""),
        onDragOver: function (e) {
          e.preventDefault();
        },
        onDrop: handleCanvasDrop,
        onWheel: handleCanvasWheel,
        onPointerDown: handleCanvasPointerDown,
      },
      h(
        "div",
        {
          className: "canvas-stage",
          style: {
            width: STAGE_WIDTH + "px",
            height: STAGE_HEIGHT + "px",
            // Single transform keeps nodes and SVG connectors aligned.
            transform: "translate(" + viewport.x + "px, " + viewport.y + "px) scale(" + viewport.scale + ")",
          },
        },
        h(
          "svg",
          { className: "edges-svg", width: STAGE_WIDTH, height: STAGE_HEIGHT },
          h(
            "defs",
            null,
            h(
              "marker",
              {
                id: "edge-arrowhead",
                markerWidth: 8,
                markerHeight: 8,
                refX: 7,
                refY: 3,
                orient: "auto",
                markerUnits: "strokeWidth",
              },
              h("path", { d: "M0,0 L0,6 L7,3 z", fill: "#2f4a63" })
            ),
            h(
              "marker",
              {
                id: "edge-arrowhead-preview",
                markerWidth: 8,
                markerHeight: 8,
                refX: 7,
                refY: 3,
                orient: "auto",
                markerUnits: "strokeWidth",
              },
              h("path", { d: "M0,0 L0,6 L7,3 z", fill: "#3b5f80" })
            )
          ),
          props.edges.map(function (edge, idx) {
            const source = nodeById[edge.source];
            const target = nodeById[edge.target];
            if (!source || !target) {
              return null;
            }

            const start = getConnectorPoint(source, "out");
            const end = getConnectorPoint(target, "in");
            const d = makeConnectorPath(start, end);
            const edgeKey = edge.source + "_" + edge.target + "_" + idx;
            const isHovered = hoveredEdgeKey === edgeKey;
            const isSelected =
              props.selectedEdge &&
              props.selectedEdge.source === edge.source &&
              props.selectedEdge.target === edge.target;

            return h(
              "g",
              { key: "edge_" + idx },
              h("path", {
                className: "edge-path-shadow",
                d: d,
              }),
              h("path", {
                className: "edge-path-hit",
                d: d,
                onPointerDown: function (event) {
                  event.stopPropagation();
                  if (props.onSelectEdge) {
                    props.onSelectEdge(edge);
                  }
                },
                onPointerEnter: function () {
                  setHoveredEdgeKey(edgeKey);
                },
                onPointerLeave: function () {
                  setHoveredEdgeKey(null);
                },
              }),
              h("path", {
                className:
                  "edge-path-main" +
                  (isHovered ? " edge-path-hovered" : "") +
                  (isSelected ? " edge-path-selected" : ""),
                d: d,
                markerEnd: "url(#edge-arrowhead)",
              })
            );
          }),
          previewPathData
            ? h("path", {
                className: "edge-path-preview",
                d: previewPathData,
                markerEnd: "url(#edge-arrowhead-preview)",
              })
            : null
        ),
        (props.groupBoxes || []).map(function (groupBox) {
          return h(GroupBoxItem, {
            key: groupBox.id,
            groupBox: groupBox,
            readOnly: props.readOnly,
            isSelected: props.selectedGroupBoxId === groupBox.id,
            onSelect: props.onSelectGroupBox,
            onChange: props.onChangeGroupBox,
            onDelete: props.onDeleteGroupBox,
            onBeginDrag: beginGroupBoxDrag,
            onBeginResize: beginGroupBoxResize,
          });
        }),
        props.nodes.map(function (node) {
          const selectedIds = Array.isArray(props.selectedNodeIds) ? props.selectedNodeIds : [];
          const isSelected = props.selectedNodeId === node.id || selectedIds.includes(node.id);
          const isSourceActive = connectionDrag && connectionDrag.sourceId === node.id;
          const isValidTarget = connectionDrag ? validTargetLookup[node.id] : false;
          const isHoveredTarget = connectionDrag && connectionDrag.hoveredTargetId === node.id;
          const orderIndex = orderLookup[node.id];
          const logo = resolveNodeLogo(node);
          const nodeRunStatus = props.nodeRunStatusMap ? props.nodeRunStatusMap[node.id] : null;
          const nodeRunState = nodeRunStatus ? normalizeRunState(nodeRunStatus.state) : "";
          const hasLogs = Boolean(
            props.canViewLogs &&
              nodeRunStatus &&
              nodeRunStatus.has_task_instance &&
              props.activeRun &&
              props.activeRun.dag_run_id
          );
          const canRetryNode = Boolean(
            props.onRetryNode &&
              nodeRunStatus &&
              nodeRunStatus.has_task_instance &&
              nodeRunStatus.task_id &&
              props.activeRun &&
              props.activeRun.dag_run_id &&
              (nodeRunState === "failed" || nodeRunState === "retry")
          );
          const retryInProgress = Boolean(props.nodeRetryingById && props.nodeRetryingById[node.id]);
          const statusBadgeLabel = nodeRunState ? toRunBadgeLabel(nodeRunStatus.state) : null;

          return h(
            "div",
            {
              key: node.id,
              className:
                "node-card" +
                (isSelected ? " selected" : "") +
                (draggingNodeId === node.id ? " dragging" : "") +
                (isSourceActive ? " connection-source" : "") +
                (nodeRunState ? " node-run-" + nodeRunState : ""),
              style: { left: node.x + "px", top: node.y + "px" },
              onClick: function (event) {
                handleNodeClick(event, node.id);
              },
              onContextMenu: function (event) {
                handleNodeContextMenu(event, node.id);
              },
            },
            h(
                  "button",
                  {
                    type: "button",
                    className:
                      "connector-zone connector-zone-in" +
                      (isValidTarget ? " connector-valid" : "") +
                      (isHoveredTarget ? " connector-hover" : "") +
                      (props.readOnly ? " connector-disabled" : ""),
                    "data-node-id": node.id,
                    title: "Target connector",
                    disabled: Boolean(props.readOnly),
                    onPointerDown: function (event) {
                      event.stopPropagation();
                    },
              },
              h("span", { className: "connector-core" })
            ),
            h(
              "button",
              {
                type: "button",
                className:
                  "connector-zone connector-zone-out" +
                  (isSourceActive ? " active" : "") +
                  (props.readOnly ? " connector-disabled" : ""),
                title: "Start connection",
                disabled: Boolean(props.readOnly),
                onPointerDown: function (event) {
                  beginConnectionDrag(event, node.id);
                },
              },
              h("span", { className: "connector-core" })
            ),
            h(
              "div",
              {
                className: "node-head",
                onPointerDown: function (event) {
                  beginNodeDrag(event, node);
                },
              },
              h(
                "div",
                { className: "node-head-left" },
                logo
                  ? h("img", {
                      className: "node-logo",
                      src: logo,
                      alt: node.type + " logo",
                      draggable: false,
                    })
                  : null,
                h("b", null, node.label),
                statusBadgeLabel
                  ? h(
                      "span",
                      { className: "node-run-badge node-run-badge-" + nodeRunState },
                      statusBadgeLabel
                    )
                  : null
              )
            ),
            h(
              "div",
              { className: "node-body" },
              h("div", null, "Node id: " + node.id),
              orderIndex ? h("div", null, "Execution order: " + orderIndex) : null,
              h(
                "div",
                { className: "node-actions" },
                props.canRetryNodeAction && (nodeRunState === "failed" || nodeRunState === "retry" || retryInProgress)
                  ? h(
                      "button",
                      {
                        type: "button",
                        className: "node-action-btn node-action-retry",
                        disabled: !canRetryNode || retryInProgress,
                        onClick: function (event) {
                          event.stopPropagation();
                          if (canRetryNode && props.onRetryNode) {
                            props.onRetryNode(node.id);
                          }
                        },
                        title: canRetryNode
                          ? "Retry this module"
                          : "Retry is available when this module is failed or up-for-retry",
                      },
                      h(RetryIcon),
                      h("span", null, retryInProgress ? "Retrying..." : "Retry")
                    )
                  : null,
                h(
                  "button",
                  {
                    type: "button",
                    className: "node-action-btn node-action-logs",
                    disabled: !hasLogs,
                    onClick: function (event) {
                      event.stopPropagation();
                      if (hasLogs && props.onOpenNodeLogs) {
                        props.onOpenNodeLogs(node.id);
                      }
                    },
                    title: hasLogs ? "View logs" : "Logs available after task starts",
                  },
                  h(LogsIcon),
                  h("span", null, "Logs")
                ),
                !props.readOnly
                  ? h(
                      "button",
                      {
                        type: "button",
                        className: "node-action-btn node-action-delete",
                        onClick: function (event) {
                          event.stopPropagation();
                          props.onDeleteNode(node.id);
                        },
                        title: "Delete node",
                      },
                      h(TrashIcon),
                      h("span", null, "Delete")
                    )
                  : null
              )
            )
          );
        })
      ),
      props.nodes.length === 0
        ? h("div", { className: "canvas-drop-hint" }, "Drag modules from the left panel into this canvas.")
        : null,
      h(
        "div",
        { className: "zoom-controls" },
        h(
          "button",
          {
            type: "button",
            onClick: function () {
              setZoomAtCenter(clamp(viewportRef.current.scale + 0.1, MIN_SCALE, MAX_SCALE));
            },
            title: "Zoom in",
          },
          "+"
        ),
        h(
          "button",
          {
            type: "button",
            onClick: function () {
              setZoomAtCenter(clamp(viewportRef.current.scale - 0.1, MIN_SCALE, MAX_SCALE));
            },
            title: "Zoom out",
          },
          "-"
        ),
        h(
          "button",
          {
            type: "button",
            onClick: function () {
              setViewport({ x: 120, y: 70, scale: 1 });
            },
            title: "Reset view",
          },
          "Reset"
        ),
        h("span", null, Math.round(viewport.scale * 100) + "%")
      )
    );
  }

  window.PipelineCanvas = PipelineCanvas;
})();
