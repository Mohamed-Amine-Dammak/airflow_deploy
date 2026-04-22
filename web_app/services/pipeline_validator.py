from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from services import schema_registry


CONTROL_FORK = "parallel_fork"
CONTROL_JOIN = "parallel_join"
CONTROL_ROUTER = "conditional_router"
CONTROL_IF_ELSE = "if_else_router"
CONTROL_SWITCH = "switch_router"
CONTROL_TRY_CATCH = "try_catch"
CONTROL_PARALLEL_LIMIT = "parallel_limit"
CONTROL_QUORUM_JOIN = "quorum_join"


def _build_node_map(nodes: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    errors: list[str] = []
    node_map: dict[str, dict[str, Any]] = {}
    for node in nodes:
        node_id = node.get("id")
        if not node_id:
            errors.append("Each node must have a non-empty id.")
            continue
        if node_id in node_map:
            errors.append(f"Duplicate node id detected: {node_id}")
            continue
        node_map[node_id] = node
    return node_map, errors


def _validate_module_nodes(nodes: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for node in nodes:
        node_id = node["id"]
        module_type = node["type"]
        if not schema_registry.get_module_schema(module_type):
            errors.append(f"Node '{node_id}' uses unsupported module type '{module_type}'.")
            continue
        module_errors = schema_registry.validate_module_config(module_type, node.get("config", {}))
        errors.extend([f"Node '{node_id}' ({module_type}): {msg}" for msg in module_errors])
    return errors


def _topological_order(
    nodes: list[dict[str, Any]], edges: list[dict[str, str]]
) -> tuple[list[str], list[str], dict[str, int], dict[str, int], dict[str, list[str]], dict[str, list[str]]]:
    node_ids = {node["id"] for node in nodes}
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    out_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    graph: dict[str, list[str]] = defaultdict(list)
    reverse_graph: dict[str, list[str]] = defaultdict(list)
    errors: list[str] = []
    seen_edges: set[tuple[str, str]] = set()

    for edge in edges:
        source = edge.get("source")
        target = edge.get("target")
        if source not in node_ids:
            errors.append(f"Edge source '{source}' does not exist.")
            continue
        if target not in node_ids:
            errors.append(f"Edge target '{target}' does not exist.")
            continue
        if source == target:
            errors.append(f"Self-cycle is not allowed for node '{source}'.")
            continue
        if (source, target) in seen_edges:
            errors.append(f"Duplicate edge '{source}' -> '{target}' is not allowed.")
            continue

        seen_edges.add((source, target))
        graph[source].append(target)
        reverse_graph[target].append(source)
        out_degree[source] += 1
        in_degree[target] += 1

    queue = deque(sorted([nid for nid in node_ids if in_degree[nid] == 0]))
    order: list[str] = []

    mutable_in_degree = dict(in_degree)
    while queue:
        current = queue.popleft()
        order.append(current)
        for nxt in graph[current]:
            mutable_in_degree[nxt] -= 1
            if mutable_in_degree[nxt] == 0:
                queue.append(nxt)

    if len(order) != len(node_ids):
        errors.append("Graph contains a cycle or disconnected cyclic component.")

    return order, errors, in_degree, out_degree, graph, reverse_graph


def _validate_flow_graph(
    nodes: list[dict[str, Any]],
    order: list[str],
    in_degree: dict[str, int],
    out_degree: dict[str, int],
    graph: dict[str, list[str]],
    reverse_graph: dict[str, list[str]],
    node_map: dict[str, dict[str, Any]],
) -> list[str]:
    errors: list[str] = []

    if not nodes:
        errors.append("At least one node is required.")
        return errors

    start_nodes = [nid for nid in order if in_degree.get(nid, 0) == 0]
    end_nodes = [nid for nid in order if out_degree.get(nid, 0) == 0]

    if len(start_nodes) != 1:
        errors.append(f"Pipeline must have exactly one start node; got {len(start_nodes)}.")
    if len(end_nodes) < 1:
        errors.append("Pipeline must have at least one end node.")

    for node_id in order:
        node = node_map[node_id]
        node_type = node["type"]
        indeg = in_degree.get(node_id, 0)
        outdeg = out_degree.get(node_id, 0)

        if node_type == CONTROL_FORK:
            if indeg != 1:
                errors.append(f"Node '{node_id}' (parallel_fork) must have exactly 1 incoming edge.")
            if outdeg < 2:
                errors.append(f"Node '{node_id}' (parallel_fork) must have at least 2 outgoing edges.")
            continue

        if node_type == CONTROL_PARALLEL_LIMIT:
            if indeg != 1:
                errors.append(f"Node '{node_id}' (parallel_limit) must have exactly 1 incoming edge.")
            if outdeg < 2:
                errors.append(f"Node '{node_id}' (parallel_limit) must have at least 2 outgoing edges.")
            max_parallel = node.get("config", {}).get("max_parallel_branches")
            if isinstance(max_parallel, int) and outdeg > max_parallel:
                errors.append(
                    f"Node '{node_id}' (parallel_limit) has {outdeg} outgoing edges but "
                    f"max_parallel_branches={max_parallel}."
                )
            continue

        if node_type == CONTROL_JOIN:
            if indeg < 2:
                errors.append(f"Node '{node_id}' (parallel_join) must have at least 2 incoming edges.")
            if outdeg != 1:
                errors.append(f"Node '{node_id}' (parallel_join) must have exactly 1 outgoing edge.")
            continue

        if node_type == CONTROL_QUORUM_JOIN:
            if indeg < 2:
                errors.append(f"Node '{node_id}' (quorum_join) must have at least 2 incoming edges.")
            if outdeg != 1:
                errors.append(f"Node '{node_id}' (quorum_join) must have exactly 1 outgoing edge.")
            min_success_count = node.get("config", {}).get("min_success_count")
            if isinstance(min_success_count, int) and min_success_count > indeg:
                errors.append(
                    f"Node '{node_id}' (quorum_join) min_success_count={min_success_count} "
                    f"cannot exceed incoming branch count ({indeg})."
                )
            continue

        if node_type == CONTROL_ROUTER:
            if indeg != 1:
                errors.append(f"Node '{node_id}' (conditional_router) must have exactly 1 incoming edge.")
            if outdeg < 2:
                errors.append(f"Node '{node_id}' (conditional_router) must have at least 2 outgoing edges.")
            upstream_nodes = reverse_graph.get(node_id, [])
            if len(upstream_nodes) == 1:
                upstream_id = upstream_nodes[0]
                upstream_type = node_map.get(upstream_id, {}).get("type")
                if upstream_type != CONTROL_JOIN:
                    errors.append(
                        f"Node '{node_id}' (conditional_router) must be placed after a parallel_join. "
                        f"Current upstream node '{upstream_id}' type is '{upstream_type}'."
                    )

            direct_downstream = set(graph.get(node_id, []))
            rules = node.get("config", {}).get("rules")
            if not isinstance(rules, list) or not rules:
                errors.append(f"Node '{node_id}' (conditional_router) must have a non-empty rules list.")
            else:
                for idx, rule in enumerate(rules):
                    if not isinstance(rule, dict):
                        errors.append(
                            f"Node '{node_id}' (conditional_router) rules[{idx}] must be an object."
                        )
                        continue
                    target = str(rule.get("target_branch_node_id", "")).strip()
                    if not target:
                        errors.append(
                            f"Node '{node_id}' (conditional_router) rules[{idx}] target_branch_node_id is required."
                        )
                    elif target not in direct_downstream:
                        errors.append(
                            f"Node '{node_id}' (conditional_router) rules[{idx}] target '{target}' "
                            "must be one of the router direct downstream nodes."
                        )

            # Only the router controls branching: each direct target must have router as sole upstream.
            for target_id in direct_downstream:
                other_sources = [src for src in reverse_graph.get(target_id, []) if src != node_id]
                if other_sources:
                    errors.append(
                        f"Node '{target_id}' is a conditional router target but also has other upstream nodes: "
                        f"{', '.join(other_sources)}."
                    )
            continue

        if node_type == CONTROL_IF_ELSE:
            if indeg != 1:
                errors.append(f"Node '{node_id}' (if_else_router) must have exactly 1 incoming edge.")
            if outdeg != 2:
                errors.append(f"Node '{node_id}' (if_else_router) must have exactly 2 outgoing edges.")

            direct_downstream = set(graph.get(node_id, []))
            true_target = str(node.get("config", {}).get("true_branch_node_id", "")).strip()
            false_target = str(node.get("config", {}).get("false_branch_node_id", "")).strip()
            if not true_target or not false_target:
                errors.append(f"Node '{node_id}' (if_else_router) requires both true and false branch targets.")
            elif true_target == false_target:
                errors.append(f"Node '{node_id}' (if_else_router) true/false targets must be different.")
            for branch_target in (true_target, false_target):
                if branch_target and branch_target not in direct_downstream:
                    errors.append(
                        f"Node '{node_id}' (if_else_router) target '{branch_target}' "
                        "must be one of the router direct downstream nodes."
                    )

            for target_id in direct_downstream:
                other_sources = [src for src in reverse_graph.get(target_id, []) if src != node_id]
                if other_sources:
                    errors.append(
                        f"Node '{target_id}' is an if_else_router target but also has other upstream nodes: "
                        f"{', '.join(other_sources)}."
                    )
            continue

        if node_type == CONTROL_SWITCH:
            if indeg != 1:
                errors.append(f"Node '{node_id}' (switch_router) must have exactly 1 incoming edge.")
            if outdeg < 2:
                errors.append(f"Node '{node_id}' (switch_router) must have at least 2 outgoing edges.")

            direct_downstream = set(graph.get(node_id, []))
            cases = node.get("config", {}).get("cases")
            if not isinstance(cases, list) or not cases:
                errors.append(f"Node '{node_id}' (switch_router) must have a non-empty cases list.")
            else:
                for idx, case_item in enumerate(cases):
                    if not isinstance(case_item, dict):
                        errors.append(f"Node '{node_id}' (switch_router) cases[{idx}] must be an object.")
                        continue
                    target = str(case_item.get("target_branch_node_id", "")).strip()
                    if not target:
                        errors.append(
                            f"Node '{node_id}' (switch_router) cases[{idx}] target_branch_node_id is required."
                        )
                    elif target not in direct_downstream:
                        errors.append(
                            f"Node '{node_id}' (switch_router) cases[{idx}] target '{target}' "
                            "must be one of the router direct downstream nodes."
                        )

            default_target = str(node.get("config", {}).get("default_branch_node_id", "")).strip()
            if not default_target:
                errors.append(f"Node '{node_id}' (switch_router) default_branch_node_id is required.")
            elif default_target not in direct_downstream:
                errors.append(
                    f"Node '{node_id}' (switch_router) default target '{default_target}' "
                    "must be one of the router direct downstream nodes."
                )

            for target_id in direct_downstream:
                other_sources = [src for src in reverse_graph.get(target_id, []) if src != node_id]
                if other_sources:
                    errors.append(
                        f"Node '{target_id}' is a switch_router target but also has other upstream nodes: "
                        f"{', '.join(other_sources)}."
                    )
            continue

        if node_type == CONTROL_TRY_CATCH:
            if indeg != 1:
                errors.append(f"Node '{node_id}' (try_catch) must have exactly 1 incoming edge.")
            if outdeg != 2:
                errors.append(f"Node '{node_id}' (try_catch) must have exactly 2 outgoing edges.")

            direct_downstream = set(graph.get(node_id, []))
            success_target = str(node.get("config", {}).get("success_branch_node_id", "")).strip()
            catch_target = str(node.get("config", {}).get("catch_branch_node_id", "")).strip()
            if not success_target or not catch_target:
                errors.append(f"Node '{node_id}' (try_catch) requires both success and catch branch targets.")
            elif success_target == catch_target:
                errors.append(f"Node '{node_id}' (try_catch) success/catch targets must be different.")
            for branch_target in (success_target, catch_target):
                if branch_target and branch_target not in direct_downstream:
                    errors.append(
                        f"Node '{node_id}' (try_catch) target '{branch_target}' "
                        "must be one of the router direct downstream nodes."
                    )

            for target_id in direct_downstream:
                other_sources = [src for src in reverse_graph.get(target_id, []) if src != node_id]
                if other_sources:
                    errors.append(
                        f"Node '{target_id}' is a try_catch target but also has other upstream nodes: "
                        f"{', '.join(other_sources)}."
                    )
            continue

        if indeg > 1:
            errors.append(
                f"Node '{node_id}' ({node_type}) has {indeg} incoming edges. "
                "Use a Parallel Join to synchronize multiple branches."
            )
        if outdeg > 1:
            errors.append(
                f"Node '{node_id}' ({node_type}) has {outdeg} outgoing edges. "
                "Use a Parallel Fork, Parallel Limit, Conditional Router, If/Else Router, Switch Router, or Try/Catch Router to branch."
            )

    return errors


def validate_pipeline(raw_payload: dict[str, Any]) -> dict[str, Any]:
    normalized_payload, normalize_errors = schema_registry.normalize_pipeline_payload(raw_payload)
    errors: list[str] = list(normalize_errors)

    pipeline = normalized_payload["pipeline"]
    nodes = normalized_payload["nodes"]
    edges = normalized_payload["edges"]

    errors.extend(schema_registry.validate_pipeline_settings(pipeline))
    node_map, node_errors = _build_node_map(nodes)
    errors.extend(node_errors)

    if not node_errors:
        errors.extend(_validate_module_nodes(nodes))

    order: list[str] = []
    if not errors:
        order, graph_errors, in_degree, out_degree, graph, reverse_graph = _topological_order(nodes, edges)
        errors.extend(graph_errors)
        if not graph_errors:
            errors.extend(_validate_flow_graph(nodes, order, in_degree, out_degree, graph, reverse_graph, node_map))
            if not errors:
                for node_id in order:
                    if node_id not in node_map:
                        errors.append(f"Execution order references unknown node '{node_id}'.")

    return {
        "valid": not errors,
        "errors": errors,
        "normalized_payload": normalized_payload,
        "execution_order": order,
    }
