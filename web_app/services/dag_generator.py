from __future__ import annotations

import pprint
import re
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from services import template_registry


def sanitize_identifier(raw: str, prefix: str = "item") -> str:
    clean = re.sub(r"[^A-Za-z0-9_]+", "_", raw or "")
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = prefix
    if not re.match(r"^[A-Za-z_]", clean):
        clean = f"{prefix}_{clean}"
    return clean.lower()


def to_python_literal(value: Any) -> str:
    return pprint.pformat(value, width=120, sort_dicts=False)


def _dataform_first_tag_task_id(tags: Any) -> str:
    tag_list = tags if isinstance(tags, list) else []
    first = str(tag_list[0]) if tag_list else "tag"
    safe = "".join(ch if ch.isalnum() else "_" for ch in first.lower())
    safe = "_".join(part for part in safe.split("_") if part)
    return f"dataform_{safe or 'tag'}"


def _dataform_tag_task_ids(tags: Any) -> list[str]:
    values: list[str]
    if isinstance(tags, list):
        values = [str(item).strip() for item in tags]
    elif isinstance(tags, str):
        values = [part.strip() for part in tags.split(",")]
    else:
        values = []

    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        task_id = _dataform_first_tag_task_id([value])
        if task_id not in seen:
            seen.add(task_id)
            unique.append(task_id)
    return unique


def _coerce_start_date_parts(start_date: Any) -> tuple[int, int, int]:
    raw = str(start_date or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        try:
            year, month, day = raw.split("-")
            return int(year), int(month), int(day)
        except ValueError:
            pass
    return 2026, 1, 1


class DagGenerator:
    def __init__(self, template_dir: Path) -> None:
        self.template_dir = template_dir
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=StrictUndefined,
        )
        self.env.filters["pyrepr"] = to_python_literal

    def generate(self, payload: dict[str, Any], execution_order: list[str]) -> dict[str, Any]:
        pipeline = payload["pipeline"]
        nodes = payload["nodes"]
        edges = payload.get("edges", [])
        node_map = {node["id"]: node for node in nodes}

        imports: set[str] = set()
        module_blocks: list[str] = []
        node_task_map: dict[str, list[str]] = {}

        node_plan: dict[str, dict[str, str]] = {}
        for index, node_id in enumerate(execution_order):
            node = node_map[node_id]
            template_name = template_registry.get_template_for_node(node)
            sanitized_node_id = sanitize_identifier(node_id, prefix=f"node_{index + 1}")

            task_id = f"{node['type']}_{index + 1}_{sanitized_node_id}"
            function_name = f"run_{sanitized_node_id}"
            task_var = f"task_{sanitized_node_id}"
            task_entry_var = f"{task_var}_entry"
            task_exit_var = f"{task_var}_exit"

            if template_name == "module_dataform_tags.py.j2":
                entry_ref = task_entry_var
                exit_ref = task_exit_var
                entry_task_id_literal = _dataform_first_tag_task_id(node.get("config", {}).get("dataform_tags", []))
                tag_task_ids = _dataform_tag_task_ids(node.get("config", {}).get("dataform_tags", []))
                node_task_map[node_id] = tag_task_ids or [entry_task_id_literal]
            else:
                entry_ref = task_var
                exit_ref = task_var
                entry_task_id_literal = task_id
                node_task_map[node_id] = [task_id]

            node_plan[node_id] = {
                "template_name": template_name,
                "task_id": task_id,
                "function_name": function_name,
                "task_var": task_var,
                "task_entry_var": task_entry_var,
                "task_exit_var": task_exit_var,
                "entry_ref": entry_ref,
                "exit_ref": exit_ref,
                "entry_task_id_literal": entry_task_id_literal,
            }

            imports.update(template_registry.get_imports_for_module(node["type"], template_name))

        outgoing_by_source: dict[str, list[str]] = {}
        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if source not in node_plan or target not in node_plan:
                continue
            outgoing_by_source.setdefault(source, []).append(target)

        descendants_cache: dict[str, set[str]] = {}

        def collect_descendants_including_self(node_id: str) -> set[str]:
            cached = descendants_cache.get(node_id)
            if cached is not None:
                return set(cached)

            collected: set[str] = {node_id}
            for child_id in outgoing_by_source.get(node_id, []):
                collected.update(collect_descendants_including_self(child_id))
            descendants_cache[node_id] = set(collected)
            return collected

        task_resume_scope_map: dict[str, list[str]] = {}
        for node_id in execution_order:
            entry_task_id = node_plan[node_id]["entry_task_id_literal"]
            descendant_node_ids = collect_descendants_including_self(node_id)
            allowed_task_ids = sorted(
                node_plan[desc_node_id]["entry_task_id_literal"]
                for desc_node_id in descendant_node_ids
                if desc_node_id in node_plan
            )
            task_resume_scope_map[entry_task_id] = allowed_task_ids

        for index, node_id in enumerate(execution_order):
            node = node_map[node_id]
            plan = node_plan[node_id]
            template = self.env.get_template(plan["template_name"])

            router_target_mappings: list[dict[str, str]] = []
            router_source_mappings: list[dict[str, str]] = []
            if node["type"] in {"conditional_router", "if_else_router", "switch_router", "try_catch"}:
                for target_id in outgoing_by_source.get(node_id, []):
                    router_target_mappings.append(
                        {
                            "node_id": target_id,
                            "target_task_id_literal": node_plan[target_id]["entry_task_id_literal"],
                        }
                    )
                for source_node_id in execution_order:
                    router_source_mappings.append(
                        {
                            "node_id": source_node_id,
                            "source_task_id_literal": node_plan[source_node_id]["entry_task_id_literal"],
                        }
                    )

            block = template.render(
                index=index,
                node=node,
                config=node.get("config", {}),
                task_id=plan["task_id"],
                function_name=plan["function_name"],
                task_var=plan["task_var"],
                task_entry_var=plan["task_entry_var"],
                task_exit_var=plan["task_exit_var"],
                router_target_mappings=router_target_mappings,
                router_source_mappings=router_source_mappings,
            ).rstrip()

            module_blocks.append(block)

        dependency_lines: list[str] = []
        seen_dependency_lines: set[str] = set()
        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if source not in node_plan or target not in node_plan:
                continue
            line = f"{node_plan[source]['exit_ref']} >> {node_plan[target]['entry_ref']}"
            if line not in seen_dependency_lines:
                seen_dependency_lines.add(line)
                dependency_lines.append(line)

        dag_function_name = sanitize_identifier(pipeline["dag_id"], prefix="pipeline")
        dag_template = self.env.get_template("pipeline_sequential.py.j2")
        start_date_year, start_date_month, start_date_day = _coerce_start_date_parts(pipeline.get("start_date"))

        task_vars = [node_plan[node_id]["task_var"] for node_id in execution_order]

        rendered = dag_template.render(
            imports=sorted(imports),
            dag_id=pipeline["dag_id"],
            dag_function_name=dag_function_name,
            description=pipeline.get("description", ""),
            start_date_year=start_date_year,
            start_date_month=start_date_month,
            start_date_day=start_date_day,
            schedule=pipeline.get("schedule"),
            catchup=pipeline.get("catchup", False),
            retries=pipeline.get("retries", 0),
            retry_delay_minutes=pipeline.get("retry_delay_minutes", 0),
            alert_emails=pipeline.get("alert_emails", []),
            alert_mode=pipeline.get("alert_mode", "both"),
            tags=pipeline.get("tags", []),
            module_blocks=module_blocks,
            dependency_lines=dependency_lines,
            task_vars=task_vars,
            task_resume_scope_map=task_resume_scope_map,
        )

        return {
            "filename": pipeline["output_filename"],
            "content": rendered.strip() + "\n",
            "execution_order": execution_order,
            "node_task_map": node_task_map,
            "node_primary_task_map": {node_id: node_plan[node_id]["entry_task_id_literal"] for node_id in execution_order},
        }
