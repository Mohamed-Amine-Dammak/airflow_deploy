from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PipelineSettings:
    dag_id: str
    description: str = ""
    start_date: str = "2026-01-01"
    schedule: str | None = None
    schedule_mode: str = ""
    catchup: bool = False
    schedule_preset: str = ""
    schedule_cron: str = ""
    interval_value: int | str | None = None
    interval_unit: str = "minutes"
    event_source: str = ""
    custom_schedule_text: str = ""
    retries: int = 0
    retry_delay_minutes: int = 0
    alert_emails: list[str] = field(default_factory=list)
    alert_mode: str = "both"
    tags: list[str] = field(default_factory=list)
    output_filename: str = "generated_pipeline.py"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineSettings":
        raw_catchup = data.get("catchup", False)
        catchup = (
            raw_catchup
            if isinstance(raw_catchup, bool)
            else str(raw_catchup).strip().lower() in {"1", "true", "yes", "y", "on"}
        )
        return cls(
            dag_id=str(data.get("dag_id", "")).strip(),
            description=str(data.get("description", "")).strip(),
            start_date=str(data.get("start_date", "2026-01-01")).strip() or "2026-01-01",
            schedule=data.get("schedule"),
            schedule_mode=str(data.get("schedule_mode", "")).strip(),
            catchup=catchup,
            schedule_preset=str(data.get("schedule_preset", "")).strip(),
            schedule_cron=str(data.get("schedule_cron", "")).strip(),
            interval_value=data.get("interval_value"),
            interval_unit=str(data.get("interval_unit", "minutes")).strip(),
            event_source=str(data.get("event_source", "")).strip(),
            custom_schedule_text=str(data.get("custom_schedule_text", "")).strip(),
            retries=int(data.get("retries", 0)),
            retry_delay_minutes=int(data.get("retry_delay_minutes", 0)),
            alert_emails=list(data.get("alert_emails", [])),
            alert_mode=str(data.get("alert_mode", "both")),
            tags=list(data.get("tags", [])),
            output_filename=str(data.get("output_filename", "generated_pipeline.py")).strip(),
        )


@dataclass(slots=True)
class PipelineNode:
    id: str
    type: str
    label: str
    config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineNode":
        return cls(
            id=str(data.get("id", "")).strip(),
            type=str(data.get("type", "")).strip().lower(),
            label=str(data.get("label", "")).strip(),
            config=dict(data.get("config", {})),
        )


@dataclass(slots=True)
class PipelineEdge:
    source: str
    target: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineEdge":
        return cls(
            source=str(data.get("source", "")).strip(),
            target=str(data.get("target", "")).strip(),
        )


@dataclass(slots=True)
class PipelineDefinition:
    pipeline: PipelineSettings
    nodes: list[PipelineNode]
    edges: list[PipelineEdge]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineDefinition":
        pipeline_data = data.get("pipeline", {})
        nodes_data = data.get("nodes", [])
        edges_data = data.get("edges", [])
        return cls(
            pipeline=PipelineSettings.from_dict(pipeline_data),
            nodes=[PipelineNode.from_dict(item) for item in nodes_data],
            edges=[PipelineEdge.from_dict(item) for item in edges_data],
        )
