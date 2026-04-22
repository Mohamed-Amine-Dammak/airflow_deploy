from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ModuleField:
    name: str
    label: str
    field_type: str = "text"
    required: bool = False
    placeholder: str = ""
    options: list[dict[str, str]] = field(default_factory=list)
    description: str = ""
    show_if: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ModuleDefinition:
    type: str
    display_name: str
    description: str
    template: str
    required_fields: list[str] = field(default_factory=list)
    fields: list[ModuleField] = field(default_factory=list)

