from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from .core import DataPacket


class Stage(Protocol):
    """Minimal contract for pipeline stages."""

    name: str

    async def __call__(self, packet: DataPacket) -> dict:
        ...


_STAGE_REGISTRY: dict[str, Callable[[], Stage]] = {}


def register_stage(name: str) -> Callable[[Callable[[], Stage]], Callable[[], Stage]]:
    """Decorator to register stage factories by name."""

    def _inner(factory: Callable[[], Stage]) -> Callable[[], Stage]:
        _STAGE_REGISTRY[name] = factory
        return factory

    return _inner


def build_stage(name: str) -> Stage:
    try:
        return _STAGE_REGISTRY[name]()
    except KeyError as exc:
        available = ", ".join(sorted(_STAGE_REGISTRY)) or "<none>"
        raise ValueError(f"Unknown stage '{name}'. Available: {available}") from exc


def list_stages() -> list[str]:
    return sorted(_STAGE_REGISTRY)
