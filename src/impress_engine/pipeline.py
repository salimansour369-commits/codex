from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from .core import DataPacket, StageResult
from .plugins import Stage


@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_base_s: float = 0.01

    def delay_for_attempt(self, attempt: int) -> float:
        return self.backoff_base_s * (2 ** (attempt - 1))


class Pipeline:
    """Async pipeline with retries and observability hooks."""

    def __init__(
        self,
        stages: list[Stage],
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._stages = stages
        self._retry = retry_policy or RetryPolicy()

    async def run(self, packet: DataPacket) -> tuple[DataPacket, list[StageResult]]:
        results: list[StageResult] = []

        for stage in self._stages:
            result = await self._run_stage_with_retry(stage, packet)
            results.append(result)
            if not result.ok:
                packet.tags.add("pipeline:failed")
                return packet, results

        packet.tags.add("pipeline:complete")
        return packet, results

    async def _run_stage_with_retry(self, stage: Stage, packet: DataPacket) -> StageResult:
        last_error: Exception | None = None

        for attempt in range(1, self._retry.max_attempts + 1):
            t0 = time.perf_counter()
            try:
                details = await stage(packet)
                duration_ms = (time.perf_counter() - t0) * 1000
                return StageResult(stage.name, True, duration_ms, details)
            except Exception as exc:  # stage-level fault boundary
                last_error = exc
                if attempt < self._retry.max_attempts:
                    await asyncio.sleep(self._retry.delay_for_attempt(attempt))
                else:
                    duration_ms = (time.perf_counter() - t0) * 1000
                    return StageResult(
                        stage.name,
                        False,
                        duration_ms,
                        {
                            "error": str(exc),
                            "attempts": attempt,
                            "error_type": type(exc).__name__,
                        },
                    )

        raise RuntimeError(f"Unreachable retry state for stage={stage.name!r}: {last_error!r}")


class ValidateRequiredFields:
    name = "validate_required_fields"

    def __init__(self, required: set[str]) -> None:
        self.required = required

    async def __call__(self, packet: DataPacket) -> dict[str, Any]:
        missing = sorted(self.required - set(packet.payload))
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
        return {"required_fields": sorted(self.required)}


class ComputePriorityScore:
    name = "compute_priority_score"

    async def __call__(self, packet: DataPacket) -> dict[str, Any]:
        complexity = len(str(packet.payload.get("description", "")))
        urgency = int(packet.payload.get("urgency", 1))
        customer_tier = int(packet.payload.get("customer_tier", 1))

        score = round((complexity * 0.1) + (urgency * 2.0) + (customer_tier * 1.5), 2)
        packet.payload["priority_score"] = score
        packet.tags.add("scored")
        return {"priority_score": score}


class RouteByScore:
    name = "route_by_score"

    async def __call__(self, packet: DataPacket) -> dict[str, Any]:
        score = float(packet.payload.get("priority_score", 0))
        queue = "critical" if score >= 15 else "standard"
        packet.payload["route"] = queue
        packet.tags.add(f"route:{queue}")
        return {"route": queue}
