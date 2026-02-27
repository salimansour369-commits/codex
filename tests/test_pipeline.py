from __future__ import annotations

import asyncio

from impress_engine.core import DataPacket
from impress_engine.pipeline import (
    ComputePriorityScore,
    FlakyEnricher,
    Pipeline,
    RetryPolicy,
    RouteByScore,
    ValidateRequiredFields,
)


def test_pipeline_successful_routing_to_critical() -> None:
    packet = DataPacket(
        id="pkt-1",
        payload={
            "description": "P1 outage impacting billing path in all regions",
            "urgency": 5,
            "customer_tier": 4,
        },
    )
    p = Pipeline(
        [
            ValidateRequiredFields({"description", "urgency", "customer_tier"}),
            ComputePriorityScore(),
            RouteByScore(),
        ],
        retry_policy=RetryPolicy(max_attempts=1),
    )

    final_packet, results = asyncio.run(p.run(packet))

    assert all(r.ok for r in results)
    assert final_packet.payload["route"] == "critical"
    assert "pipeline:complete" in final_packet.tags


def test_pipeline_stops_after_validation_failure() -> None:
    packet = DataPacket(
        id="pkt-2",
        payload={"description": "missing fields"},
    )
    p = Pipeline(
        [
            ValidateRequiredFields({"description", "urgency", "customer_tier"}),
            ComputePriorityScore(),
        ],
        retry_policy=RetryPolicy(max_attempts=1),
    )

    final_packet, results = asyncio.run(p.run(packet))

    assert len(results) == 1
    assert results[0].ok is False
    assert "Missing required fields" in results[0].details["error"]
    assert "pipeline:failed" in final_packet.tags


def test_pipeline_retry_recovers_flaky_stage() -> None:
    packet = DataPacket(
        id="pkt-3",
        payload={
            "description": "Intermittent cache timeout",
            "urgency": 3,
            "customer_tier": 2,
        },
    )
    p = Pipeline(
        [
            ValidateRequiredFields({"description", "urgency", "customer_tier"}),
            ComputePriorityScore(),
            FlakyEnricher(failures_before_success=1),
            RouteByScore(),
        ],
        retry_policy=RetryPolicy(max_attempts=3, backoff_base_s=0),
    )

    results, report = asyncio.run(p.run_with_report(packet))

    assert report.ok is True
    assert report.failed_stage is None
    assert report.completed_stages == 4
    assert any(r.stage_name == "flaky_enricher" and r.details["attempt"] == 2 for r in results)
    assert packet.payload["enriched"] is True
    assert "pipeline:complete" in packet.tags
