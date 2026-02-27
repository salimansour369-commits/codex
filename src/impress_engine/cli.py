from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from .core import DataPacket
from .pipeline import (
    ComputePriorityScore,
    FlakyEnricher,
    Pipeline,
    RetryPolicy,
    RouteByScore,
    ValidateRequiredFields,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="impress-engine",
        description="Run a high-signal async processing pipeline demo.",
    )
    parser.add_argument("--description", default="Intermittent API failures across regions")
    parser.add_argument("--urgency", type=int, default=4)
    parser.add_argument("--customer-tier", type=int, default=3)
    parser.add_argument("--with-flaky-enricher", action="store_true")
    parser.add_argument("--flaky-failures", type=int, default=1)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args()


async def _run() -> None:
    args = parse_args()

    packet = DataPacket(
        id=str(uuid.uuid4()),
        payload={
            "description": args.description,
            "urgency": args.urgency,
            "customer_tier": args.customer_tier,
        },
    )

    stages = [
        ValidateRequiredFields({"description", "urgency", "customer_tier"}),
        ComputePriorityScore(),
    ]
    if args.with_flaky_enricher:
        stages.append(FlakyEnricher(args.flaky_failures))
    stages.append(RouteByScore())

    pipeline = Pipeline(
        stages=stages,
        retry_policy=RetryPolicy(max_attempts=args.max_attempts, backoff_base_s=0.005),
    )

    stage_results, report = await pipeline.run_with_report(packet)

    if args.json:
        print(
            json.dumps(
                {
                    "packet": {
                        "id": packet.id,
                        "payload": packet.payload,
                        "tags": sorted(packet.tags),
                    },
                    "report": {
                        "ok": report.ok,
                        "total_duration_ms": round(report.total_duration_ms, 3),
                        "completed_stages": report.completed_stages,
                        "failed_stage": report.failed_stage,
                    },
                    "stages": [
                        {
                            "name": r.stage_name,
                            "ok": r.ok,
                            "duration_ms": round(r.duration_ms, 3),
                            "details": r.details,
                        }
                        for r in stage_results
                    ],
                },
                indent=2,
            )
        )
        return

    print("🚀 impress-engine run complete")
    print(f"packet_id={packet.id}")
    print(f"tags={sorted(packet.tags)}")
    print(
        "report="
        f"ok:{report.ok} "
        f"total:{report.total_duration_ms:.2f}ms "
        f"completed:{report.completed_stages} "
        f"failed_stage:{report.failed_stage}"
    )
    for r in stage_results:
        state = "OK" if r.ok else "FAIL"
        print(f" - [{state}] {r.stage_name:<24} {r.duration_ms:7.2f}ms  {r.details}")
    print(f"final_payload={packet.payload}")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
