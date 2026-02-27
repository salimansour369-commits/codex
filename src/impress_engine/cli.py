from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from .core import DataPacket
from .pipeline import ComputePriorityScore, Pipeline, RetryPolicy, RouteByScore, ValidateRequiredFields


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="impress-engine",
        description="Run a high-signal async processing pipeline demo.",
    )
    parser.add_argument("--description", default="Intermittent API failures across regions")
    parser.add_argument("--urgency", type=int, default=4)
    parser.add_argument("--customer-tier", type=int, default=3)
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

    pipeline = Pipeline(
        stages=[
            ValidateRequiredFields({"description", "urgency", "customer_tier"}),
            ComputePriorityScore(),
            RouteByScore(),
        ],
        retry_policy=RetryPolicy(max_attempts=2, backoff_base_s=0.005),
    )

    final_packet, stage_results = await pipeline.run(packet)

    if args.json:
        print(
            json.dumps(
                {
                    "packet": {
                        "id": final_packet.id,
                        "payload": final_packet.payload,
                        "tags": sorted(final_packet.tags),
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
    print(f"packet_id={final_packet.id}")
    print(f"tags={sorted(final_packet.tags)}")
    for r in stage_results:
        state = "OK" if r.ok else "FAIL"
        print(f" - [{state}] {r.stage_name:<24} {r.duration_ms:7.2f}ms  {r.details}")
    print(f"final_payload={final_packet.payload}")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
