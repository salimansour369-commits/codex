# Impress Engine

A production-style starter project that demonstrates:

- **Async orchestration** with deterministic stage ordering.
- **Retry policy** with exponential backoff.
- **Fault boundaries** at stage level (pipeline fails fast, cleanly).
- **Typed datamodels** and structured stage result envelopes.
- **CLI UX** for both human-readable and JSON output.
- **Unit tests** for success and failure paths.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e . pytest
pytest -q
impress-engine --json
```

## Architecture

`DataPacket` flows through ordered stages:

1. `ValidateRequiredFields`
2. `ComputePriorityScore`
3. `RouteByScore`

Each stage returns details that are captured in `StageResult`.
If a stage fails, the pipeline stops and tags the packet with `pipeline:failed`.

## Example

```bash
impress-engine --description "Global auth outage" --urgency 5 --customer-tier 4
```

Sample output:

```text
🚀 impress-engine run complete
packet_id=...
tags=['pipeline:complete', 'route:critical', 'scored']
 - [OK] validate_required_fields      0.02ms  {'required_fields': ['customer_tier', 'description', 'urgency']}
 - [OK] compute_priority_score        0.01ms  {'priority_score': 20.8}
 - [OK] route_by_score                0.01ms  {'route': 'critical'}
final_payload={'description': 'Global auth outage', 'urgency': 5, 'customer_tier': 4, 'priority_score': 20.8, 'route': 'critical'}
```
