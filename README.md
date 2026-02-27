# Impress Engine

A production-style starter project that demonstrates:

- **Async orchestration** with deterministic stage ordering.
- **Retry policy** with exponential backoff.
- **Fault boundaries** at stage level (pipeline fails fast, cleanly).
- **Pipeline execution report** (`ok`, duration, completed stages, failed stage).
- **Typed datamodels** and structured stage result envelopes.
- **CLI UX** for both human-readable and JSON output.
- **Unit tests** for success, failure, and retry behavior.

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
3. `FlakyEnricher` (optional; used to showcase retry recovery)
4. `RouteByScore`

Each stage returns details captured in `StageResult`. The pipeline also emits a
`PipelineReport` with high-level execution metadata.
If a stage fails after retries, the pipeline stops and tags the packet with
`pipeline:failed`.

## Example runs

```bash
# Baseline
impress-engine --description "Global auth outage" --urgency 5 --customer-tier 4

# Retry demo (fails once, succeeds on second attempt)
impress-engine --with-flaky-enricher --flaky-failures 1 --max-attempts 3 --json
```
