> **This repository is archived.** It is part of Symbia v1, which is no longer maintained. Symbia v2 replaces it: [github.com/Symbia-Labs/symbia](https://github.com/Symbia-Labs/symbia)

# Canonical Event Bus (CEG) — Minimal Open Source Stub

Small, self-contained FastAPI app that implements the bare CEG primitives:

- append-only event log (hash-chained, fsync)
- rebuildable recency/counter projection
- simple decision stub (allow/deny/abstain from projections only)
- integrity audit

## Quickstart

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

python ceg.py   # serves on http://127.0.0.1:8000
```

## API (minimal)

- `GET /health` — liveness
- `GET /ceg/status` — seq/hash, projection freshness
- `POST /ceg/events` — append immutable envelope; body: `{"type":"allow|deny|...","key":"acct:123","body":{...}}`
- `POST /ceg/projections/rebuild` — rebuild projections from log (returns duration_ms)
- `POST /ceg/decide` — decision stub using projections only; body: `{"task":{"op":"...","key":"acct:123"}}`
- `GET /ceg/audit` — hash-chain integrity check over the log

## State layout

Under the repo:

- `./.ceg/log/events.ndjson` — append-only authoritative log
- `./.ceg/projections/recency.json` — rebuildable recency/counter projection
- `./.ceg/derived_state.json` — non-authoritative seq/hash tracker

Delete `./.ceg/projections/*` and call `/ceg/projections/rebuild` to prove rebuildability. The log is authoritative; projections are derived.

## Notes

- No auth, no external deps beyond FastAPI/uvicorn.
- Decisions are CEG-only (no payload fetch, no retrieval).
- Monotonic time and hash chaining included; integrity check via `/ceg/audit`.
