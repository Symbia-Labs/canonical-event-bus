import hashlib
import json
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

APP = FastAPI(title="Canonical Event Bus (CEG)")

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / ".ceg"
LOG_PATH = STATE_DIR / "log" / "events.ndjson"
STATE_PATH = STATE_DIR / "derived_state.json"
PROJ_PATH = STATE_DIR / "projections" / "recency.json"

LOCK = threading.Lock()


class EventIn(BaseModel):
    type: str
    key: str
    body: Optional[Dict[str, Any]] = None


class DecideIn(BaseModel):
    task: Dict[str, Any]
    key: Optional[str] = None
    stale_after_ms: Optional[int] = 300_000


def _ensure_dirs():
    for p in [STATE_DIR, LOG_PATH.parent, PROJ_PATH.parent]:
        p.mkdir(parents=True, exist_ok=True)


def _hash_env(data: Dict[str, Any]) -> str:
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {"seq": 0, "last_hash": None, "last_ts": 0}
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {"seq": 0, "last_hash": None, "last_ts": 0}


def _persist_state(state: Dict[str, Any]):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


def _load_projection() -> Dict[str, Any]:
    if not PROJ_PATH.exists():
        return {"recency": {}, "counters": {}, "updated_at_ms": 0}
    try:
        return json.loads(PROJ_PATH.read_text())
    except Exception:
        return {"recency": {}, "counters": {}, "updated_at_ms": 0}


def _persist_projection(proj: Dict[str, Any]):
    PROJ_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROJ_PATH.write_text(json.dumps(proj, indent=2))


def _append_event(env: Dict[str, Any], state: Dict[str, Any]):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(env) + "\n")
        f.flush()
    _persist_state(state)


def _rebuild_projection() -> Dict[str, Any]:
    recency = {}
    counters = {}
    processed = 0
    if LOG_PATH.exists():
        for line in LOG_PATH.read_text().splitlines():
            try:
                ev = json.loads(line)
            except Exception:
                continue
            key = ev.get("key")
            if not key:
                continue
            counters[key] = counters.get(key, 0) + 1
            recency[key] = {
                "seq": ev.get("seq"),
                "ts": ev.get("ts"),
                "type": ev.get("type"),
                "hash": ev.get("hash"),
            }
            processed += 1
    now_ms = int(time.time() * 1000)
    proj = {"recency": recency, "counters": counters, "updated_at_ms": now_ms, "processed": processed}
    _persist_projection(proj)
    return proj


@APP.get("/health")
def health():
    return {"ok": True}


@APP.get("/seb/status")
def status():
    _ensure_dirs()
    state = _load_state()
    proj = _load_projection()
    return {
        "seq": state.get("seq"),
        "last_hash": state.get("last_hash"),
        "last_ts": state.get("last_ts"),
        "log_path": str(LOG_PATH),
        "projection": {
            "updated_at_ms": proj.get("updated_at_ms"),
            "keys": len((proj.get("recency") or {}).keys()),
        },
    }


@APP.post("/seb/events")
def append_event(event: EventIn):
    _ensure_dirs()
    with LOCK:
        state = _load_state()
        seq = int(state.get("seq", 0)) + 1
        ts_ns = max(int(time.time_ns()), int(state.get("last_ts", 0)) + 1)
        base = {
            "id": str(uuid.uuid4()),
            "seq": seq,
            "ts": ts_ns,
            "type": event.type,
            "key": event.key,
            "body": event.body or {},
            "prev_hash": state.get("last_hash"),
        }
        env = dict(base)
        env["hash"] = _hash_env(base)
        new_state = {"seq": seq, "last_hash": env["hash"], "last_ts": ts_ns}
        _append_event(env, new_state)
        proj = _rebuild_projection()
    return {"envelope": env, "state": new_state, "projection": proj.get("recency", {}).get(event.key)}


@APP.post("/seb/projections/rebuild")
def rebuild_projection():
    _ensure_dirs()
    with LOCK:
        start = time.perf_counter()
        proj = _rebuild_projection()
        dur = int((time.perf_counter() - start) * 1000)
    return {"projection": proj, "duration_ms": dur}


@APP.get("/seb/audit")
def audit():
    _ensure_dirs()
    if not LOG_PATH.exists():
        return {"ok": True, "entries": 0, "reason": "log_missing"}
    prev_hash = None
    entries = 0
    mismatch = None
    for line in LOG_PATH.read_text().splitlines():
        try:
            ev = json.loads(line)
        except Exception:
            mismatch = {"seq": entries + 1, "reason": "unparseable_line"}
            break
        if prev_hash and ev.get("prev_hash") != prev_hash:
            mismatch = {"seq": ev.get("seq"), "reason": "prev_hash_mismatch"}
            break
        computed = _hash_env({k: v for k, v in ev.items() if k != "hash"})
        if ev.get("hash") != computed:
            mismatch = {"seq": ev.get("seq"), "reason": "hash_mismatch"}
            break
        prev_hash = ev.get("hash")
        entries += 1
    return {"ok": mismatch is None, "entries": entries, "last_hash": prev_hash, "mismatch": mismatch}


@APP.post("/seb/decide")
def decide(req: DecideIn):
    _ensure_dirs()
    key = req.key or req.task.get("key")
    proj = _load_projection()
    rec = (proj.get("recency") or {}).get(key) if key else None
    now_ms = int(time.time() * 1000)
    stale_after = req.stale_after_ms or 0
    stale = False
    if rec and stale_after:
        rec_ms = int((rec.get("ts") or 0) / 1_000_000)
        stale = now_ms - rec_ms > stale_after

    deterministic = {"decision": "abstain", "reasons": []}
    if rec:
        if rec.get("type") == "deny":
            deterministic = {"decision": "deny", "reasons": ["last_event_deny"]}
        elif rec.get("type") == "allow":
            deterministic = {"decision": "allow", "reasons": ["last_event_allow"]}
    if stale:
        deterministic = {"decision": "escalate", "reasons": ["projection_stale"]}

    probabilistic = {"decision": "abstain", "confidence": 0.0, "notes": "stub; SEB-only context"}
    arbiter = {"decision": "escalate", "reason": "default"}
    if deterministic["decision"] in ("allow", "deny"):
        arbiter = {"decision": deterministic["decision"], "reason": "deterministic"}
    elif deterministic["decision"] == "escalate":
        arbiter = {"decision": "escalate", "reason": "projection_stale"}
    else:
        arbiter = {"decision": "escalate", "reason": "insufficient_abstraction"}

    return {
        "task": req.task,
        "key": key,
        "projection": rec,
        "deterministic": deterministic,
        "probabilistic": probabilistic,
        "arbiter": arbiter,
        "projection_updated_at_ms": proj.get("updated_at_ms"),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(APP, host="127.0.0.1", port=8000)
