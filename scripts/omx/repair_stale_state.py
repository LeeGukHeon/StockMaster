#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

TRACKED_WORKFLOW_SKILLS = {
    "autopilot",
    "autoresearch",
    "team",
    "ralph",
    "ultrawork",
    "ultraqa",
    "ralplan",
    "deep-interview",
}
TERMINAL_PHASES = {"complete", "completed", "failed", "cancelled", "canceled"}
SKILL_ACTIVE_FILE = "skill-active-state.json"
STATE_FILE_RE = re.compile(r"^(?P<mode>.+)-state\.json$")
SKILL_PATH_RE = re.compile(r"^(?P<root>.*?/\.omx/state)(?:/sessions/(?P<session>[^/]+))?/skill-active-state\.json$")
STALE_NONCURRENT_SESSION_AFTER = timedelta(hours=2)


@dataclass
class Change:
    path: str
    reason: str


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


def safe_str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def parse_timestamp(value: Any) -> datetime | None:
    text = safe_str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def session_id_from_state_path(path: Path) -> str:
    match = re.search(r"/\.omx/state/sessions/([^/]+)/", str(path))
    return match.group(1) if match else ""


def read_current_session_id(state_root: Path) -> str:
    session_file = state_root / "session.json"
    data = read_json(session_file) if session_file.exists() else None
    if not isinstance(data, dict):
        return ""
    return safe_str(data.get("session_id")).strip()


def is_stale_noncurrent_session(path: Path, data: dict[str, Any], current_session_id: str, current_time: datetime) -> bool:
    session_id = session_id_from_state_path(path)
    if not session_id or not current_session_id or session_id == current_session_id:
        return False
    if data.get("active") is not True:
        return False
    reference = parse_timestamp(data.get("updated_at")) or parse_timestamp(data.get("last_turn_at")) or parse_timestamp(data.get("started_at"))
    if reference is None:
        return False
    return current_time - reference > STALE_NONCURRENT_SESSION_AFTER


def is_terminal_phase(phase: Any) -> bool:
    return safe_str(phase).strip().lower() in TERMINAL_PHASES


def normalize_active_entries(state: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    raw_entries = state.get("active_skills")
    if isinstance(raw_entries, list):
        for raw in raw_entries:
            if not isinstance(raw, dict):
                continue
            skill = safe_str(raw.get("skill")).strip()
            if not skill or raw.get("active") is False:
                continue
            session_id = safe_str(raw.get("session_id")).strip()
            key = (skill, session_id)
            if key in seen:
                continue
            seen.add(key)
            entries.append({
                **raw,
                "skill": skill,
                "phase": safe_str(raw.get("phase")).strip(),
                "session_id": session_id or None,
                "thread_id": safe_str(raw.get("thread_id")).strip() or None,
                "turn_id": safe_str(raw.get("turn_id")).strip() or None,
                "active": True,
            })
    top_skill = safe_str(state.get("skill")).strip()
    if not entries and state.get("active") is True and top_skill:
        session_id = safe_str(state.get("session_id")).strip()
        entries.append({
            "skill": top_skill,
            "phase": safe_str(state.get("phase")).strip(),
            "session_id": session_id or None,
            "thread_id": safe_str(state.get("thread_id")).strip() or None,
            "turn_id": safe_str(state.get("turn_id")).strip() or None,
            "active": True,
            "activated_at": safe_str(state.get("activated_at")).strip() or None,
            "updated_at": safe_str(state.get("updated_at")).strip() or None,
        })
    return entries


def release_input_lock(state: dict[str, Any], timestamp: str, reason: str) -> bool:
    input_lock = state.get("input_lock")
    if not isinstance(input_lock, dict):
        return False
    changed = input_lock.get("active") is not False
    if safe_str(input_lock.get("released_at")).strip() == "":
        changed = True
    if safe_str(input_lock.get("exit_reason")).strip() == "":
        changed = True
    input_lock["active"] = False
    input_lock.setdefault("acquired_at", timestamp)
    input_lock.setdefault("released_at", timestamp)
    input_lock.setdefault("exit_reason", reason)
    state["input_lock"] = input_lock
    return changed


def sanitize_mode_state(path: Path, data: dict[str, Any], timestamp: str, current_session_id: str, current_time: datetime) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    next_data = dict(data)
    if is_stale_noncurrent_session(path, next_data, current_session_id, current_time):
        next_data["active"] = False
        next_data["current_phase"] = safe_str(next_data.get("current_phase")).strip() or "cancelled"
        next_data.setdefault("completed_at", timestamp)
        next_data["updated_at"] = timestamp
        reasons.append("stale non-current session forced inactive")
    if next_data.get("active") is True and is_terminal_phase(next_data.get("current_phase")):
        next_data["active"] = False
        next_data.setdefault("completed_at", timestamp)
        if safe_str(next_data.get("updated_at")).strip() == "":
            next_data["updated_at"] = timestamp
        reasons.append(f"terminal phase {safe_str(next_data.get('current_phase')).strip()} forced inactive")
    if path.name == "deep-interview-state.json" and (next_data.get("active") is not True or is_terminal_phase(next_data.get("current_phase"))):
        if release_input_lock(next_data, timestamp, "sanitize"):
            reasons.append("released stale deep-interview input lock")
    return next_data, reasons


def resolve_mode_state_path(skill_state_path: Path, skill: str, entry_session_id: str | None) -> Path:
    match = SKILL_PATH_RE.match(str(skill_state_path))
    if not match:
        root_dir = skill_state_path.parent
        return root_dir / f"{skill}-state.json"
    root = Path(match.group("root"))
    session_id = entry_session_id or match.group("session")
    if session_id:
        return root / "sessions" / session_id / f"{skill}-state.json"
    return root / f"{skill}-state.json"


def sanitize_skill_state(path: Path, data: dict[str, Any], timestamp: str) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    entries = normalize_active_entries(data)
    kept: list[dict[str, Any]] = []
    for entry in entries:
        skill = entry["skill"]
        if skill not in TRACKED_WORKFLOW_SKILLS:
            kept.append(entry)
            continue
        mode_path = resolve_mode_state_path(path, skill, entry.get("session_id"))
        mode_state = read_json(mode_path) if mode_path.exists() else None
        if not isinstance(mode_state, dict):
            reasons.append(f"dropped stale {skill} entry with missing mode state")
            continue
        if mode_state.get("active") is not True or is_terminal_phase(mode_state.get("current_phase")):
            reasons.append(f"dropped stale {skill} entry with inactive/terminal mode state")
            continue
        mode_phase = safe_str(mode_state.get("current_phase")).strip()
        if mode_phase and mode_phase != safe_str(entry.get("phase")).strip():
            entry = dict(entry)
            entry["phase"] = mode_phase
            reasons.append(f"resynced {skill} phase from mode state")
        kept.append(entry)
    next_state = dict(data)
    if kept:
        preferred_skill = safe_str(next_state.get("skill")).strip()
        primary = next((entry for entry in kept if entry["skill"] == preferred_skill), kept[0])
        next_state["active"] = True
        next_state["skill"] = primary["skill"]
        next_state["phase"] = safe_str(primary.get("phase")).strip() or safe_str(next_state.get("phase")).strip()
        next_state["session_id"] = primary.get("session_id")
        next_state["thread_id"] = primary.get("thread_id")
        next_state["turn_id"] = primary.get("turn_id")
        next_state["active_skills"] = kept
        if reasons:
            next_state["updated_at"] = timestamp
        if next_state["skill"] != "deep-interview":
            if release_input_lock(next_state, timestamp, "handoff"):
                reasons.append("released deep-interview lock on non-deep-interview skill state")
                next_state["updated_at"] = timestamp
        return next_state, reasons
    already_cleared = (next_state.get("active") is False and not normalize_active_entries(next_state))
    input_lock = next_state.get("input_lock") if isinstance(next_state.get("input_lock"), dict) else {}
    if safe_str(next_state.get("phase")).strip().lower() in TERMINAL_PHASES and not input_lock.get("active", False):
        if already_cleared:
            return next_state, reasons
    next_state["active"] = False
    next_state["active_skills"] = []
    if safe_str(next_state.get("phase")).strip().lower() not in TERMINAL_PHASES:
        next_state["phase"] = "completed"
        reasons.append("normalized inactive skill-active phase")
    if safe_str(next_state.get("completed_at")).strip() == "":
        next_state["completed_at"] = timestamp
        reasons.append("stamped missing completed_at on inactive skill-active state")
    if release_input_lock(next_state, timestamp, "sanitize"):
        reasons.append("released stale deep-interview lock on inactive skill-active state")
    if next_state.get("active") is False and not normalize_active_entries(data) and not reasons:
        return next_state, []
    if next_state != data and reasons:
        next_state["updated_at"] = timestamp
    if next_state != data and not reasons:
        reasons.append("cleared stale canonical skill-active state")
        next_state["updated_at"] = timestamp
    elif reasons:
        reasons.append("cleared stale canonical skill-active state")
    return next_state, reasons


def should_process_state_path(path: Path, current_session_id: str, include_other_sessions: bool) -> bool:
    session_id = session_id_from_state_path(path)
    if not session_id:
        return True
    if include_other_sessions:
        return True
    return bool(current_session_id) and session_id == current_session_id


def sanitize_state_tree(project_root: Path, include_other_sessions: bool = False) -> list[Change]:
    state_root = project_root / ".omx" / "state"
    if not state_root.exists():
        return []
    timestamp = now_iso()
    current_time = parse_timestamp(timestamp) or datetime.now(timezone.utc)
    current_session_id = read_current_session_id(state_root)
    changes: list[Change] = []
    state_files = sorted(state_root.rglob("*-state.json"))

    # First normalize regular mode states so skill-active cleanup can trust them.
    for path in state_files:
        if path.name == SKILL_ACTIVE_FILE:
            continue
        if not should_process_state_path(path, current_session_id, include_other_sessions):
            continue
        data = read_json(path)
        if not isinstance(data, dict):
            continue
        updated, reasons = sanitize_mode_state(path, data, timestamp, current_session_id, current_time)
        if reasons and updated != data:
            write_json(path, updated)
            changes.append(Change(str(path), "; ".join(reasons)))

    # Then clean canonical skill-active root/session views.
    for path in state_files:
        if path.name != SKILL_ACTIVE_FILE:
            continue
        if not should_process_state_path(path, current_session_id, include_other_sessions):
            continue
        data = read_json(path)
        if not isinstance(data, dict):
            continue
        updated, reasons = sanitize_skill_state(path, data, timestamp)
        if reasons and updated != data:
            write_json(path, updated)
            changes.append(Change(str(path), "; ".join(reasons)))
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair stale OMX root/session state before hooks or status checks.")
    parser.add_argument("--cwd", default=os.getcwd(), help="Project root that contains .omx/state")
    parser.add_argument("--quiet", action="store_true", help="Suppress normal output; only return process status")
    parser.add_argument(
        "--include-other-sessions",
        action="store_true",
        help="Also repair stale state in non-current session folders. Omit this during normal hook execution.",
    )
    args = parser.parse_args()

    project_root = Path(args.cwd).resolve()
    changes = sanitize_state_tree(project_root, include_other_sessions=args.include_other_sessions)
    if not args.quiet:
        if not changes:
            print(f"[omx-state-repair] clean: {project_root}")
        else:
            print(f"[omx-state-repair] repaired {len(changes)} file(s) under {project_root}")
            for change in changes:
                print(f"- {change.path}: {change.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
