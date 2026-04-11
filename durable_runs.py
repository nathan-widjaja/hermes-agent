#!/usr/bin/env python3
"""
Durable Runs store and orchestration helpers.

This module implements the V1 "Execution Spine" persistence layer behind the
user-facing "Durable Runs" feature.  It intentionally lives in a separate
SQLite database from the session store to avoid recreating the same WAL
contention class that Hermes already documents in hermes_state.py.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = get_hermes_home() / "execution_state.db"
SCHEMA_VERSION = 1

ACTIVE_RUN_STATUSES = (
    "queued",
    "running",
    "waiting_for_user",
    "waiting_for_external",
    "interrupted",
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS execution_schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_runs (
    run_id TEXT PRIMARY KEY,
    session_id TEXT,
    workflow_name TEXT NOT NULL,
    user_facing_name TEXT NOT NULL,
    user_id TEXT,
    source_platform TEXT NOT NULL,
    source_chat_id TEXT NOT NULL,
    request_text TEXT,
    admission_reason TEXT,
    admission_score REAL DEFAULT 0,
    admission_reasons_json TEXT,
    status TEXT NOT NULL,
    current_step_id TEXT,
    current_blocker TEXT,
    next_action TEXT,
    claimed_by TEXT,
    lease_expires_at REAL,
    run_version INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL,
    last_error TEXT,
    last_error_code TEXT
);

CREATE TABLE IF NOT EXISTS execution_steps (
    step_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES execution_runs(run_id) ON DELETE CASCADE,
    step_kind TEXT NOT NULL,
    step_order INTEGER NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    input_json TEXT,
    output_json TEXT,
    prompt_message_id TEXT,
    blocking_decision_key TEXT,
    expected_answer_type TEXT,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    failure_kind TEXT,
    error_text TEXT
);

CREATE TABLE IF NOT EXISTS execution_decisions (
    decision_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES execution_runs(run_id) ON DELETE CASCADE,
    decision_key TEXT NOT NULL,
    question_text TEXT NOT NULL,
    answer_type TEXT NOT NULL,
    choices_json TEXT,
    answer_text TEXT,
    source_message_id TEXT,
    decision_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    supersedes_decision_id TEXT,
    resolved_at REAL,
    invalidated_at REAL,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS execution_step_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    step_id TEXT NOT NULL REFERENCES execution_steps(step_id) ON DELETE CASCADE,
    decision_id TEXT NOT NULL REFERENCES execution_decisions(decision_id) ON DELETE CASCADE,
    dependency_kind TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_updates (
    update_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES execution_runs(run_id) ON DELETE CASCADE,
    source_message_id TEXT,
    raw_text TEXT,
    classification TEXT NOT NULL,
    status TEXT NOT NULL,
    received_at REAL NOT NULL,
    consumed_at REAL,
    supersedes_update_id TEXT
);

CREATE TABLE IF NOT EXISTS execution_subtasks (
    subtask_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES execution_runs(run_id) ON DELETE CASCADE,
    parent_step_id TEXT REFERENCES execution_steps(step_id) ON DELETE SET NULL,
    child_session_id TEXT,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT,
    result_json TEXT,
    effect_proposals_json TEXT,
    retryable INTEGER NOT NULL DEFAULT 0,
    failure_kind TEXT,
    error TEXT,
    started_at REAL NOT NULL,
    finished_at REAL
);

CREATE TABLE IF NOT EXISTS execution_effects (
    effect_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES execution_runs(run_id) ON DELETE CASCADE,
    step_id TEXT REFERENCES execution_steps(step_id) ON DELETE SET NULL,
    effect_type TEXT NOT NULL,
    target TEXT NOT NULL,
    logical_effect_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    result_hash TEXT,
    provider_receipt_id TEXT,
    status TEXT NOT NULL,
    applied_at REAL,
    confirmed_at REAL,
    error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_runs_status_updated
    ON execution_runs(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_runs_session_created
    ON execution_runs(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_runs_source_active
    ON execution_runs(source_platform, source_chat_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_steps_run_order
    ON execution_steps(run_id, step_order);
CREATE INDEX IF NOT EXISTS idx_execution_decisions_run_key
    ON execution_decisions(run_id, decision_key, decision_version DESC);
CREATE INDEX IF NOT EXISTS idx_execution_updates_run_status
    ON execution_updates(run_id, status, received_at);
CREATE INDEX IF NOT EXISTS idx_execution_subtasks_run_status
    ON execution_subtasks(run_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_effects_logical
    ON execution_effects(logical_effect_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_effects_run_idempotency
    ON execution_effects(run_id, idempotency_key);
"""


def execution_db_path() -> Path:
    return get_hermes_home() / "execution_state.db"


def _now() -> float:
    return time.time()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Any) -> Any:
    if value in (None, "", b""):
        return None
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def normalize_workflow_name(message: str) -> str:
    text = (message or "").lower()
    aa_nathan_terms = (
        "paul graham",
        "gbrain",
        "google workspace",
        "telegram",
        "failure-only alert",
        "skill generation",
    )
    if sum(term in text for term in aa_nathan_terms) >= 3:
        return "AA/Nathan Founder Setup"
    return "Durable Run"


def score_admission(message: str) -> tuple[float, list[str]]:
    text = (message or "").strip().lower()
    if not text:
        return 0.0, []

    score = 0.0
    reasons: list[str] = []

    keyword_groups = [
        ("setup", 0.35),
        ("configure", 0.25),
        ("operationalize", 0.35),
        ("migrate", 0.25),
        ("cron", 0.25),
        ("telegram", 0.25),
        ("gmail", 0.25),
        ("calendar", 0.25),
        ("gbrain", 0.35),
        ("google workspace", 0.35),
        ("delegate", 0.2),
        ("watchlist", 0.2),
        ("recurring", 0.2),
        ("summary", 0.1),
    ]
    for keyword, value in keyword_groups:
        if keyword in text:
            score += value
            reasons.append(keyword)

    if text.count("\n-") >= 2 or text.count("\n1.") >= 2:
        score += 0.35
        reasons.append("multi-part")
    if len(text) > 500:
        score += 0.2
        reasons.append("high-context")
    if text.count("(") + text.count(")") >= 4:
        score += 0.1
        reasons.append("nested-decisions")

    return min(score, 1.0), reasons


@dataclass
class AdmissionDecision:
    admitted: bool
    mode: str
    score: float
    reasons: list[str]
    reason_text: str


def decide_admission(message: str, config: Optional[dict] = None) -> AdmissionDecision:
    execution_cfg = ((config or {}).get("execution_spine") or {})
    enabled = bool(execution_cfg.get("enabled", False))
    mode = str(execution_cfg.get("admission_mode", "auto") or "auto").lower()
    if not enabled or mode == "off":
        return AdmissionDecision(False, mode, 0.0, [], "disabled")

    score, reasons = score_admission(message)
    if mode == "force":
        return AdmissionDecision(True, mode, max(score, 1.0), reasons or ["forced"], "forced")
    admitted = score >= 0.6
    return AdmissionDecision(admitted, mode, score, reasons, ", ".join(reasons) or "score below threshold")


class DurableRunDB:
    _WRITE_MAX_RETRIES = 15
    _WRITE_RETRY_MIN_S = 0.020
    _WRITE_RETRY_MAX_S = 0.150

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path or execution_db_path())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=1.0,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(SCHEMA_SQL)
        cur.execute("SELECT version FROM execution_schema_version LIMIT 1")
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO execution_schema_version(version) VALUES (?)", (SCHEMA_VERSION,))
        elif int(row["version"]) != SCHEMA_VERSION:
            cur.execute("UPDATE execution_schema_version SET version = ?", (SCHEMA_VERSION,))
        self._conn.commit()

    def _execute_write(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        last_err: Optional[Exception] = None
        for attempt in range(self._WRITE_MAX_RETRIES):
            try:
                with self._lock:
                    self._conn.execute("BEGIN IMMEDIATE")
                    try:
                        result = fn(self._conn)
                        self._conn.commit()
                    except BaseException:
                        self._conn.rollback()
                        raise
                return result
            except sqlite3.OperationalError as exc:
                text = str(exc).lower()
                if ("locked" in text or "busy" in text) and attempt < self._WRITE_MAX_RETRIES - 1:
                    last_err = exc
                    time.sleep(random.uniform(self._WRITE_RETRY_MIN_S, self._WRITE_RETRY_MAX_S))
                    continue
                raise
        raise last_err or sqlite3.OperationalError("execution_state.db is locked")

    def _row_to_dict(self, row: Optional[sqlite3.Row]) -> Optional[dict]:
        if row is None:
            return None
        return {k: row[k] for k in row.keys()}

    def create_run(
        self,
        *,
        session_id: Optional[str],
        workflow_name: str,
        source_platform: str,
        source_chat_id: str,
        user_id: Optional[str],
        request_text: str,
        admission: AdmissionDecision,
        claimant: Optional[str],
        metadata: Optional[dict] = None,
        user_facing_name: str = "Durable Runs",
        lease_seconds: int = 90,
    ) -> dict:
        run_id = uuid.uuid4().hex
        now = _now()
        lease_expires_at = now + lease_seconds if claimant else None

        def _write(conn: sqlite3.Connection) -> dict:
            conn.execute(
                """
                INSERT INTO execution_runs(
                    run_id, session_id, workflow_name, user_facing_name, user_id,
                    source_platform, source_chat_id, request_text, admission_reason,
                    admission_score, admission_reasons_json, status, current_step_id,
                    current_blocker, next_action, claimed_by, lease_expires_at,
                    run_version, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    session_id,
                    workflow_name,
                    user_facing_name,
                    user_id,
                    source_platform,
                    source_chat_id,
                    request_text,
                    admission.reason_text,
                    admission.score,
                    _json_dumps(admission.reasons),
                    "running",
                    None,
                    None,
                    "Running",
                    claimant,
                    lease_expires_at,
                    0,
                    _json_dumps(metadata or {}),
                    now,
                    now,
                ),
            )
            return self.get_run(run_id)

        return self._execute_write(_write)

    def get_run(self, run_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM execution_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return self._row_to_dict(row)

    def list_runs(self, *, limit: int = 20, status: Optional[str] = None) -> List[dict]:
        sql = "SELECT * FROM execution_runs"
        params: list[Any] = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_latest_active_run(
        self,
        *,
        session_id: Optional[str] = None,
        source_platform: Optional[str] = None,
        source_chat_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Optional[dict]:
        clauses = [f"status IN ({', '.join('?' for _ in ACTIVE_RUN_STATUSES)})"]
        params: list[Any] = list(ACTIVE_RUN_STATUSES)
        if session_id:
            clauses.append("session_id = ?")
            params.append(session_id)
        if source_platform:
            clauses.append("source_platform = ?")
            params.append(source_platform)
        if source_chat_id:
            clauses.append("source_chat_id = ?")
            params.append(source_chat_id)
        if user_id:
            clauses.append("user_id = ?")
            params.append(user_id)
        row = self._conn.execute(
            f"SELECT * FROM execution_runs WHERE {' AND '.join(clauses)} ORDER BY updated_at DESC LIMIT 1",
            params,
        ).fetchone()
        return self._row_to_dict(row)

    def claim_run(self, run_id: str, claimant: str, *, lease_seconds: int = 90) -> bool:
        now = _now()

        def _write(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                "SELECT claimed_by, lease_expires_at, run_version FROM execution_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return False
            claimed_by = row["claimed_by"]
            lease_expires_at = row["lease_expires_at"] or 0
            if claimed_by and claimed_by != claimant and lease_expires_at > now:
                return False
            updated = conn.execute(
                """
                UPDATE execution_runs
                   SET claimed_by = ?, lease_expires_at = ?, run_version = run_version + 1, updated_at = ?
                 WHERE run_id = ?
                """,
                (claimant, now + lease_seconds, now, run_id),
            )
            return updated.rowcount == 1

        return bool(self._execute_write(_write))

    def heartbeat_run(self, run_id: str, claimant: str, *, lease_seconds: int = 90) -> bool:
        now = _now()

        def _write(conn: sqlite3.Connection) -> bool:
            updated = conn.execute(
                """
                UPDATE execution_runs
                   SET lease_expires_at = ?, updated_at = ?
                 WHERE run_id = ? AND claimed_by = ?
                """,
                (now + lease_seconds, now, run_id, claimant),
            )
            return updated.rowcount == 1

        return bool(self._execute_write(_write))

    def release_run(self, run_id: str, claimant: str) -> None:
        now = _now()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE execution_runs
                   SET claimed_by = NULL, lease_expires_at = NULL, updated_at = ?
                 WHERE run_id = ? AND claimed_by = ?
                """,
                (now, run_id, claimant),
            )

        self._execute_write(_write)

    def update_run(
        self,
        run_id: str,
        *,
        status: Optional[str] = None,
        current_step_id: Optional[str] = None,
        current_blocker: Optional[str] = None,
        next_action: Optional[str] = None,
        last_error: Optional[str] = None,
        last_error_code: Optional[str] = None,
        metadata: Optional[dict] = None,
        completed: bool = False,
    ) -> None:
        now = _now()
        fields: list[str] = ["updated_at = ?"]
        params: list[Any] = [now]
        if status is not None:
            fields.append("status = ?")
            params.append(status)
        if current_step_id is not None:
            fields.append("current_step_id = ?")
            params.append(current_step_id)
        if current_blocker is not None:
            fields.append("current_blocker = ?")
            params.append(current_blocker)
        if next_action is not None:
            fields.append("next_action = ?")
            params.append(next_action)
        if last_error is not None:
            fields.append("last_error = ?")
            params.append(last_error)
        if last_error_code is not None:
            fields.append("last_error_code = ?")
            params.append(last_error_code)
        if metadata is not None:
            fields.append("metadata_json = ?")
            params.append(_json_dumps(metadata))
        if completed:
            fields.append("completed_at = ?")
            params.append(now)
        params.append(run_id)

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(f"UPDATE execution_runs SET {', '.join(fields)} WHERE run_id = ?", params)

        self._execute_write(_write)

    def add_step(
        self,
        run_id: str,
        *,
        step_kind: str,
        step_order: int,
        title: str,
        status: str = "running",
        input_json: Optional[dict] = None,
        prompt_message_id: Optional[str] = None,
        blocking_decision_key: Optional[str] = None,
        expected_answer_type: Optional[str] = None,
    ) -> dict:
        step_id = uuid.uuid4().hex
        now = _now()

        def _write(conn: sqlite3.Connection) -> dict:
            conn.execute(
                """
                INSERT INTO execution_steps(
                    step_id, run_id, step_kind, step_order, title, status,
                    input_json, prompt_message_id, blocking_decision_key,
                    expected_answer_type, created_at, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    step_id,
                    run_id,
                    step_kind,
                    step_order,
                    title,
                    status,
                    _json_dumps(input_json or {}),
                    prompt_message_id,
                    blocking_decision_key,
                    expected_answer_type,
                    now,
                    now if status == "running" else None,
                ),
            )
            conn.execute(
                """
                UPDATE execution_runs
                   SET current_step_id = ?, updated_at = ?, current_blocker = NULL
                 WHERE run_id = ?
                """,
                (step_id, now, run_id),
            )
            row = conn.execute("SELECT * FROM execution_steps WHERE step_id = ?", (step_id,)).fetchone()
            return self._row_to_dict(row)

        return self._execute_write(_write)

    def update_step(
        self,
        step_id: str,
        *,
        status: Optional[str] = None,
        output_json: Optional[dict] = None,
        blocking_decision_key: Optional[str] = None,
        expected_answer_type: Optional[str] = None,
        failure_kind: Optional[str] = None,
        error_text: Optional[str] = None,
    ) -> None:
        now = _now()
        row = self._conn.execute("SELECT run_id FROM execution_steps WHERE step_id = ?", (step_id,)).fetchone()
        if row is None:
            return
        run_id = row["run_id"]
        fields = ["finished_at = CASE WHEN ? IN ('completed', 'failed', 'paused', 'skipped') THEN ? ELSE finished_at END"]
        params: list[Any] = [status or "", now]
        if status is not None:
            fields.append("status = ?")
            params.append(status)
        if output_json is not None:
            fields.append("output_json = ?")
            params.append(_json_dumps(output_json))
        if blocking_decision_key is not None:
            fields.append("blocking_decision_key = ?")
            params.append(blocking_decision_key)
        if expected_answer_type is not None:
            fields.append("expected_answer_type = ?")
            params.append(expected_answer_type)
        if failure_kind is not None:
            fields.append("failure_kind = ?")
            params.append(failure_kind)
        if error_text is not None:
            fields.append("error_text = ?")
            params.append(error_text)
        params.append(step_id)

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(f"UPDATE execution_steps SET {', '.join(fields)} WHERE step_id = ?", params)
            conn.execute("UPDATE execution_runs SET updated_at = ? WHERE run_id = ?", (now, run_id))

        self._execute_write(_write)

    def list_steps(self, run_id: str) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM execution_steps WHERE run_id = ? ORDER BY step_order ASC, created_at ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def upsert_decision(
        self,
        run_id: str,
        *,
        decision_key: str,
        question_text: str,
        answer_type: str,
        choices: Optional[Iterable[str]] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        existing = self._conn.execute(
            """
            SELECT * FROM execution_decisions
             WHERE run_id = ? AND decision_key = ?
             ORDER BY decision_version DESC
             LIMIT 1
            """,
            (run_id, decision_key),
        ).fetchone()
        now = _now()

        def _write(conn: sqlite3.Connection) -> dict:
            if existing and existing["status"] == "resolved":
                return self._row_to_dict(existing)
            if existing and existing["status"] == "invalidated":
                decision_id = uuid.uuid4().hex
                decision_version = int(existing["decision_version"] or 1) + 1
                conn.execute(
                    """
                    INSERT INTO execution_decisions(
                        decision_id, run_id, decision_key, question_text, answer_type,
                        choices_json, status, supersedes_decision_id, decision_version,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        decision_id,
                        run_id,
                        decision_key,
                        question_text,
                        answer_type,
                        _json_dumps(list(choices or [])),
                        existing["decision_id"],
                        decision_version,
                        _json_dumps(metadata or {}),
                    ),
                )
                row = conn.execute("SELECT * FROM execution_decisions WHERE decision_id = ?", (decision_id,)).fetchone()
                conn.execute(
                    "UPDATE execution_runs SET status = 'waiting_for_user', current_blocker = ?, next_action = ?, updated_at = ? WHERE run_id = ?",
                    (question_text, "Reply to the tracked question to continue.", now, run_id),
                )
                return self._row_to_dict(row)
            if existing:
                decision_id = existing["decision_id"]
                conn.execute(
                    """
                    UPDATE execution_decisions
                       SET question_text = ?, answer_type = ?, choices_json = ?, metadata_json = ?
                     WHERE decision_id = ?
                    """,
                    (
                        question_text,
                        answer_type,
                        _json_dumps(list(choices or [])),
                        _json_dumps(metadata or {}),
                        decision_id,
                    ),
                )
                row = conn.execute("SELECT * FROM execution_decisions WHERE decision_id = ?", (decision_id,)).fetchone()
                return self._row_to_dict(row)

            decision_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO execution_decisions(
                    decision_id, run_id, decision_key, question_text, answer_type,
                    choices_json, status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    decision_id,
                    run_id,
                    decision_key,
                    question_text,
                    answer_type,
                    _json_dumps(list(choices or [])),
                    _json_dumps(metadata or {}),
                ),
            )
            row = conn.execute("SELECT * FROM execution_decisions WHERE decision_id = ?", (decision_id,)).fetchone()
            conn.execute(
                "UPDATE execution_runs SET status = 'waiting_for_user', current_blocker = ?, next_action = ?, updated_at = ? WHERE run_id = ?",
                (question_text, "Reply to the tracked question to continue.", now, run_id),
            )
            return self._row_to_dict(row)

        return self._execute_write(_write)

    def link_step_decision(self, step_id: str, decision_id: str, dependency_kind: str = "blocks") -> None:
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO execution_step_decisions(step_id, decision_id, dependency_kind)
                VALUES (?, ?, ?)
                """,
                (step_id, decision_id, dependency_kind),
            )

        self._execute_write(_write)

    def resolve_decision(
        self,
        run_id: str,
        *,
        decision_key: str,
        answer_text: str,
        source_message_id: Optional[str] = None,
    ) -> None:
        now = _now()

        def _write(conn: sqlite3.Connection) -> None:
            row = conn.execute(
                """
                SELECT decision_id, decision_version
                  FROM execution_decisions
                 WHERE run_id = ? AND decision_key = ?
                 ORDER BY decision_version DESC
                 LIMIT 1
                """,
                (run_id, decision_key),
            ).fetchone()
            if row is None:
                return
            conn.execute(
                """
                UPDATE execution_decisions
                   SET answer_text = ?, source_message_id = ?, status = 'resolved', resolved_at = ?
                 WHERE decision_id = ?
                """,
                (answer_text, source_message_id, now, row["decision_id"]),
            )
            conn.execute(
                """
                UPDATE execution_runs
                   SET status = 'running', current_blocker = NULL, next_action = 'Continue execution', updated_at = ?
                 WHERE run_id = ?
                """,
                (now, run_id),
            )

        self._execute_write(_write)

    def invalidate_decision(
        self,
        run_id: str,
        *,
        decision_key: str,
        reason: str,
    ) -> None:
        now = _now()
        existing = self._conn.execute(
            """
            SELECT * FROM execution_decisions
             WHERE run_id = ? AND decision_key = ?
             ORDER BY decision_version DESC
             LIMIT 1
            """,
            (run_id, decision_key),
        ).fetchone()
        if existing is None:
            return

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE execution_decisions
                   SET status = 'invalidated', invalidated_at = ?, metadata_json = ?
                 WHERE decision_id = ?
                """,
                (
                    now,
                    _json_dumps({"reason": reason}),
                    existing["decision_id"],
                ),
            )

        self._execute_write(_write)

    def list_decisions(self, run_id: str) -> List[dict]:
        rows = self._conn.execute(
            """
            SELECT * FROM execution_decisions
             WHERE run_id = ?
             ORDER BY decision_key ASC, decision_version DESC
            """,
            (run_id,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_pending_decision(self, run_id: str) -> Optional[dict]:
        row = self._conn.execute(
            """
            SELECT * FROM execution_decisions
             WHERE run_id = ? AND status = 'pending'
             ORDER BY decision_version DESC, decision_id DESC
             LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        return self._row_to_dict(row)

    def queue_update(
        self,
        run_id: str,
        *,
        raw_text: str,
        classification: str,
        source_message_id: Optional[str] = None,
    ) -> dict:
        update_id = uuid.uuid4().hex
        now = _now()

        def _write(conn: sqlite3.Connection) -> dict:
            conn.execute(
                """
                INSERT INTO execution_updates(
                    update_id, run_id, source_message_id, raw_text, classification,
                    status, received_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?)
                """,
                (update_id, run_id, source_message_id, raw_text, classification, now),
            )
            conn.execute(
                """
                UPDATE execution_runs
                   SET next_action = ?, updated_at = ?
                 WHERE run_id = ?
                """,
                ("Queued update pending review", now, run_id),
            )
            row = conn.execute("SELECT * FROM execution_updates WHERE update_id = ?", (update_id,)).fetchone()
            return self._row_to_dict(row)

        return self._execute_write(_write)

    def list_updates(self, run_id: str, *, status: Optional[str] = None) -> List[dict]:
        sql = "SELECT * FROM execution_updates WHERE run_id = ?"
        params: list[Any] = [run_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY received_at ASC"
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def add_subtask(
        self,
        run_id: str,
        *,
        parent_step_id: Optional[str],
        child_session_id: Optional[str],
        goal: str,
    ) -> dict:
        subtask_id = uuid.uuid4().hex
        now = _now()

        def _write(conn: sqlite3.Connection) -> dict:
            conn.execute(
                """
                INSERT INTO execution_subtasks(
                    subtask_id, run_id, parent_step_id, child_session_id, goal,
                    status, started_at
                ) VALUES (?, ?, ?, ?, ?, 'running', ?)
                """,
                (subtask_id, run_id, parent_step_id, child_session_id, goal, now),
            )
            row = conn.execute("SELECT * FROM execution_subtasks WHERE subtask_id = ?", (subtask_id,)).fetchone()
            return self._row_to_dict(row)

        return self._execute_write(_write)

    def finish_subtask(
        self,
        subtask_id: str,
        *,
        status: str,
        summary: Optional[str] = None,
        result_json: Optional[dict] = None,
        effect_proposals_json: Optional[dict] = None,
        retryable: bool = False,
        failure_kind: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        now = _now()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE execution_subtasks
                   SET status = ?, summary = ?, result_json = ?, effect_proposals_json = ?,
                       retryable = ?, failure_kind = ?, error = ?, finished_at = ?
                 WHERE subtask_id = ?
                """,
                (
                    status,
                    summary,
                    _json_dumps(result_json or {}) if result_json is not None else None,
                    _json_dumps(effect_proposals_json or {}) if effect_proposals_json is not None else None,
                    1 if retryable else 0,
                    failure_kind,
                    error,
                    now,
                    subtask_id,
                ),
            )

        self._execute_write(_write)

    def list_subtasks(self, run_id: str) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM execution_subtasks WHERE run_id = ? ORDER BY started_at ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def plan_effect(
        self,
        run_id: str,
        *,
        step_id: Optional[str],
        effect_type: str,
        target: str,
        logical_effect_key: str,
        idempotency_key: str,
        request_payload: dict,
    ) -> dict:
        effect_id = uuid.uuid4().hex
        request_hash = _hash_payload(request_payload)
        row = self._conn.execute(
            "SELECT * FROM execution_effects WHERE logical_effect_key = ?",
            (logical_effect_key,),
        ).fetchone()
        if row is not None:
            return self._row_to_dict(row)

        def _write(conn: sqlite3.Connection) -> dict:
            conn.execute(
                """
                INSERT INTO execution_effects(
                    effect_id, run_id, step_id, effect_type, target,
                    logical_effect_key, idempotency_key, request_hash, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'planned')
                """,
                (
                    effect_id,
                    run_id,
                    step_id,
                    effect_type,
                    target,
                    logical_effect_key,
                    idempotency_key,
                    request_hash,
                ),
            )
            row2 = conn.execute("SELECT * FROM execution_effects WHERE effect_id = ?", (effect_id,)).fetchone()
            return self._row_to_dict(row2)

        return self._execute_write(_write)

    def finish_effect(
        self,
        logical_effect_key: str,
        *,
        status: str,
        result_payload: Optional[dict] = None,
        provider_receipt_id: Optional[str] = None,
        error_text: Optional[str] = None,
    ) -> None:
        now = _now()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE execution_effects
                   SET status = ?,
                       result_hash = ?,
                       provider_receipt_id = COALESCE(?, provider_receipt_id),
                       error_text = ?,
                       applied_at = CASE WHEN ? IN ('applied', 'confirmed', 'unknown_needs_operator_review') THEN COALESCE(applied_at, ?) ELSE applied_at END,
                       confirmed_at = CASE WHEN ? = 'confirmed' THEN ? ELSE confirmed_at END
                 WHERE logical_effect_key = ?
                """,
                (
                    status,
                    _hash_payload(result_payload or {}) if result_payload is not None else None,
                    provider_receipt_id,
                    error_text,
                    status,
                    now,
                    status,
                    now,
                    logical_effect_key,
                ),
            )

        self._execute_write(_write)

    def list_effects(self, run_id: str) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM execution_effects WHERE run_id = ? ORDER BY effect_id ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def inspect_run(self, run_id: str) -> Optional[dict]:
        run = self.get_run(run_id)
        if not run:
            return None
        return {
            "run": run,
            "steps": self.list_steps(run_id),
            "decisions": self.list_decisions(run_id),
            "updates": self.list_updates(run_id),
            "subtasks": self.list_subtasks(run_id),
            "effects": self.list_effects(run_id),
        }

    def doctor(self) -> dict:
        now = _now()
        run_count = self._conn.execute("SELECT COUNT(*) FROM execution_runs").fetchone()[0]
        active_count = self._conn.execute(
            f"SELECT COUNT(*) FROM execution_runs WHERE status IN ({', '.join('?' for _ in ACTIVE_RUN_STATUSES)})",
            ACTIVE_RUN_STATUSES,
        ).fetchone()[0]
        stuck_waiting = self._conn.execute(
            """
            SELECT COUNT(*) FROM execution_runs
             WHERE status IN ('waiting_for_user', 'waiting_for_external')
               AND updated_at < ?
            """,
            (now - 3600,),
        ).fetchone()[0]
        leased_stale = self._conn.execute(
            """
            SELECT COUNT(*) FROM execution_runs
             WHERE claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """,
            (now,),
        ).fetchone()[0]
        version = self._conn.execute("SELECT version FROM execution_schema_version LIMIT 1").fetchone()[0]
        return {
            "db_path": str(self.db_path),
            "schema_version": int(version),
            "run_count": int(run_count),
            "active_run_count": int(active_count),
            "stuck_waiting_count": int(stuck_waiting),
            "stale_lease_count": int(leased_stale),
        }

    def create_demo_run(self) -> dict:
        admission = AdmissionDecision(True, "force", 1.0, ["demo"], "demo")
        run = self.create_run(
            session_id="demo",
            workflow_name="Durable Run Demo",
            source_platform="cli",
            source_chat_id="cli",
            user_id=None,
            request_text="Local Durable Runs hello world",
            admission=admission,
            claimant=f"demo:{os.getpid()}",
            metadata={"demo": True},
        )
        step = self.add_step(
            run["run_id"],
            step_kind="clarify",
            step_order=1,
            title="Ask one tracked yes/no decision",
            status="paused",
            expected_answer_type="yes_no",
            blocking_decision_key="demo.approval",
        )
        decision = self.upsert_decision(
            run["run_id"],
            decision_key="demo.approval",
            question_text="Should Hermes continue the demo run?",
            answer_type="yes_no",
            choices=["yes", "no"],
            metadata={"demo": True},
        )
        self.link_step_decision(step["step_id"], decision["decision_id"])
        self.update_run(
            run["run_id"],
            status="waiting_for_user",
            current_step_id=step["step_id"],
            current_blocker="Should Hermes continue the demo run?",
            next_action="Run `hermes runs resume <run_id> --answer yes`.",
        )
        return self.get_run(run["run_id"]) or run


def backup_execution_db(db_path: Optional[Path] = None) -> Optional[Path]:
    source = Path(db_path or execution_db_path())
    if not source.exists():
        return None
    ts = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    backup = source.with_name(f"{source.name}.{ts}.bak")
    shutil.copy2(source, backup)
    wal = source.with_suffix(source.suffix + "-wal")
    shm = source.with_suffix(source.suffix + "-shm")
    if wal.exists():
        shutil.copy2(wal, backup.with_name(f"{backup.name}-wal"))
    if shm.exists():
        shutil.copy2(shm, backup.with_name(f"{backup.name}-shm"))
    return backup


def list_execution_backups(db_path: Optional[Path] = None) -> List[Path]:
    source = Path(db_path or execution_db_path())
    pattern = f"{source.name}.*.bak"
    return sorted(source.parent.glob(pattern))


def rollback_execution_db(backup_path: Optional[Path] = None, db_path: Optional[Path] = None) -> Optional[Path]:
    target = Path(db_path or execution_db_path())
    backup = Path(backup_path) if backup_path else (list_execution_backups(target)[-1] if list_execution_backups(target) else None)
    if backup is None or not backup.exists():
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(backup, target)
    wal_backup = backup.with_name(f"{backup.name}-wal")
    shm_backup = backup.with_name(f"{backup.name}-shm")
    target_wal = target.with_suffix(target.suffix + "-wal")
    target_shm = target.with_suffix(target.suffix + "-shm")
    if wal_backup.exists():
        shutil.copy2(wal_backup, target_wal)
    elif target_wal.exists():
        target_wal.unlink()
    if shm_backup.exists():
        shutil.copy2(shm_backup, target_shm)
    elif target_shm.exists():
        target_shm.unlink()
    return backup


class DurableRunContext:
    """
    Lightweight in-memory helper bound to one live AIAgent execution.
    """

    def __init__(self, db: DurableRunDB, run: dict, *, claimant: str):
        self.db = db
        self.run_id = run["run_id"]
        self.claimant = claimant
        self.workflow_name = run["workflow_name"]
        self._step_order = len(self.db.list_steps(self.run_id))
        self._current_step_id: Optional[str] = run.get("current_step_id")
        self._lock = threading.Lock()

    def heartbeat(self) -> None:
        self.db.heartbeat_run(self.run_id, self.claimant)

    def start_step(
        self,
        step_kind: str,
        title: str,
        *,
        status: str = "running",
        input_json: Optional[dict] = None,
        blocking_decision_key: Optional[str] = None,
        expected_answer_type: Optional[str] = None,
    ) -> dict:
        with self._lock:
            self._step_order += 1
            step = self.db.add_step(
                self.run_id,
                step_kind=step_kind,
                step_order=self._step_order,
                title=title,
                status=status,
                input_json=input_json,
                blocking_decision_key=blocking_decision_key,
                expected_answer_type=expected_answer_type,
            )
            self._current_step_id = step["step_id"]
            self.heartbeat()
            return step

    def complete_step(self, step_id: Optional[str], *, output_json: Optional[dict] = None) -> None:
        if not step_id:
            return
        self.db.update_step(step_id, status="completed", output_json=output_json)
        self.db.update_run(self.run_id, status="running", current_blocker="", next_action="Continue execution")
        self.heartbeat()

    def fail_step(self, step_id: Optional[str], *, error_text: str, failure_kind: str = "error") -> None:
        if step_id:
            self.db.update_step(step_id, status="failed", failure_kind=failure_kind, error_text=error_text)
        self.db.update_run(
            self.run_id,
            status="failed",
            last_error=error_text,
            last_error_code=failure_kind,
            current_blocker=error_text,
            next_action="Inspect the run and retry once the issue is fixed.",
            completed=True,
        )

    def ask_decision(
        self,
        *,
        decision_key: str,
        question: str,
        choices: Optional[list[str]],
        answer_type: str,
        callback: Optional[Callable[..., str]],
        metadata: Optional[dict] = None,
        source_message_id: Optional[str] = None,
    ) -> dict:
        step = self.start_step(
            "clarify",
            title=question,
            status="paused",
            input_json={"choices": choices or []},
            blocking_decision_key=decision_key,
            expected_answer_type=answer_type,
        )
        decision = self.db.upsert_decision(
            self.run_id,
            decision_key=decision_key,
            question_text=question,
            answer_type=answer_type,
            choices=choices,
            metadata=metadata,
        )
        self.db.link_step_decision(step["step_id"], decision["decision_id"])

        if callback is None:
            self.db.update_run(
                self.run_id,
                status="waiting_for_user",
                current_step_id=step["step_id"],
                current_blocker=question,
                next_action="Reply to the tracked question to continue.",
            )
            return {"status": "waiting_for_user", "decision_key": decision_key}

        try:
            try:
                answer = callback(question, choices, metadata or {})
            except TypeError:
                answer = callback(question, choices)
        except Exception as exc:
            self.fail_step(step["step_id"], error_text=str(exc), failure_kind="ask_decision_failed")
            raise

        answer_text = str(answer or "").strip()
        if answer_text:
            self.db.resolve_decision(
                self.run_id,
                decision_key=decision_key,
                answer_text=answer_text,
                source_message_id=source_message_id,
            )
            self.complete_step(step["step_id"], output_json={"answer": answer_text})
            return {
                "status": "answered",
                "decision_key": decision_key,
                "question": question,
                "choices_offered": choices,
                "answer_type": answer_type,
                "user_response": answer_text,
            }

        self.db.update_run(
            self.run_id,
            status="waiting_for_user",
            current_step_id=step["step_id"],
            current_blocker=question,
            next_action="Reply to the tracked question to continue.",
        )
        return {"status": "waiting_for_user", "decision_key": decision_key}

    def queue_update(self, text: str, *, classification: str, source_message_id: Optional[str] = None) -> dict:
        return self.db.queue_update(self.run_id, raw_text=text, classification=classification, source_message_id=source_message_id)

    def record_delegate_result(self, function_args: dict, function_result: Any) -> None:
        try:
            payload = function_result if isinstance(function_result, dict) else json.loads(function_result or "{}")
        except Exception:
            payload = {"raw": str(function_result)}
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            return
        for idx, item in enumerate(results):
            goal = ""
            tasks = function_args.get("tasks")
            if isinstance(tasks, list) and idx < len(tasks):
                goal = tasks[idx].get("goal") or ""
            goal = goal or function_args.get("goal") or f"delegated-task-{idx + 1}"
            subtask = self.db.add_subtask(
                self.run_id,
                parent_step_id=self._current_step_id,
                child_session_id=item.get("child_session_id"),
                goal=goal,
            )
            self.db.finish_subtask(
                subtask["subtask_id"],
                status=item.get("status") or "completed",
                summary=item.get("summary"),
                result_json=item,
                effect_proposals_json=item.get("effect_proposals_json"),
                retryable=bool(item.get("retryable", False)),
                failure_kind=item.get("failure_kind"),
                error=item.get("error"),
            )

    def plan_effect(self, effect_type: str, args: dict) -> dict:
        target = str(args.get("target") or args.get("chat_id") or args.get("job_id") or effect_type)
        logical_effect_key = hashlib.sha256(
            f"{effect_type}:{target}:{_json_dumps(args)}".encode("utf-8")
        ).hexdigest()
        idempotency_key = hashlib.sha256(
            f"{self.run_id}:{effect_type}:{target}:{_json_dumps(args)}".encode("utf-8")
        ).hexdigest()
        effect = self.db.plan_effect(
            self.run_id,
            step_id=self._current_step_id,
            effect_type=effect_type,
            target=target,
            logical_effect_key=logical_effect_key,
            idempotency_key=idempotency_key,
            request_payload=args,
        )
        self.heartbeat()
        return effect

    def finish_effect(self, effect: Optional[dict], *, result_payload: Optional[dict], error: Optional[str] = None) -> None:
        if not effect:
            return
        status = "failed" if error else "confirmed"
        self.db.finish_effect(
            effect["logical_effect_key"],
            status=status,
            result_payload=result_payload,
            provider_receipt_id=(result_payload or {}).get("message_id") if isinstance(result_payload, dict) else None,
            error_text=error,
        )

    def finalize(self, *, status: str, final_response: Optional[str] = None, error: Optional[str] = None) -> None:
        self.db.update_run(
            self.run_id,
            status=status,
            current_blocker="" if status == "completed" else None,
            next_action="Done" if status == "completed" else "Inspect the run.",
            last_error=error,
            completed=status in {"completed", "failed", "cancelled"},
            metadata={"final_response_preview": (final_response or "")[:500]},
        )
        self.db.release_run(self.run_id, self.claimant)


def format_run_markdown(run: dict, *, decisions: Optional[list[dict]] = None, updates: Optional[list[dict]] = None, effects: Optional[list[dict]] = None) -> str:
    decisions = decisions or []
    updates = updates or []
    effects = effects or []
    lines = [
        "🧵 **Durable Run**",
        "",
        f"**Run ID:** `{run['run_id']}`",
        f"**Workflow:** {run.get('workflow_name') or 'Durable Run'}",
        f"**Status:** `{run.get('status')}`",
    ]
    if run.get("current_blocker"):
        lines.append(f"**Blocker:** {run['current_blocker']}")
    if run.get("next_action"):
        lines.append(f"**Next Action:** {run['next_action']}")
    if run.get("current_step_id"):
        lines.append(f"**Current Step:** `{run['current_step_id']}`")
    if decisions:
        resolved = [d for d in decisions if d.get("status") == "resolved"]
        if resolved:
            lines.append("")
            lines.append("**Resolved Decisions:**")
            for decision in resolved[:5]:
                lines.append(f"- `{decision['decision_key']}` -> {decision.get('answer_text') or 'resolved'}")
    if updates:
        queued = [u for u in updates if u.get("status") == "queued"]
        if queued:
            lines.append("")
            lines.append(f"**Queued Updates:** {len(queued)}")
    if effects:
        confirmed = [e for e in effects if e.get("status") == "confirmed"]
        if confirmed:
            lines.append("")
            lines.append("**Applied Effects:**")
            for effect in confirmed[:5]:
                lines.append(f"- `{effect['effect_type']}` -> {effect['target']}")
    return "\n".join(lines)
