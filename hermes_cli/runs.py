"""
Durable Runs subcommand for Hermes CLI.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from durable_runs import (
    DurableRunDB,
    backup_execution_db,
    execution_db_path,
    format_run_markdown,
    list_execution_backups,
    rollback_execution_db,
)
from hermes_cli.colors import Colors, color


def _open_db() -> DurableRunDB:
    return DurableRunDB()


def _resolve_run(db: DurableRunDB, run_id: Optional[str], *, latest: bool = False) -> Optional[dict]:
    if run_id:
        return db.get_run(run_id)
    if latest:
        runs = db.list_runs(limit=1)
        return runs[0] if runs else None
    return None


def _print_run_table(runs: list[dict]) -> int:
    if not runs:
        print(color("No Durable Runs found.", Colors.DIM))
        return 0
    print()
    print(color("┌─────────────────────────────────────────────────────────────────────────┐", Colors.CYAN))
    print(color("│                            Durable Runs                                 │", Colors.CYAN))
    print(color("└─────────────────────────────────────────────────────────────────────────┘", Colors.CYAN))
    print()
    for run in runs:
        status = run.get("status", "unknown")
        workflow = run.get("workflow_name") or "Durable Run"
        print(f"  {color(run['run_id'], Colors.YELLOW)}  {status}")
        print(f"    Workflow:   {workflow}")
        print(f"    Session:    {run.get('session_id') or '-'}")
        print(f"    Platform:   {run.get('source_platform')}:{run.get('source_chat_id')}")
        if run.get("current_blocker"):
            print(f"    Blocker:    {run['current_blocker']}")
        if run.get("next_action"):
            print(f"    Next:       {run['next_action']}")
        print()
    return 0


def _show_run(db: DurableRunDB, run_id: Optional[str], *, latest: bool = False) -> int:
    run = _resolve_run(db, run_id, latest=latest)
    if not run:
        print(color("Run not found.", Colors.RED))
        return 1
    payload = db.inspect_run(run["run_id"])
    print(
        format_run_markdown(
            payload["run"],
            decisions=payload["decisions"],
            updates=payload["updates"],
            effects=payload["effects"],
        )
    )
    return 0


def _inspect_run(db: DurableRunDB, run_id: Optional[str], *, latest: bool = False, as_json: bool = False) -> int:
    run = _resolve_run(db, run_id, latest=latest)
    if not run:
        print(color("Run not found.", Colors.RED))
        return 1
    payload = db.inspect_run(run["run_id"])
    if as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    return _show_run(db, run["run_id"])


def _resume_run(db: DurableRunDB, run_id: Optional[str], *, latest: bool = False, answer: Optional[str] = None,
                message: Optional[str] = None, source_message_id: Optional[str] = None) -> int:
    run = _resolve_run(db, run_id, latest=latest)
    if not run:
        print(color("Run not found.", Colors.RED))
        return 1

    if answer:
        pending = db.get_pending_decision(run["run_id"])
        if not pending:
            print(color("No pending decision to answer for this run.", Colors.YELLOW))
            return 1
        db.resolve_decision(
            run["run_id"],
            decision_key=pending["decision_key"],
            answer_text=answer,
            source_message_id=source_message_id,
        )
        print(color(f"Resolved decision {pending['decision_key']} for run {run['run_id']}.", Colors.GREEN))
        return 0

    if message:
        db.queue_update(run["run_id"], raw_text=message, classification="operator_resume", source_message_id=source_message_id)
        db.update_run(run["run_id"], status="running", current_blocker="", next_action="Queued operator update for the next execution pass.")
        print(color(f"Queued update for run {run['run_id']}.", Colors.GREEN))
        return 0

    db.update_run(run["run_id"], status="running", current_blocker="", next_action="Ready to continue.")
    print(color(f"Marked run {run['run_id']} as ready to continue.", Colors.GREEN))
    return 0


def _stop_run(db: DurableRunDB, run_id: Optional[str], *, latest: bool = False) -> int:
    run = _resolve_run(db, run_id, latest=latest)
    if not run:
        print(color("Run not found.", Colors.RED))
        return 1
    db.update_run(
        run["run_id"],
        status="cancelled",
        current_blocker="Stopped by operator.",
        next_action="Run inspection only; execution has been cancelled.",
        completed=True,
    )
    print(color(f"Stopped run {run['run_id']}.", Colors.GREEN))
    return 0


def _doctor(db_path: Optional[Path], *, dry_run: bool = False, rollback: Optional[str] = None) -> int:
    path = Path(db_path or execution_db_path())
    if rollback is not None:
        restored = rollback_execution_db(Path(rollback) if rollback else None, path)
        if restored is None:
            print(color("No backup found to roll back.", Colors.RED))
            return 1
        print(color(f"Rolled back Durable Runs DB from {restored}.", Colors.GREEN))
        return 0

    exists = path.exists()
    if dry_run:
        action = "would verify existing DB" if exists else "would create new execution_state.db"
        print(color(f"Dry run: {action}", Colors.CYAN))
        print(f"  Path: {path}")
        return 0

    backup = backup_execution_db(path) if exists else None
    db = DurableRunDB(path)
    try:
        payload = db.doctor()
    finally:
        db.close()
    print(color("Durable Runs doctor", Colors.CYAN))
    print(f"  DB path:           {payload['db_path']}")
    print(f"  Schema version:    {payload['schema_version']}")
    print(f"  Total runs:        {payload['run_count']}")
    print(f"  Active runs:       {payload['active_run_count']}")
    print(f"  Waiting >1h:       {payload['stuck_waiting_count']}")
    print(f"  Stale leases:      {payload['stale_lease_count']}")
    if backup:
        print(f"  Backup:            {backup}")
    backups = list_execution_backups(path)
    if backups:
        print(f"  Latest backup:     {backups[-1]}")
    return 0


def runs_command(args) -> int:
    subcmd = getattr(args, "runs_command", None) or "list"
    if subcmd == "doctor":
        return _doctor(
            getattr(args, "db_path", None),
            dry_run=getattr(args, "dry_run", False),
            rollback=getattr(args, "rollback", None),
        )

    db = _open_db()
    try:
        if subcmd == "list":
            return _print_run_table(db.list_runs(limit=getattr(args, "limit", 20)))
        if subcmd == "show":
            return _show_run(db, getattr(args, "run_id", None), latest=getattr(args, "latest", False))
        if subcmd == "inspect":
            return _inspect_run(
                db,
                getattr(args, "run_id", None),
                latest=getattr(args, "latest", False),
                as_json=getattr(args, "json", False),
            )
        if subcmd == "resume":
            return _resume_run(
                db,
                getattr(args, "run_id", None),
                latest=getattr(args, "latest", False),
                answer=getattr(args, "answer", None),
                message=getattr(args, "message", None),
                source_message_id=getattr(args, "source_message_id", None),
            )
        if subcmd == "stop":
            return _stop_run(db, getattr(args, "run_id", None), latest=getattr(args, "latest", False))
        if subcmd == "demo":
            run = db.create_demo_run()
            print(color("Created local Durable Runs hello world.", Colors.GREEN))
            print(f"  Run ID: {run['run_id']}")
            print("  Next:   hermes runs resume --latest --answer yes")
            return 0
        print(color(f"Unknown runs subcommand: {subcmd}", Colors.RED))
        return 1
    finally:
        db.close()
