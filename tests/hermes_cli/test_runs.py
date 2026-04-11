from argparse import Namespace

from durable_runs import DurableRunDB
from hermes_cli.runs import runs_command


def test_runs_demo_and_resume_latest_answer(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    runs_command(Namespace(runs_command="demo"))
    out = capsys.readouterr().out
    assert "Created local Durable Runs hello world." in out

    runs_command(
        Namespace(
            runs_command="resume",
            run_id=None,
            latest=True,
            answer="yes",
            message=None,
            source_message_id=None,
        )
    )
    out = capsys.readouterr().out
    assert "Resolved decision" in out

    db = DurableRunDB(tmp_path / "execution_state.db")
    try:
        run = db.list_runs(limit=1)[0]
        decisions = db.list_decisions(run["run_id"])
        assert decisions[0]["status"] == "resolved"
        assert decisions[0]["answer_text"] == "yes"
    finally:
        db.close()


def test_runs_doctor_dry_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    runs_command(Namespace(runs_command="doctor", dry_run=True, rollback=None, db_path=None))
    out = capsys.readouterr().out
    assert "Dry run:" in out
