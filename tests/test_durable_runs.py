from durable_runs import AdmissionDecision, DurableRunDB


def test_invalidated_decision_creates_new_version(tmp_path):
    db = DurableRunDB(tmp_path / "execution_state.db")
    try:
        run = db.create_run(
            session_id="sess-1",
            workflow_name="AA/Nathan Founder Setup",
            source_platform="telegram",
            source_chat_id="chat-1",
            user_id="u1",
            request_text="big founder setup request",
            admission=AdmissionDecision(True, "force", 1.0, ["test"], "forced"),
            claimant="test-claimant",
        )
        first = db.upsert_decision(
            run["run_id"],
            decision_key="google.scope",
            question_text="Use all Google Workspace apps?",
            answer_type="yes_no",
            choices=["yes", "no"],
        )
        assert first["decision_version"] == 1

        db.invalidate_decision(run["run_id"], decision_key="google.scope", reason="scope changed")

        second = db.upsert_decision(
            run["run_id"],
            decision_key="google.scope",
            question_text="Use all Google Workspace apps?",
            answer_type="yes_no",
            choices=["yes", "no"],
        )
        assert second["decision_version"] == 2
        assert second["supersedes_decision_id"] == first["decision_id"]
    finally:
        db.close()


def test_inspect_run_includes_subrecords(tmp_path):
    db = DurableRunDB(tmp_path / "execution_state.db")
    try:
        run = db.create_demo_run()
        payload = db.inspect_run(run["run_id"])
        assert payload["run"]["run_id"] == run["run_id"]
        assert payload["steps"]
        assert payload["decisions"]
    finally:
        db.close()
