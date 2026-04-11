# Durable Runs

Durable Runs is the V1 operator surface for long, stateful Hermes workflows.

The internal runtime is still called Execution Spine. The public contract is:

- enable
- run
- inspect
- pause
- resume
- cancel
- recover

## What V1 covers

V1 is anchored on the AA/Nathan founder setup workflow from the transcript review.

It adds:

- a separate execution store at `~/.hermes/execution_state.db`
- tracked decisions that survive retries and inspection
- typed delegated child results
- idempotency fencing for `send_message` and `cronjob`
- gateway `/status` that shows the active Durable Run first
- operator commands under `hermes runs`

## Commands

Use these commands to operate Durable Runs:

```bash
hermes runs list
hermes runs show <run_id>
hermes runs inspect <run_id> --json
hermes runs resume <run_id> --answer yes
hermes runs resume <run_id> --message "also wire Sheets and Slides"
hermes runs stop <run_id>
hermes runs doctor
```

## Hello World

Local hello world should take under five minutes:

```bash
hermes runs demo
hermes runs show --latest
hermes runs resume --latest --answer yes
hermes runs inspect --latest --json
```

## Gateway behavior

When a Durable Run is active in the gateway:

- `/status` shows the run first
- clarifying questions are tracked as decisions
- mid-run text updates are queued into the run instead of interrupting the whole execution
- `/stop` cancels the run and releases the active agent

## Doctor and recovery

Use `hermes runs doctor` to inspect the execution store.

- `--dry-run` reports what would happen
- default mode verifies the DB and makes a backup if one already exists
- `--rollback` restores the latest backup, or a specific backup path

## Current V1 limits

Deferred for later:

- dashboard UI
- generic DAG runtime
- broader side-effect fencing beyond `send_message` and `cronjob`
