# Durable Runs Migration Notes

Durable Runs uses a separate SQLite database:

- session history stays in `state.db`
- execution state lives in `execution_state.db`

That split is intentional. It reduces write contention and gives rollback a clean boundary.

## Rollout

1. Turn on `execution_spine.enabled` in `config.yaml`
2. Keep `admission_mode: auto` unless you are forcing a rollout
3. Run `hermes runs doctor --dry-run`
4. Run one local demo with `hermes runs demo`
5. Verify gateway `/status` shows the active run before legacy session info

## Rollback

If the execution DB already exists, `hermes runs doctor` creates a timestamped backup before applying checks.

To restore:

```bash
hermes runs doctor --rollback
```

Or restore a specific backup:

```bash
hermes runs doctor --rollback ~/.hermes/execution_state.db.<timestamp>.bak
```
