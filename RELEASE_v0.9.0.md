# Hermes Agent v0.9.0 (v2026.4.11)

**Release Date:** April 11, 2026

> The durable execution release — long-running gateway work can now survive inspection, tracked decisions, operator updates, and resume flows, while outbound screenshot delivery gets stricter artifact verification and the host bridge picks up exact paste, browser-window capture, and safer Telegram image fallback behavior.

---

## Highlights

- **Durable Runs / Execution Spine** — Hermes can now persist admitted long-running gateway work into a dedicated execution store, expose active run state in `/status`, track blocker decisions, queue operator updates, and resume/cancel runs through the new `hermes runs` CLI. This is the first slice of durable execution: decision state, effects, and run inspection survive retries and interruptions.

- **Tracked Decisions (`ask_decision`)** — The agent now has a named decision tool that persists questions and answers as part of a run, instead of treating every clarification as ephemeral chat state.

- **Gateway Run Visibility** — Active Durable Runs are surfaced directly in gateway `/status`, `/stop` now cancels the live durable execution record, and mid-run user messages can be queued for the current run instead of getting lost.

- **Artifact Verification for Proof Screenshots** — Hermes now blocks obviously mismatched or synthetic screenshot artifacts before sending them as proof, using filename heuristics plus best-effort OCR/title matching on macOS.

- **Host Bridge UX Upgrades** — The Mac host bridge now supports exact clipboard paste, front browser-window screenshots via ScreenCaptureKit, and composing X replies with explicit verification of the pasted text before optional submit.

- **Telegram Image Delivery Hardening** — Oversized Telegram photos now degrade to document upload instead of falling back to a broken text-path leak.

---

## Included in this release

- New `durable_runs.py` persistence layer and `hermes runs` CLI.
- Durable-run admission, claiming, heartbeat, tracked decision, delegated-result, and effect-recording hooks in gateway and agent runtime.
- New docs for Durable Runs and migration notes.
- New artifact verification module for outbound proof-like images.
- Host bridge CLI/MCP additions for paste, browser-window screenshot, and X reply flows.
- Expanded regression coverage for Durable Runs, gateway `/status`, host bridge, artifact verification, and Telegram image fallback.
