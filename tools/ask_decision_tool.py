#!/usr/bin/env python3
"""
Tracked decision tool for Durable Runs.

This is a thin schema/dispatch layer. Persistence is handled by the agent's
runtime DurableRunContext when available; otherwise it falls back to the same
interactive callback model as clarify.
"""

import json
from typing import Callable, List, Optional

from tools.registry import registry, tool_error


def ask_decision_tool(
    decision_key: str,
    question: str,
    answer_type: str = "open_text",
    choices: Optional[List[str]] = None,
    callback: Optional[Callable] = None,
) -> str:
    if not decision_key or not str(decision_key).strip():
        return tool_error("decision_key is required")
    if not question or not str(question).strip():
        return tool_error("question is required")
    if callback is None:
        return json.dumps(
            {
                "status": "waiting_for_user",
                "decision_key": str(decision_key).strip(),
                "question": str(question).strip(),
                "answer_type": str(answer_type or "open_text").strip() or "open_text",
                "choices_offered": choices or [],
            },
            ensure_ascii=False,
        )
    try:
        user_response = callback(str(question).strip(), choices or [])
    except Exception as exc:
        return json.dumps({"error": f"Failed to get decision: {exc}"}, ensure_ascii=False)
    return json.dumps(
        {
            "status": "answered",
            "decision_key": str(decision_key).strip(),
            "question": str(question).strip(),
            "answer_type": str(answer_type or "open_text").strip() or "open_text",
            "choices_offered": choices or [],
            "user_response": str(user_response or "").strip(),
        },
        ensure_ascii=False,
    )


ASK_DECISION_SCHEMA = {
    "name": "ask_decision",
    "description": (
        "Ask a tracked decision question that should be persisted as part of a "
        "Durable Run. Use this when the workflow needs a named decision that "
        "must survive retries, inspection, and resume."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "decision_key": {
                "type": "string",
                "description": "Stable identifier for this decision inside the workflow.",
            },
            "question": {
                "type": "string",
                "description": "Question to ask the user.",
            },
            "answer_type": {
                "type": "string",
                "description": "Expected answer type, for example yes_no, enum, or open_text.",
                "default": "open_text",
            },
            "choices": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional list of suggested choices.",
            },
        },
        "required": ["decision_key", "question"],
    },
}


registry.register(
    name="ask_decision",
    toolset="clarify",
    schema=ASK_DECISION_SCHEMA,
    handler=lambda args, **kw: ask_decision_tool(
        decision_key=args.get("decision_key", ""),
        question=args.get("question", ""),
        answer_type=args.get("answer_type", "open_text"),
        choices=args.get("choices"),
        callback=kw.get("callback"),
    ),
    check_fn=lambda: True,
    emoji="🧭",
)
