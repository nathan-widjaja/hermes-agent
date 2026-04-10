#!/usr/bin/env python3
"""Host-side Hermes bridge for explicit macOS control."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.request import urlopen

from hermes_constants import get_hermes_home

from tools.browser_bridge_state import load_browser_bridge_state, save_browser_bridge_state

HOST_ROOT = Path.home()
HERMES_HOME = Path(get_hermes_home())
HOST_BRIDGE_DIR = HERMES_HOME / "state" / "host-bridge"
HOST_BRIDGE_LOG = HERMES_HOME / "logs" / "host-bridge.log"
X_ATTACH_SCRIPT = HOST_ROOT / ".openclaw" / "workspace" / "scripts" / "x-real-chrome-cdp-attach.sh"
X_STATUS_SCRIPT = HOST_ROOT / ".openclaw" / "workspace" / "scripts" / "x-real-chrome-cdp-check.sh"

DEFAULT_TIMEOUT_SEC = 20
DEFAULT_PORT = 9222
DEFAULT_PROFILE_DIRECTORY = "Default"
DEFAULT_MAX_READ_BYTES = 200_000
DEFAULT_TAIL_LINES = 200
DEFAULT_CLICK_WAIT_MS = 80

CLICLICK_CANDIDATES = (
    Path("/Users/nathan/.homebrew/bin/cliclick"),
    Path("/opt/homebrew/bin/cliclick"),
    Path("/usr/local/bin/cliclick"),
)

SAFE_COMMAND_PREFIXES = {
    "awk",
    "basename",
    "cat",
    "chmod",
    "cp",
    "curl",
    "defaults",
    "echo",
    "file",
    "find",
    "git",
    "grep",
    "head",
    "id",
    "launchctl",
    "ls",
    "mdfind",
    "mdls",
    "mkdir",
    "mv",
    "open",
    "osascript",
    "pgrep",
    "plutil",
    "ps",
    "pwd",
    "python",
    "python3",
    "qlmanage",
    "rg",
    "sed",
    "shasum",
    "stat",
    "sw_vers",
    "tail",
    "test",
    "touch",
    "tee",
    "uname",
    "whoami",
    "zsh",
}

ALLOWED_APPS = {
    "Activity Monitor",
    "Codex",
    "Finder",
    "Google Chrome",
    "System Settings",
    "Terminal",
    "Telegram",
    "TextEdit",
}

ALLOWED_SERVICE_LABELS = {
    "ai.hermes.gateway",
    "ai.hermes.refresh-on-policy-change",
    "ai.openclaw.gateway",
}

DESTRUCTIVE_PATTERNS = [
    re.compile(r"(^|[\s;&|])rm\s+-rf\b"),
    re.compile(r"(^|[\s;&|])rm\s+-r\b"),
    re.compile(r"(^|[\s;&|])git\s+reset\s+--hard\b"),
    re.compile(r"(^|[\s;&|])git\s+checkout\s+--\b"),
    re.compile(r"(^|[\s;&|])git\s+restore\s+--source\b"),
    re.compile(r"(^|[\s;&|])git\s+push\b.*\s(--force|-f)(\s|$)"),
    re.compile(r"(^|[\s;&|])diskutil\s+erase"),
    re.compile(r"(^|[\s;&|])launchctl\s+bootout\b"),
    re.compile(r"(^|[\s;&|])shutdown\b"),
    re.compile(r"(^|[\s;&|])reboot\b"),
    re.compile(r"(^|[\s;&|])mkfs\b"),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_host_dirs() -> None:
    HOST_BRIDGE_DIR.mkdir(parents=True, exist_ok=True)
    HOST_BRIDGE_LOG.parent.mkdir(parents=True, exist_ok=True)


def _append_log(event: str, payload: Dict[str, Any]) -> None:
    _ensure_host_dirs()
    entry = {"ts": _now_iso(), "event": event, "payload": payload}
    with HOST_BRIDGE_LOG.open("a") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def _expand_host_path(path: str, base: Optional[Path] = None) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = (base or HOST_ROOT) / candidate
    return candidate.resolve(strict=False)


def _path_allowed_for_write(path: Path) -> bool:
    allowed_roots = (HOST_ROOT.resolve(strict=False), Path("/tmp"))
    for root in allowed_roots:
        if path == root or root in path.parents:
            return True
    return False


def _command_prefix(cmd: str) -> str:
    try:
        parts = shlex.split(cmd, posix=True)
    except ValueError:
        return ""
    if not parts:
        return ""
    return os.path.basename(parts[0])


def _check_command_policy(cmd: str, allow_unlisted: bool, allow_destructive: bool) -> Optional[str]:
    normalized = (cmd or "").strip()
    if not normalized:
        return "Command is required."

    if not allow_destructive:
        for pattern in DESTRUCTIVE_PATTERNS:
            if pattern.search(normalized):
                return "Command matched a destructive-pattern guardrail."

    prefix = _command_prefix(normalized)
    if prefix in SAFE_COMMAND_PREFIXES or allow_unlisted:
        return None

    return (
        f"Command prefix '{prefix or 'unknown'}' is outside the safe allowlist. "
        "Retry with allow_unlisted=true only for non-destructive host actions."
    )


def _run_host_command(
    cmd: str,
    *,
    cwd: Optional[str] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    _ensure_host_dirs()
    working_dir = str(Path(cwd).expanduser()) if cwd else str(HOST_ROOT)
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)

    completed = subprocess.run(
        ["/bin/zsh", "-lc", cmd],
        cwd=working_dir,
        env=merged_env,
        capture_output=True,
        text=True,
        timeout=max(int(timeout_sec), 1),
    )
    result = {
        "ok": completed.returncode == 0,
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "cwd": working_dir,
        "timeout_sec": max(int(timeout_sec), 1),
    }
    _append_log("host_exec", {"cmd": cmd, "cwd": working_dir, "exit_code": completed.returncode})
    return result


def _run_host_args(
    args: List[str],
    *,
    cwd: Optional[str] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    env: Optional[Dict[str, str]] = None,
    event: str = "host_process",
) -> Dict[str, Any]:
    """Run a host command without shell interpolation."""
    _ensure_host_dirs()
    working_dir = str(Path(cwd).expanduser()) if cwd else str(HOST_ROOT)
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)

    completed = subprocess.run(
        args,
        cwd=working_dir,
        env=merged_env,
        capture_output=True,
        text=True,
        timeout=max(int(timeout_sec), 1),
    )
    result = {
        "ok": completed.returncode == 0,
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "cwd": working_dir,
        "timeout_sec": max(int(timeout_sec), 1),
        "argv": args,
    }
    _append_log(event, {"argv": args, "cwd": working_dir, "exit_code": completed.returncode})
    return result


def _result_from_exception(error: str, **extra: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"ok": False, "error": error}
    payload.update(extra)
    return payload


def host_exec(
    cmd: str,
    *,
    cwd: str = str(HOST_ROOT),
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    allow_unlisted: bool = False,
    allow_destructive: bool = False,
) -> Dict[str, Any]:
    """Run a host macOS shell command with policy gates."""
    blocked_reason = _check_command_policy(cmd, allow_unlisted, allow_destructive)
    if blocked_reason:
        result = {
            "ok": False,
            "blocked": True,
            "blocked_reason": blocked_reason,
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "cwd": str(Path(cwd).expanduser()),
            "timeout_sec": max(int(timeout_sec), 1),
        }
        _append_log("host_exec_blocked", {"cmd": cmd, "reason": blocked_reason})
        return result

    try:
        return _run_host_command(cmd, cwd=cwd, timeout_sec=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        result = {
            "ok": False,
            "blocked": False,
            "exit_code": None,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
            "cwd": str(Path(cwd).expanduser()),
            "timeout_sec": max(int(timeout_sec), 1),
            "error": f"Timed out after {max(int(timeout_sec), 1)}s",
        }
        _append_log("host_exec_timeout", {"cmd": cmd, "cwd": cwd, "timeout_sec": timeout_sec})
        return result


def host_open_app(app: str) -> Dict[str, Any]:
    """Open a visible macOS app."""
    if app not in ALLOWED_APPS:
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": (
                f"App '{app}' is not in the allowed host-open set: "
                f"{', '.join(sorted(ALLOWED_APPS))}"
            ),
        }
    return _run_host_command(f"/usr/bin/open -a {shlex.quote(app)}", timeout_sec=15)


def host_focus_app(app: str) -> Dict[str, Any]:
    """Bring an allowed app to the foreground."""
    if app not in ALLOWED_APPS:
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": (
                f"App '{app}' is not in the allowed host-focus set: "
                f"{', '.join(sorted(ALLOWED_APPS))}"
            ),
        }
    host_open_app(app)
    result = host_applescript("activate_app", app=app)
    result["action"] = "focus"
    return result


def host_applescript(template: str, app: Optional[str] = None) -> Dict[str, Any]:
    """Run a small allowlisted AppleScript template."""
    templates = {
        "activate_app": (
            app is not None,
            f'tell application {json.dumps(app or "")} to activate',
        ),
        "is_app_running": (
            app is not None,
            (
                'tell application "System Events" to return '
                f'(name of processes contains {json.dumps(app or "")})'
            ),
        ),
        "frontmost_app": (
            True,
            'tell application "System Events" to get name of first application process whose frontmost is true',
        ),
        "prepare_textedit_surface": (
            True,
            "\n".join(
                [
                    'tell application "TextEdit"',
                    "activate",
                    "if not (exists document 1) then make new document",
                    "set bounds of front window to {120, 120, 920, 720}",
                    "end tell",
                ]
            ),
        ),
    }

    valid, script = templates.get(template, (False, ""))
    if template not in templates:
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": (
                "Unknown AppleScript template. Allowed templates: "
                f"{', '.join(sorted(templates))}"
            ),
        }
    if not valid:
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": f"Template '{template}' requires an app value.",
        }

    result = _run_host_command(
        f"/usr/bin/osascript -e {shlex.quote(script)}",
        timeout_sec=15,
    )
    result["template"] = template
    result["app"] = app
    return result


def host_keystroke(text: str, app: Optional[str] = None) -> Dict[str, Any]:
    """Send a keystroke sequence via System Events."""
    if app:
        focus_result = host_focus_app(app)
        if not focus_result.get("ok"):
            return focus_result

    lines = []
    if app:
        lines.append(f'tell application {json.dumps(app)} to activate')
    lines.append(
        'tell application "System Events" to keystroke '
        f'{json.dumps(text)}'
    )
    script = "\n".join(lines)
    result = _run_host_command(
        f"/usr/bin/osascript -e {shlex.quote(script)}",
        timeout_sec=15,
    )
    result["text"] = text
    result["app"] = app
    return result


def _resolve_cliclick_path() -> Optional[Path]:
    for candidate in CLICLICK_CANDIDATES:
        if candidate.exists():
            return candidate
    found = shutil.which("cliclick")
    return Path(found) if found else None


def _require_cliclick() -> Path:
    binary = _resolve_cliclick_path()
    if binary:
        return binary
    raise FileNotFoundError(
        "cliclick is not installed. Install it with `brew install cliclick`."
    )


def _run_cliclick(commands: List[str], *, wait_ms: int = DEFAULT_CLICK_WAIT_MS) -> Dict[str, Any]:
    binary = _require_cliclick()
    argv = [str(binary), "-w", str(max(int(wait_ms), 20)), *commands]
    result = _run_host_args(argv, timeout_sec=20, event="host_cliclick")
    result["commands"] = commands
    result["binary"] = str(binary)
    result["permission_warning"] = "Accessibility privileges not enabled" in (result.get("stderr") or "")
    return result


def _coord(x: Optional[int], y: Optional[int]) -> str:
    if x is None and y is None:
        return "."
    if x is None or y is None:
        raise ValueError("Both x and y are required when specifying coordinates.")
    return f"{int(x)},{int(y)}"


def host_cursor_position() -> Dict[str, Any]:
    """Return the current cursor position on the real Mac host."""
    try:
        result = _run_cliclick(["p"])
    except FileNotFoundError as exc:
        return _result_from_exception(str(exc))
    raw = (result.get("stdout") or "").strip()
    match = re.search(r"(-?\d+),\s*(-?\d+)", raw)
    if match:
        result["x"] = int(match.group(1))
        result["y"] = int(match.group(2))
    else:
        result["ok"] = False
        result["error"] = f"Could not parse cursor position from output: {raw}"
    return result


def host_move_mouse(x: int, y: int, *, wait_ms: int = DEFAULT_CLICK_WAIT_MS) -> Dict[str, Any]:
    """Move the mouse to screen coordinates."""
    try:
        result = _run_cliclick([f"m:{int(x)},{int(y)}"], wait_ms=wait_ms)
    except FileNotFoundError as exc:
        return _result_from_exception(str(exc))
    result["x"] = int(x)
    result["y"] = int(y)
    return result


def host_click(
    x: Optional[int] = None,
    y: Optional[int] = None,
    *,
    button: str = "left",
    wait_ms: int = DEFAULT_CLICK_WAIT_MS,
) -> Dict[str, Any]:
    """Click at coordinates or at the current cursor position."""
    command_map = {
        "left": "c",
        "right": "rc",
    }
    if button not in command_map:
        return _result_from_exception("Unsupported button. Allowed values: left, right", button=button)
    try:
        result = _run_cliclick([f"{command_map[button]}:{_coord(x, y)}"], wait_ms=wait_ms)
    except (FileNotFoundError, ValueError) as exc:
        return _result_from_exception(str(exc), button=button)
    result["button"] = button
    if x is not None:
        result["x"] = int(x)
    if y is not None:
        result["y"] = int(y)
    return result


def host_double_click(
    x: Optional[int] = None,
    y: Optional[int] = None,
    *,
    wait_ms: int = DEFAULT_CLICK_WAIT_MS,
) -> Dict[str, Any]:
    """Double-click at coordinates or current cursor position."""
    try:
        result = _run_cliclick([f"dc:{_coord(x, y)}"], wait_ms=wait_ms)
    except (FileNotFoundError, ValueError) as exc:
        return _result_from_exception(str(exc))
    if x is not None:
        result["x"] = int(x)
    if y is not None:
        result["y"] = int(y)
    return result


def host_drag(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    *,
    wait_ms: int = DEFAULT_CLICK_WAIT_MS,
) -> Dict[str, Any]:
    """Drag from one coordinate to another."""
    try:
        result = _run_cliclick(
            [
                f"dd:{int(start_x)},{int(start_y)}",
                f"dm:{int(end_x)},{int(end_y)}",
                f"du:{int(end_x)},{int(end_y)}",
            ],
            wait_ms=wait_ms,
        )
    except FileNotFoundError as exc:
        return _result_from_exception(str(exc))
    result.update(
        {
            "start_x": int(start_x),
            "start_y": int(start_y),
            "end_x": int(end_x),
            "end_y": int(end_y),
        }
    )
    return result


def host_type(text: str, app: Optional[str] = None, *, wait_ms: int = DEFAULT_CLICK_WAIT_MS) -> Dict[str, Any]:
    """Type text into the frontmost app using macOS accessibility events."""
    if app:
        focus_result = host_focus_app(app)
        if not focus_result.get("ok"):
            return focus_result
    lines = text.splitlines() or [""]
    commands: List[str] = []
    for idx, line in enumerate(lines):
        commands.append(f"t:{line}")
        if idx < len(lines) - 1:
            commands.append("kp:return")
    try:
        result = _run_cliclick(commands, wait_ms=wait_ms)
    except FileNotFoundError as exc:
        return _result_from_exception(str(exc), app=app)
    result["text"] = text
    result["app"] = app
    return result


def host_press_key(key: str, *, wait_ms: int = DEFAULT_CLICK_WAIT_MS) -> Dict[str, Any]:
    """Press a single special key in the foreground app."""
    try:
        result = _run_cliclick([f"kp:{key}"], wait_ms=wait_ms)
    except FileNotFoundError as exc:
        return _result_from_exception(str(exc), key=key)
    result["key"] = key
    return result


def host_hotkey(modifiers: List[str], key: str, *, wait_ms: int = DEFAULT_CLICK_WAIT_MS) -> Dict[str, Any]:
    """Press a hotkey chord such as cmd+s or cmd+shift+4."""
    if not modifiers:
        return host_press_key(key, wait_ms=wait_ms)
    mods = ",".join(modifiers)
    key_command = f"kp:{key}"
    if len(key) == 1 and key.isprintable():
        key_command = f"t:{key}"
    try:
        result = _run_cliclick(
            [f"kd:{mods}", key_command, f"ku:{mods}"],
            wait_ms=wait_ms,
        )
    except FileNotFoundError as exc:
        return _result_from_exception(str(exc), key=key, modifiers=modifiers)
    result["key"] = key
    result["modifiers"] = modifiers
    return result


def host_ui_snapshot(path: Optional[str] = None) -> Dict[str, Any]:
    """Capture a structured UI snapshot of the real Mac host."""
    frontmost = host_applescript("frontmost_app")
    cursor = host_cursor_position()
    screenshot = host_screenshot(path)
    payload: Dict[str, Any] = {
        "ok": frontmost.get("ok", False) and cursor.get("ok", False) and screenshot.get("ok", False),
        "frontmost_app": frontmost.get("stdout", "").strip(),
        "cursor": {
            "x": cursor.get("x"),
            "y": cursor.get("y"),
        },
        "screenshot": {
            "path": screenshot.get("path"),
            "bytes": screenshot.get("bytes", 0),
            "ok": screenshot.get("ok", False),
        },
        "frontmost_check": frontmost,
        "cursor_check": cursor,
        "screenshot_check": screenshot,
    }
    if payload["frontmost_app"] == "Google Chrome":
        payload["browser_tab"] = host_browser_tab_info()
    _append_log(
        "host_ui_snapshot",
        {
            "ok": payload["ok"],
            "frontmost_app": payload["frontmost_app"],
            "screenshot_path": payload["screenshot"]["path"],
        },
    )
    return payload


def host_gui_doctor() -> Dict[str, Any]:
    """Run a GUI automation doctor against a harmless TextEdit surface."""
    checks: Dict[str, Any] = {
        "prepare_surface": host_applescript("prepare_textedit_surface"),
        "cursor_before": host_cursor_position(),
    }
    checks["move_mouse"] = host_move_mouse(240, 220)
    checks["click"] = host_click(240, 220)
    checks["type"] = host_type("Hermes GUI doctor test.", app="TextEdit")
    checks["press_key"] = host_press_key("return")
    checks["hotkey"] = host_hotkey(["cmd"], "a")
    checks["ui_snapshot"] = host_ui_snapshot()

    checks["summary"] = {
        "prepare_surface_ok": checks["prepare_surface"].get("ok", False),
        "cursor_ok": checks["cursor_before"].get("ok", False),
        "move_ok": checks["move_mouse"].get("ok", False),
        "click_ok": checks["click"].get("ok", False),
        "type_ok": checks["type"].get("ok", False),
        "press_key_ok": checks["press_key"].get("ok", False),
        "hotkey_ok": checks["hotkey"].get("ok", False),
        "ui_snapshot_ok": checks["ui_snapshot"].get("ok", False),
        "accessibility_granted": not any(
            checks[name].get("permission_warning", False)
            for name in ("cursor_before", "move_mouse", "click", "type", "press_key", "hotkey")
        ),
    }
    checks["ok"] = all(checks["summary"].values())
    _append_log("host_gui_doctor", checks["summary"])
    return checks


def host_open_url(url: str, app: str = "Google Chrome") -> Dict[str, Any]:
    """Open a URL in a real browser app on the Mac host."""
    if app not in {"Google Chrome", "Safari"}:
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": "URL opening is only enabled for Google Chrome or Safari.",
        }
    script = "\n".join(
        [
            f'tell application {json.dumps(app)}',
            "activate",
            f"open location {json.dumps(url)}",
            "end tell",
        ]
    )
    result = _run_host_command(
        f"/usr/bin/osascript -e {shlex.quote(script)}",
        timeout_sec=15,
    )
    result["url"] = url
    result["app"] = app
    return result


def _chrome_preferences_path(profile_directory: str) -> Path:
    return HOST_ROOT / "Library" / "Application Support" / "Google" / "Chrome" / profile_directory / "Preferences"


def host_browser_enable_apple_event_javascript(
    profile_directory: str = DEFAULT_PROFILE_DIRECTORY,
    relaunch_chrome: bool = True,
) -> Dict[str, Any]:
    """Enable Chrome's 'Allow JavaScript from Apple Events' preference."""
    prefs_path = _chrome_preferences_path(profile_directory)
    if not prefs_path.exists():
        return _result_from_exception(
            f"Chrome preferences not found for profile '{profile_directory}'",
            path=str(prefs_path),
        )

    chrome_was_running = _is_chrome_running()
    if chrome_was_running:
        _run_host_command(
            "/usr/bin/osascript -e 'tell application \"Google Chrome\" to quit'",
            timeout_sec=15,
        )
        _wait_for_chrome_exit(timeout_sec=15)

    prefs = json.loads(prefs_path.read_text())
    browser_prefs = prefs.setdefault("browser", {})
    browser_prefs["allow_javascript_apple_events"] = True
    prefs_path.write_text(json.dumps(prefs, separators=(",", ":")))

    if relaunch_chrome:
        host_open_app("Google Chrome")

    result = {
        "ok": True,
        "profile_directory": profile_directory,
        "path": str(prefs_path),
        "chrome_was_running": chrome_was_running,
        "relaunch_chrome": relaunch_chrome,
    }
    _append_log("host_browser_enable_apple_event_javascript", result)
    return result


def host_browser_tab_info(app: str = "Google Chrome") -> Dict[str, Any]:
    """Return the active tab title and URL from a real browser app."""
    script = "\n".join(
        [
            f'tell application {json.dumps(app)}',
            'set theTitle to title of active tab of front window',
            'set theURL to URL of active tab of front window',
            'return theTitle & "\\n" & theURL',
            "end tell",
        ]
    )
    result = _run_host_command(
        f"/usr/bin/osascript -e {shlex.quote(script)}",
        timeout_sec=15,
    )
    if result.get("ok"):
        lines = result.get("stdout", "").splitlines()
        result["title"] = lines[0] if lines else ""
        result["url"] = lines[1] if len(lines) > 1 else ""
    result["app"] = app
    return result


def host_browser_execute_javascript(script: str, app: str = "Google Chrome") -> Dict[str, Any]:
    """Execute JavaScript in the active tab of a real browser app."""
    apple_script = "\n".join(
        [
            f'tell application {json.dumps(app)}',
            f"set jsResult to execute active tab of front window javascript {json.dumps(script)}",
            'return jsResult as text',
            "end tell",
        ]
    )
    result = _run_host_command(
        f"/usr/bin/osascript -e {shlex.quote(apple_script)}",
        timeout_sec=20,
    )
    result["app"] = app
    return result


def host_list_dir(path: str = str(HOST_ROOT), limit: int = 200, include_hidden: bool = False) -> Dict[str, Any]:
    """List a host directory with structured metadata."""
    target = _expand_host_path(path)
    if not target.exists():
        return _result_from_exception(f"Path does not exist: {target}", path=str(target))
    if not target.is_dir():
        return _result_from_exception(f"Path is not a directory: {target}", path=str(target))

    entries = []
    for child in sorted(target.iterdir(), key=lambda item: item.name.lower()):
        if not include_hidden and child.name.startswith("."):
            continue
        entry_type = "symlink" if child.is_symlink() else ("dir" if child.is_dir() else "file")
        entries.append(
            {
                "name": child.name,
                "path": str(child),
                "type": entry_type,
            }
        )
        if len(entries) >= max(int(limit), 1):
            break

    result = {
        "ok": True,
        "path": str(target),
        "entries": entries,
        "count": len(entries),
        "truncated": len(entries) >= max(int(limit), 1),
    }
    _append_log("host_list_dir", {"path": str(target), "count": len(entries)})
    return result


def host_read_path(path: str, max_bytes: int = DEFAULT_MAX_READ_BYTES) -> Dict[str, Any]:
    """Read a host file as text with truncation metadata."""
    target = _expand_host_path(path)
    if not target.exists():
        return _result_from_exception(f"Path does not exist: {target}", path=str(target))
    if not target.is_file():
        return _result_from_exception(f"Path is not a file: {target}", path=str(target))

    raw = target.read_bytes()
    limited = raw[: max(int(max_bytes), 1)]
    text = limited.decode("utf-8", errors="replace")
    result = {
        "ok": True,
        "path": str(target),
        "content": text,
        "truncated": len(raw) > len(limited),
        "bytes_read": len(limited),
        "total_bytes": len(raw),
    }
    _append_log("host_read_path", {"path": str(target), "bytes_read": len(limited)})
    return result


def host_tail_path(path: str, lines: int = DEFAULT_TAIL_LINES) -> Dict[str, Any]:
    """Read the tail of a host file."""
    target = _expand_host_path(path)
    if not target.exists():
        return _result_from_exception(f"Path does not exist: {target}", path=str(target))
    if not target.is_file():
        return _result_from_exception(f"Path is not a file: {target}", path=str(target))

    line_count = max(int(lines), 1)
    content_lines = target.read_text(errors="replace").splitlines()
    tail_lines = content_lines[-line_count:]
    result = {
        "ok": True,
        "path": str(target),
        "lines": tail_lines,
        "line_count": len(tail_lines),
        "requested_lines": line_count,
    }
    _append_log("host_tail_path", {"path": str(target), "line_count": len(tail_lines)})
    return result


def host_write_path(
    path: str,
    content: str,
    *,
    append: bool = False,
    create_dirs: bool = True,
) -> Dict[str, Any]:
    """Write text to a host file within the user-owned machine scope."""
    target = _expand_host_path(path)
    if not _path_allowed_for_write(target):
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": f"Write path is outside the allowed host scope: {target}",
            "path": str(target),
        }

    if create_dirs:
        target.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if append else "w"
    with target.open(mode, encoding="utf-8") as handle:
        handle.write(content)

    result = {
        "ok": True,
        "path": str(target),
        "append": append,
        "bytes_written": len(content.encode("utf-8")),
    }
    _append_log("host_write_path", {"path": str(target), "append": append})
    return result


def host_screenshot(path: Optional[str] = None, open_preview: bool = False) -> Dict[str, Any]:
    """Capture a screenshot from the real Mac host."""
    if path:
        target = _expand_host_path(path)
    else:
        target = HOST_BRIDGE_DIR / "screenshots" / f"screenshot-{datetime.now().strftime('%Y%m%d-%H%M%S')}.png"
    target.parent.mkdir(parents=True, exist_ok=True)

    result = _run_host_command(
        f"/usr/sbin/screencapture -x {shlex.quote(str(target))}",
        timeout_sec=20,
    )
    result["path"] = str(target)
    if result.get("ok"):
        result["bytes"] = target.stat().st_size if target.exists() else 0
        if open_preview:
            host_open_app("Finder")
            _run_host_command(f"/usr/bin/open {shlex.quote(str(target))}", timeout_sec=10)
    return result


def _read_cdp_version(port: int) -> Dict[str, Any]:
    version_url = f"http://127.0.0.1:{port}/json/version"
    with urlopen(version_url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _is_chrome_running() -> bool:
    completed = subprocess.run(
        ["/bin/zsh", "-lc", "pgrep -f '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' >/dev/null"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return completed.returncode == 0


def _wait_for_chrome_exit(timeout_sec: int = 15) -> bool:
    deadline = time.time() + max(timeout_sec, 1)
    while time.time() < deadline:
        if not _is_chrome_running():
            return True
        time.sleep(0.5)
    return not _is_chrome_running()


def _service_target(label: str) -> str:
    return f"gui/{os.getuid()}/{label}"


def host_service_status(label: str) -> Dict[str, Any]:
    """Inspect a user launchd service."""
    target = _service_target(label)
    result = _run_host_command(f"/bin/launchctl print {shlex.quote(target)}", timeout_sec=15)
    stdout = result.get("stdout", "")
    pid_match = re.search(r'"pid"\s*=\s*(\d+)', stdout)
    last_exit_match = re.search(r'"last exit code"\s*=\s*(\d+)', stdout)
    result.update(
        {
            "label": label,
            "service_target": target,
            "loaded": result.get("ok", False),
            "pid": int(pid_match.group(1)) if pid_match else None,
            "last_exit_code": int(last_exit_match.group(1)) if last_exit_match else None,
        }
    )
    return result


def host_service_control(label: str, action: str, allow_unlisted_label: bool = False) -> Dict[str, Any]:
    """Start, stop, or restart a user launchd service."""
    if label not in ALLOWED_SERVICE_LABELS and not allow_unlisted_label:
        return {
            "ok": False,
            "blocked": True,
            "blocked_reason": (
                f"Service label '{label}' is outside the default control set: "
                f"{', '.join(sorted(ALLOWED_SERVICE_LABELS))}"
            ),
        }

    target = _service_target(label)
    if action == "restart":
        cmd = f"/bin/launchctl kickstart -k {shlex.quote(target)}"
    elif action == "start":
        cmd = f"/bin/launchctl kickstart {shlex.quote(target)}"
    elif action == "stop":
        cmd = f"/bin/launchctl kill TERM {shlex.quote(target)}"
    else:
        return _result_from_exception(
            "Unknown service action. Allowed actions: start, stop, restart",
            label=label,
            action=action,
        )

    result = _run_host_command(cmd, timeout_sec=20)
    result["label"] = label
    result["action"] = action
    result["service_target"] = target
    return result


def host_doctor(include_screenshot_test: bool = False) -> Dict[str, Any]:
    """Run a host-capability doctor for Hermes production."""
    checks = {
        "host_exec": host_exec("uname"),
        "applications_visible": host_list_dir("/Applications", limit=5),
        "frontmost_app": host_applescript("frontmost_app"),
        "hermes_gateway": host_service_status("ai.hermes.gateway"),
        "browser_status": host_browser_status(),
        "browser_tab_info": host_browser_tab_info(),
        "browser_js": host_browser_execute_javascript("document.title"),
    }
    if include_screenshot_test:
        checks["screenshot"] = host_screenshot()

    checks["summary"] = {
        "host_exec_ok": checks["host_exec"].get("ok", False),
        "applications_visible_ok": checks["applications_visible"].get("ok", False),
        "automation_ok": checks["frontmost_app"].get("ok", False),
        "hermes_gateway_ok": checks["hermes_gateway"].get("loaded", False),
        "browser_control_ok": (
            checks["browser_tab_info"].get("ok", False)
            and checks["browser_js"].get("ok", False)
        ),
        "browser_cdp_ready": checks["browser_status"].get("ready", False),
    }
    checks["ok"] = all(
        checks["summary"][key]
        for key in (
            "host_exec_ok",
            "applications_visible_ok",
            "automation_ok",
            "hermes_gateway_ok",
            "browser_control_ok",
        )
    )
    _append_log("host_doctor", checks["summary"])
    return checks


def host_browser_status(port: int = DEFAULT_PORT) -> Dict[str, Any]:
    """Return structured status for the real Chrome CDP bridge."""
    _ensure_host_dirs()
    state = load_browser_bridge_state()
    try:
        payload = _read_cdp_version(port)
        ready = True
        error = ""
    except Exception as exc:
        payload = {}
        ready = False
        error = str(exc)

    result = {
        "ok": ready,
        "ready": ready,
        "port": int(port),
        "mode": "real-chrome-cdp" if ready else "not-ready",
        "browser": payload.get("Browser", ""),
        "protocol_version": payload.get("Protocol-Version", ""),
        "websocket_url": payload.get("webSocketDebuggerUrl", state.get("websocket_url", "")),
        "state_file": str(HOST_BRIDGE_DIR / "state.json"),
        "profile_directory": state.get("profile_directory", DEFAULT_PROFILE_DIRECTORY),
        "chrome_running": _is_chrome_running(),
        "x_account_verified": state.get("x_account_verified"),
        "gemini_verified": state.get("gemini_verified"),
        "last_attached_at": state.get("last_attached_at"),
        "source": state.get("source", "host-bridge"),
    }
    if error:
        result["error"] = error
    _append_log("host_browser_status", {"port": int(port), "ready": ready})
    return result


def host_browser_attach(
    *,
    port: int = DEFAULT_PORT,
    profile_directory: str = DEFAULT_PROFILE_DIRECTORY,
    x_account_verified: Optional[bool] = None,
    gemini_verified: Optional[bool] = None,
    force_restart_chrome: bool = False,
) -> Dict[str, Any]:
    """Launch or attach to Nathan's real Chrome profile via CDP."""
    env = dict(os.environ)
    env["X_CHROME_PROFILE_DIR"] = profile_directory
    cmd = f"{shlex.quote(str(X_ATTACH_SCRIPT))} {int(port)}"

    if force_restart_chrome and _is_chrome_running():
        _run_host_command(
            "/usr/bin/osascript -e 'tell application \"Google Chrome\" to quit'",
            timeout_sec=15,
        )
        _wait_for_chrome_exit(timeout_sec=15)

    result = _run_host_command(cmd, timeout_sec=45, env=env)
    stdout = result.get("stdout", "")
    ws_match = re.search(r"webSocketDebuggerUrl:\s*(\S+)", stdout)
    websocket_url = ws_match.group(1) if ws_match else ""
    already_running = "already appears live" in stdout.lower()

    status = host_browser_status(int(port))
    if not status.get("ready") and _is_chrome_running() and not force_restart_chrome:
        result["next_step"] = (
            "Chrome is already running without CDP on the real profile. "
            "Retry with force_restart_chrome=true to restart Chrome into attachable mode."
        )
        result["needs_restart"] = True
    persisted = load_browser_bridge_state()
    save_browser_bridge_state(
        {
            **persisted,
            "ready": status.get("ready", False),
            "mode": "real-chrome-cdp" if status.get("ready") else "attach-failed",
            "websocket_url": websocket_url or status.get("websocket_url", ""),
            "cdp_url": websocket_url or status.get("websocket_url", ""),
            "port": int(port),
            "profile_directory": profile_directory,
            "last_attached_at": _now_iso(),
            "source": "host_browser_attach",
            "x_account_verified": (
                x_account_verified
                if x_account_verified is not None
                else persisted.get("x_account_verified", False)
            ),
            "gemini_verified": (
                gemini_verified
                if gemini_verified is not None
                else persisted.get("gemini_verified", False)
            ),
        }
    )

    result.update(
        {
            "port": int(port),
            "profile_directory": profile_directory,
            "already_running": already_running,
            "force_restart_chrome": force_restart_chrome,
            "websocket_url": websocket_url or status.get("websocket_url", ""),
            "status": status,
        }
    )
    _append_log(
        "host_browser_attach",
        {
            "port": int(port),
            "profile_directory": profile_directory,
            "already_running": already_running,
            "ok": result.get("ok", False),
        },
    )
    return result
