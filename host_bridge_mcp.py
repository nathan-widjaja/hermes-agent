#!/usr/bin/env python3
"""MCP server exposing explicit macOS host control tools to Hermes."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from tools.host_bridge import (
    DEFAULT_PORT,
    DEFAULT_PROFILE_DIRECTORY,
    host_applescript,
    host_browser_attach,
    host_browser_status,
    host_browser_x_reply,
    host_click,
    host_cursor_position,
    host_double_click,
    host_doctor,
    host_drag,
    host_browser_execute_javascript,
    host_browser_enable_apple_event_javascript,
    host_browser_tab_info,
    host_exec,
    host_focus_app,
    host_gui_doctor,
    host_hotkey,
    host_keystroke,
    host_paste,
    host_list_dir,
    host_move_mouse,
    host_open_url,
    host_open_app,
    host_press_key,
    host_read_path,
    host_screenshot,
    host_browser_window_screenshot,
    host_service_control,
    host_service_status,
    host_tail_path,
    host_type,
    host_ui_snapshot,
    host_write_path,
)

mcp = FastMCP(
    "hostbridge",
    instructions=(
        "Explicit macOS host-control bridge for Hermes production. "
        "Use host_exec for structured host shell commands, host_read_path/host_write_path for host files, "
        "host_service_status/host_service_control for launchd services, host_open_app/host_focus_app/"
        "host_keystroke/host_screenshot plus host_cursor_position/host_move_mouse/host_click/"
        "host_double_click/host_drag/host_type/host_press_key/host_hotkey/host_ui_snapshot/"
        "host_gui_doctor for desktop control, and host_open_url/"
        "host_browser_tab_info/host_browser_execute_javascript/"
        "host_browser_enable_apple_event_javascript plus host_browser_attach/"
        "host_browser_status for Nathan's real Chrome profile."
    ),
)


@mcp.tool()
def host_exec_tool(
    cmd: str,
    cwd: str = str(__import__("pathlib").Path.home()),
    timeout_sec: int = 20,
    allow_unlisted: bool = False,
    allow_destructive: bool = False,
) -> dict:
    """Run a macOS host command behind explicit allowlist/guardrail checks."""
    return host_exec(
        cmd,
        cwd=cwd,
        timeout_sec=timeout_sec,
        allow_unlisted=allow_unlisted,
        allow_destructive=allow_destructive,
    )


@mcp.tool()
def host_open_app_tool(app: str) -> dict:
    """Open a visible app on the real Mac host."""
    return host_open_app(app)


@mcp.tool()
def host_focus_app_tool(app: str) -> dict:
    """Bring an allowed app to the foreground on the real Mac host."""
    return host_focus_app(app)


@mcp.tool()
def host_applescript_tool(template: str, app: str | None = None) -> dict:
    """Run an allowlisted AppleScript template on the real Mac host."""
    return host_applescript(template, app=app)


@mcp.tool()
def host_keystroke_tool(text: str, app: str | None = None) -> dict:
    """Send a keystroke sequence through System Events on the real Mac host."""
    return host_keystroke(text, app=app)


@mcp.tool()
def host_cursor_position_tool() -> dict:
    """Return the current mouse cursor position on the real Mac host."""
    return host_cursor_position()


@mcp.tool()
def host_move_mouse_tool(x: int, y: int, wait_ms: int = 80) -> dict:
    """Move the mouse cursor to screen coordinates on the real Mac host."""
    return host_move_mouse(x, y, wait_ms=wait_ms)


@mcp.tool()
def host_click_tool(
    x: int | None = None,
    y: int | None = None,
    button: str = "left",
    wait_ms: int = 80,
) -> dict:
    """Click at screen coordinates or the current cursor position on the real Mac host."""
    return host_click(x, y, button=button, wait_ms=wait_ms)


@mcp.tool()
def host_double_click_tool(
    x: int | None = None,
    y: int | None = None,
    wait_ms: int = 80,
) -> dict:
    """Double-click at screen coordinates or the current cursor position on the real Mac host."""
    return host_double_click(x, y, wait_ms=wait_ms)


@mcp.tool()
def host_drag_tool(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    wait_ms: int = 80,
) -> dict:
    """Drag the mouse from one coordinate to another on the real Mac host."""
    return host_drag(start_x, start_y, end_x, end_y, wait_ms=wait_ms)


@mcp.tool()
def host_type_tool(text: str, app: str | None = None, wait_ms: int = 80) -> dict:
    """Type text into the foreground Mac app using accessibility events."""
    return host_type(text, app=app, wait_ms=wait_ms)


@mcp.tool()
def host_paste_tool(
    text: str,
    app: str | None = None,
    wait_ms: int = 80,
    preserve_clipboard: bool = True,
) -> dict:
    """Paste exact text into the foreground Mac app using the system clipboard."""
    return host_paste(text, app=app, wait_ms=wait_ms, preserve_clipboard=preserve_clipboard)


@mcp.tool()
def host_press_key_tool(key: str, wait_ms: int = 80) -> dict:
    """Press a special key such as return, tab, or esc in the foreground Mac app."""
    return host_press_key(key, wait_ms=wait_ms)


@mcp.tool()
def host_hotkey_tool(modifiers: list[str], key: str, wait_ms: int = 80) -> dict:
    """Press a hotkey chord such as cmd+s or cmd+shift+4 in the foreground Mac app."""
    return host_hotkey(modifiers, key, wait_ms=wait_ms)


@mcp.tool()
def host_ui_snapshot_tool(path: str | None = None) -> dict:
    """Capture a structured UI snapshot with screenshot, cursor position, and frontmost app."""
    return host_ui_snapshot(path=path)


@mcp.tool()
def host_open_url_tool(url: str, app: str = "Google Chrome") -> dict:
    """Open a URL in the real browser app on the Mac host."""
    return host_open_url(url, app=app)


@mcp.tool()
def host_browser_tab_info_tool(app: str = "Google Chrome") -> dict:
    """Return the active tab title and URL from the real browser app."""
    return host_browser_tab_info(app=app)


@mcp.tool()
def host_browser_execute_javascript_tool(script: str, app: str = "Google Chrome") -> dict:
    """Execute JavaScript in the active tab of the real browser app."""
    return host_browser_execute_javascript(script, app=app)


@mcp.tool()
def host_browser_enable_apple_event_javascript_tool(
    profile_directory: str = DEFAULT_PROFILE_DIRECTORY,
    relaunch_chrome: bool = True,
) -> dict:
    """Enable Chrome's 'Allow JavaScript from Apple Events' setting for the real profile."""
    return host_browser_enable_apple_event_javascript(
        profile_directory=profile_directory,
        relaunch_chrome=relaunch_chrome,
    )


@mcp.tool()
def host_list_dir_tool(
    path: str = str(__import__("pathlib").Path.home()),
    limit: int = 200,
    include_hidden: bool = False,
) -> dict:
    """List a real host directory with structured metadata."""
    return host_list_dir(path, limit=limit, include_hidden=include_hidden)


@mcp.tool()
def host_read_path_tool(path: str, max_bytes: int = 200000) -> dict:
    """Read a real host file as text."""
    return host_read_path(path, max_bytes=max_bytes)


@mcp.tool()
def host_tail_path_tool(path: str, lines: int = 200) -> dict:
    """Tail a real host file for log inspection."""
    return host_tail_path(path, lines=lines)


@mcp.tool()
def host_write_path_tool(
    path: str,
    content: str,
    append: bool = False,
    create_dirs: bool = True,
) -> dict:
    """Write a real host text file within Nathan's machine scope."""
    return host_write_path(path, content, append=append, create_dirs=create_dirs)


@mcp.tool()
def host_screenshot_tool(path: str | None = None, open_preview: bool = False) -> dict:
    """Capture a screenshot from the real Mac host."""
    return host_screenshot(path, open_preview=open_preview)


@mcp.tool()
def host_browser_window_screenshot_tool(path: str | None = None, app: str = "Google Chrome", open_preview: bool = False) -> dict:
    """Capture only the front browser window from the real Mac host."""
    return host_browser_window_screenshot(path, app=app, open_preview=open_preview)


@mcp.tool()
def host_service_status_tool(label: str) -> dict:
    """Inspect a user launchd service on the real Mac host."""
    return host_service_status(label)


@mcp.tool()
def host_service_control_tool(
    label: str,
    action: str,
    allow_unlisted_label: bool = False,
) -> dict:
    """Start, stop, or restart a user launchd service."""
    return host_service_control(label, action, allow_unlisted_label=allow_unlisted_label)


@mcp.tool()
def host_doctor_tool(include_screenshot_test: bool = False) -> dict:
    """Run a host capability doctor for Hermes production."""
    return host_doctor(include_screenshot_test=include_screenshot_test)


@mcp.tool()
def host_gui_doctor_tool() -> dict:
    """Run a GUI automation doctor against a harmless TextEdit surface."""
    return host_gui_doctor()


@mcp.tool()
def host_browser_attach_tool(
    port: int = DEFAULT_PORT,
    profile_directory: str = DEFAULT_PROFILE_DIRECTORY,
    x_account_verified: bool | None = None,
    gemini_verified: bool | None = None,
    force_restart_chrome: bool = False,
) -> dict:
    """Launch or attach Hermes browser tools to Nathan's real Chrome profile via CDP."""
    return host_browser_attach(
        port=port,
        profile_directory=profile_directory,
        x_account_verified=x_account_verified,
        gemini_verified=gemini_verified,
        force_restart_chrome=force_restart_chrome,
    )


@mcp.tool()
def host_browser_status_tool(port: int = DEFAULT_PORT) -> dict:
    """Report real-Chrome CDP readiness and current verification state."""
    return host_browser_status(port=port)


@mcp.tool()
def host_browser_x_reply_tool(
    tweet_url: str,
    text: str,
    app: str = "Google Chrome",
    submit: bool = False,
    wait_ms: int = 120,
) -> dict:
    """Compose an exact X reply in the correct reply surface, with optional submit."""
    return host_browser_x_reply(tweet_url, text, app=app, submit=submit, wait_ms=wait_ms)


if __name__ == "__main__":
    mcp.run()
