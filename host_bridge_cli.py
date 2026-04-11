#!/usr/bin/env python3
"""CLI helpers for Hermes host bridge operations."""

from __future__ import annotations

import argparse
import json

from tools.host_bridge import (
    host_applescript,
    host_browser_attach,
    host_browser_status,
    host_click,
    host_cursor_position,
    host_double_click,
    host_doctor,
    host_drag,
    host_exec,
    host_focus_app,
    host_browser_execute_javascript,
    host_browser_x_reply,
    host_browser_enable_apple_event_javascript,
    host_browser_tab_info,
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


def _bool_flag(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(prog="host_bridge_cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    attach = subparsers.add_parser("attach")
    attach.add_argument("--port", type=int, default=9222)
    attach.add_argument("--profile-directory", default="Default")
    attach.add_argument("--x-verified", type=_bool_flag)
    attach.add_argument("--gemini-verified", type=_bool_flag)
    attach.add_argument("--force-restart-chrome", action="store_true")

    status = subparsers.add_parser("status")
    status.add_argument("--port", type=int, default=9222)

    exec_p = subparsers.add_parser("exec")
    exec_p.add_argument("--cmd", required=True)
    exec_p.add_argument("--cwd", default=str(__import__("pathlib").Path.home()))
    exec_p.add_argument("--timeout-sec", type=int, default=20)
    exec_p.add_argument("--allow-unlisted", action="store_true")
    exec_p.add_argument("--allow-destructive", action="store_true")

    open_p = subparsers.add_parser("open-app")
    open_p.add_argument("--app", required=True)

    focus_p = subparsers.add_parser("focus-app")
    focus_p.add_argument("--app", required=True)

    applescript = subparsers.add_parser("applescript")
    applescript.add_argument("--template", required=True)
    applescript.add_argument("--app")

    list_p = subparsers.add_parser("list-dir")
    list_p.add_argument("--path", default=str(__import__("pathlib").Path.home()))
    list_p.add_argument("--limit", type=int, default=200)
    list_p.add_argument("--include-hidden", action="store_true")

    read_p = subparsers.add_parser("read-path")
    read_p.add_argument("--path", required=True)
    read_p.add_argument("--max-bytes", type=int, default=200000)

    tail_p = subparsers.add_parser("tail-path")
    tail_p.add_argument("--path", required=True)
    tail_p.add_argument("--lines", type=int, default=200)

    write_p = subparsers.add_parser("write-path")
    write_p.add_argument("--path", required=True)
    write_p.add_argument("--content", required=True)
    write_p.add_argument("--append", action="store_true")
    write_p.add_argument("--no-create-dirs", action="store_true")

    screenshot_p = subparsers.add_parser("screenshot")
    screenshot_p.add_argument("--path")
    screenshot_p.add_argument("--open-preview", action="store_true")

    browser_window_screenshot_p = subparsers.add_parser("browser-window-screenshot")
    browser_window_screenshot_p.add_argument("--path")
    browser_window_screenshot_p.add_argument("--app", default="Google Chrome")
    browser_window_screenshot_p.add_argument("--open-preview", action="store_true")

    keystroke_p = subparsers.add_parser("keystroke")
    keystroke_p.add_argument("--text", required=True)
    keystroke_p.add_argument("--app")

    cursor_p = subparsers.add_parser("cursor-position")

    move_p = subparsers.add_parser("move-mouse")
    move_p.add_argument("--x", type=int, required=True)
    move_p.add_argument("--y", type=int, required=True)
    move_p.add_argument("--wait-ms", type=int, default=80)

    click_p = subparsers.add_parser("click")
    click_p.add_argument("--x", type=int)
    click_p.add_argument("--y", type=int)
    click_p.add_argument("--button", default="left")
    click_p.add_argument("--wait-ms", type=int, default=80)

    dbl_p = subparsers.add_parser("double-click")
    dbl_p.add_argument("--x", type=int)
    dbl_p.add_argument("--y", type=int)
    dbl_p.add_argument("--wait-ms", type=int, default=80)

    drag_p = subparsers.add_parser("drag")
    drag_p.add_argument("--start-x", type=int, required=True)
    drag_p.add_argument("--start-y", type=int, required=True)
    drag_p.add_argument("--end-x", type=int, required=True)
    drag_p.add_argument("--end-y", type=int, required=True)
    drag_p.add_argument("--wait-ms", type=int, default=80)

    type_p = subparsers.add_parser("type")
    type_p.add_argument("--text", required=True)
    type_p.add_argument("--app")
    type_p.add_argument("--wait-ms", type=int, default=80)

    paste_p = subparsers.add_parser("paste")
    paste_p.add_argument("--text", required=True)
    paste_p.add_argument("--app")
    paste_p.add_argument("--wait-ms", type=int, default=80)
    paste_p.add_argument("--no-preserve-clipboard", action="store_true")

    press_p = subparsers.add_parser("press-key")
    press_p.add_argument("--key", required=True)
    press_p.add_argument("--wait-ms", type=int, default=80)

    hotkey_p = subparsers.add_parser("hotkey")
    hotkey_p.add_argument("--modifiers", required=True)
    hotkey_p.add_argument("--key", required=True)
    hotkey_p.add_argument("--wait-ms", type=int, default=80)

    ui_snapshot_p = subparsers.add_parser("ui-snapshot")
    ui_snapshot_p.add_argument("--path")

    open_url_p = subparsers.add_parser("open-url")
    open_url_p.add_argument("--url", required=True)
    open_url_p.add_argument("--app", default="Google Chrome")

    tab_info_p = subparsers.add_parser("browser-tab-info")
    tab_info_p.add_argument("--app", default="Google Chrome")

    js_p = subparsers.add_parser("browser-js")
    js_p.add_argument("--script", required=True)
    js_p.add_argument("--app", default="Google Chrome")

    x_reply_p = subparsers.add_parser("browser-x-reply")
    x_reply_p.add_argument("--tweet-url", required=True)
    x_reply_p.add_argument("--text", required=True)
    x_reply_p.add_argument("--app", default="Google Chrome")
    x_reply_p.add_argument("--submit", action="store_true")
    x_reply_p.add_argument("--wait-ms", type=int, default=120)

    enable_js_p = subparsers.add_parser("browser-enable-apple-event-js")
    enable_js_p.add_argument("--profile-directory", default="Default")
    enable_js_p.add_argument("--no-relaunch-chrome", action="store_true")

    svc_status = subparsers.add_parser("service-status")
    svc_status.add_argument("--label", required=True)

    svc_control = subparsers.add_parser("service-control")
    svc_control.add_argument("--label", required=True)
    svc_control.add_argument("--action", required=True)
    svc_control.add_argument("--allow-unlisted-label", action="store_true")

    doctor = subparsers.add_parser("doctor")
    doctor.add_argument("--include-screenshot-test", action="store_true")

    gui_doctor = subparsers.add_parser("gui-doctor")

    args = parser.parse_args()

    if args.command == "attach":
        payload = host_browser_attach(
            port=args.port,
            profile_directory=args.profile_directory,
            x_account_verified=args.x_verified,
            gemini_verified=args.gemini_verified,
            force_restart_chrome=args.force_restart_chrome,
        )
    elif args.command == "status":
        payload = host_browser_status(port=args.port)
    elif args.command == "exec":
        payload = host_exec(
            args.cmd,
            cwd=args.cwd,
            timeout_sec=args.timeout_sec,
            allow_unlisted=args.allow_unlisted,
            allow_destructive=args.allow_destructive,
        )
    elif args.command == "open-app":
        payload = host_open_app(args.app)
    elif args.command == "focus-app":
        payload = host_focus_app(args.app)
    elif args.command == "list-dir":
        payload = host_list_dir(
            args.path,
            limit=args.limit,
            include_hidden=args.include_hidden,
        )
    elif args.command == "read-path":
        payload = host_read_path(args.path, max_bytes=args.max_bytes)
    elif args.command == "tail-path":
        payload = host_tail_path(args.path, lines=args.lines)
    elif args.command == "write-path":
        payload = host_write_path(
            args.path,
            args.content,
            append=args.append,
            create_dirs=not args.no_create_dirs,
        )
    elif args.command == "screenshot":
        payload = host_screenshot(args.path, open_preview=args.open_preview)
    elif args.command == "browser-window-screenshot":
        payload = host_browser_window_screenshot(args.path, app=args.app, open_preview=args.open_preview)
    elif args.command == "keystroke":
        payload = host_keystroke(args.text, app=args.app)
    elif args.command == "cursor-position":
        payload = host_cursor_position()
    elif args.command == "move-mouse":
        payload = host_move_mouse(args.x, args.y, wait_ms=args.wait_ms)
    elif args.command == "click":
        payload = host_click(args.x, args.y, button=args.button, wait_ms=args.wait_ms)
    elif args.command == "double-click":
        payload = host_double_click(args.x, args.y, wait_ms=args.wait_ms)
    elif args.command == "drag":
        payload = host_drag(
            args.start_x,
            args.start_y,
            args.end_x,
            args.end_y,
            wait_ms=args.wait_ms,
        )
    elif args.command == "type":
        payload = host_type(args.text, app=args.app, wait_ms=args.wait_ms)
    elif args.command == "paste":
        payload = host_paste(
            args.text,
            app=args.app,
            wait_ms=args.wait_ms,
            preserve_clipboard=not args.no_preserve_clipboard,
        )
    elif args.command == "press-key":
        payload = host_press_key(args.key, wait_ms=args.wait_ms)
    elif args.command == "hotkey":
        payload = host_hotkey(
            [m.strip() for m in args.modifiers.split(",") if m.strip()],
            args.key,
            wait_ms=args.wait_ms,
        )
    elif args.command == "ui-snapshot":
        payload = host_ui_snapshot(path=args.path)
    elif args.command == "open-url":
        payload = host_open_url(args.url, app=args.app)
    elif args.command == "browser-tab-info":
        payload = host_browser_tab_info(app=args.app)
    elif args.command == "browser-js":
        payload = host_browser_execute_javascript(args.script, app=args.app)
    elif args.command == "browser-x-reply":
        payload = host_browser_x_reply(
            args.tweet_url,
            args.text,
            app=args.app,
            submit=args.submit,
            wait_ms=args.wait_ms,
        )
    elif args.command == "browser-enable-apple-event-js":
        payload = host_browser_enable_apple_event_javascript(
            profile_directory=args.profile_directory,
            relaunch_chrome=not args.no_relaunch_chrome,
        )
    elif args.command == "service-status":
        payload = host_service_status(args.label)
    elif args.command == "service-control":
        payload = host_service_control(
            args.label,
            args.action,
            allow_unlisted_label=args.allow_unlisted_label,
        )
    elif args.command == "doctor":
        payload = host_doctor(include_screenshot_test=args.include_screenshot_test)
    elif args.command == "gui-doctor":
        payload = host_gui_doctor()
    else:
        payload = host_applescript(args.template, app=args.app)

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("ok", False) else 1


if __name__ == "__main__":
    raise SystemExit(main())
