from pathlib import Path


def test_host_exec_blocks_destructive_command():
    from tools.host_bridge import host_exec

    result = host_exec("rm -rf /tmp/demo", allow_unlisted=True)

    assert result["ok"] is False
    assert result["blocked"] is True
    assert "destructive-pattern" in result["blocked_reason"]


def test_host_exec_blocks_unlisted_command_by_default():
    from tools.host_bridge import host_exec

    result = host_exec("brew services list")

    assert result["ok"] is False
    assert result["blocked"] is True
    assert "safe allowlist" in result["blocked_reason"]


def test_host_list_dir_returns_entries(tmp_path, monkeypatch):
    import tools.host_bridge as host_bridge

    target = tmp_path / "demo"
    target.mkdir()
    (target / "a.txt").write_text("hi")
    (target / "b.txt").write_text("there")
    monkeypatch.setattr(host_bridge, "HOST_ROOT", tmp_path)

    result = host_bridge.host_list_dir(str(target), limit=10)

    assert result["ok"] is True
    assert result["count"] == 2
    assert result["entries"][0]["name"] == "a.txt"


def test_host_read_and_write_path_roundtrip(tmp_path, monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "HOST_ROOT", tmp_path)
    target = tmp_path / "notes.txt"

    write_result = host_bridge.host_write_path(str(target), "hello world")
    read_result = host_bridge.host_read_path(str(target))

    assert write_result["ok"] is True
    assert read_result["ok"] is True
    assert read_result["content"] == "hello world"


def test_host_service_control_blocks_unlisted_label():
    from tools.host_bridge import host_service_control

    result = host_service_control("com.apple.Safari", "restart")

    assert result["ok"] is False
    assert result["blocked"] is True
    assert "default control set" in result["blocked_reason"]


def test_host_browser_status_reads_persisted_state_when_port_unreachable(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_LOG", tmp_path / "logs" / "host-bridge.log")
    monkeypatch.setattr(host_bridge, "_read_cdp_version", lambda port: (_ for _ in ()).throw(OSError("boom")))
    monkeypatch.setattr(
        host_bridge,
        "_real_browser_control_status",
        lambda app="Google Chrome": {
            "ok": False,
            "app": app,
            "url": "",
            "title": "",
            "tab_info": {"ok": False},
            "javascript": {"ok": False},
        },
    )
    monkeypatch.setattr(
        host_bridge,
        "load_browser_bridge_state",
        lambda: {
            "profile_directory": "Profile 2",
            "websocket_url": "ws://127.0.0.1:9222/devtools/browser/demo",
            "x_account_verified": True,
            "gemini_verified": False,
            "last_attached_at": "2026-04-10T00:00:00+00:00",
            "source": "test",
        },
    )

    result = host_bridge.host_browser_status(port=9222)

    assert result["ready"] is False
    assert result["cdp_ready"] is False
    assert result["control_ready"] is False
    assert result["websocket_url"] == "ws://127.0.0.1:9222/devtools/browser/demo"
    assert result["profile_directory"] == "Profile 2"
    assert result["x_account_verified"] is True


def test_host_browser_status_falls_back_to_real_chrome_control_when_cdp_unreachable(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_LOG", tmp_path / "logs" / "host-bridge.log")
    monkeypatch.setattr(host_bridge, "_read_cdp_version", lambda port: (_ for _ in ()).throw(OSError("boom")))
    monkeypatch.setattr(
        host_bridge,
        "_real_browser_control_status",
        lambda app="Google Chrome": {
            "ok": True,
            "app": app,
            "url": "https://x.com/home",
            "title": "Home / X",
            "tab_info": {"ok": True, "url": "https://x.com/home", "title": "Home / X"},
            "javascript": {"ok": True, "stdout": "https://x.com/home\n"},
        },
    )
    monkeypatch.setattr(
        host_bridge,
        "load_browser_bridge_state",
        lambda: {
            "profile_directory": "Default",
            "x_account_verified": True,
            "gemini_verified": False,
            "last_attached_at": "2026-04-10T00:00:00+00:00",
            "source": "test",
        },
    )
    monkeypatch.setattr(host_bridge, "_is_chrome_running", lambda: True)

    result = host_bridge.host_browser_status(port=9222)

    assert result["ready"] is True
    assert result["cdp_ready"] is False
    assert result["control_ready"] is True
    assert result["mode"] == "real-chrome-apple-events"
    assert result["active_tab_url"] == "https://x.com/home"


def test_host_browser_attach_persists_browser_state(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "X_ATTACH_SCRIPT", Path("/tmp/fake-attach.sh"))
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_LOG", tmp_path / "logs" / "host-bridge.log")
    monkeypatch.setattr(host_bridge, "_is_chrome_running", lambda: False)
    monkeypatch.setattr(
        host_bridge,
        "_run_host_command",
        lambda *args, **kwargs: {
            "ok": True,
            "exit_code": 0,
            "stdout": "CDP endpoint is live.\nwebSocketDebuggerUrl: ws://127.0.0.1:9222/devtools/browser/demo\n",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        host_bridge,
        "host_browser_status",
        lambda port=9222: {
            "ok": True,
            "ready": True,
            "port": port,
            "websocket_url": "ws://127.0.0.1:9222/devtools/browser/demo",
        },
    )
    monkeypatch.setattr(host_bridge, "load_browser_bridge_state", lambda: {})

    saved = {}

    def _save(payload):
        saved.update(payload)
        return tmp_path / "state" / "state.json"

    monkeypatch.setattr(host_bridge, "save_browser_bridge_state", _save)

    result = host_bridge.host_browser_attach(
        port=9222,
        profile_directory="Profile 1",
        x_account_verified=True,
        gemini_verified=False,
    )

    assert result["ok"] is True
    assert result["websocket_url"] == "ws://127.0.0.1:9222/devtools/browser/demo"
    assert saved["profile_directory"] == "Profile 1"
    assert saved["x_account_verified"] is True
    assert saved["gemini_verified"] is False


def test_host_browser_attach_succeeds_with_real_chrome_control_fallback(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "X_ATTACH_SCRIPT", Path("/tmp/fake-attach.sh"))
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_LOG", tmp_path / "logs" / "host-bridge.log")
    monkeypatch.setattr(host_bridge, "_is_chrome_running", lambda: True)
    monkeypatch.setattr(
        host_bridge,
        "_run_host_command",
        lambda *args, **kwargs: {
            "ok": False,
            "exit_code": 2,
            "stdout": "",
            "stderr": "no cdp",
        },
    )
    monkeypatch.setattr(
        host_bridge,
        "host_browser_status",
        lambda port=9222: {
            "ok": True,
            "ready": True,
            "cdp_ready": False,
            "control_ready": True,
            "mode": "real-chrome-apple-events",
            "port": port,
            "websocket_url": "",
        },
    )
    monkeypatch.setattr(host_bridge, "load_browser_bridge_state", lambda: {})

    saved = {}

    def _save(payload):
        saved.update(payload)
        return tmp_path / "state" / "state.json"

    monkeypatch.setattr(host_bridge, "save_browser_bridge_state", _save)

    result = host_bridge.host_browser_attach(port=9222, profile_directory="Default")

    assert result["ok"] is True
    assert result["degraded"] is True
    assert result["blocked_by_chrome_policy"] is True
    assert saved["mode"] == "real-chrome-apple-events"
    assert saved["control_ready"] is True


def test_host_browser_attach_surfaces_restart_hint(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "X_ATTACH_SCRIPT", Path("/tmp/fake-attach.sh"))
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")
    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_LOG", tmp_path / "logs" / "host-bridge.log")
    monkeypatch.setattr(host_bridge, "_is_chrome_running", lambda: True)
    monkeypatch.setattr(
        host_bridge,
        "_run_host_command",
        lambda *args, **kwargs: {
            "ok": False,
            "exit_code": 2,
            "stdout": "",
            "stderr": "no cdp",
        },
    )
    monkeypatch.setattr(
        host_bridge,
        "host_browser_status",
        lambda port=9222: {
            "ok": False,
            "ready": False,
            "cdp_ready": False,
            "control_ready": False,
            "port": port,
            "websocket_url": "",
        },
    )
    monkeypatch.setattr(host_bridge, "load_browser_bridge_state", lambda: {})
    monkeypatch.setattr(host_bridge, "save_browser_bridge_state", lambda payload: tmp_path / "state" / "state.json")

    result = host_bridge.host_browser_attach(port=9222, profile_directory="Default")

    assert result["ok"] is False
    assert result["needs_restart"] is True
    assert "force_restart_chrome=true" in result["next_step"]


def test_host_doctor_summarizes_checks(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "host_exec", lambda *args, **kwargs: {"ok": True, "stdout": "Darwin\n"})
    monkeypatch.setattr(host_bridge, "host_list_dir", lambda *args, **kwargs: {"ok": True, "count": 5})
    monkeypatch.setattr(host_bridge, "host_applescript", lambda *args, **kwargs: {"ok": True, "stdout": "Codex\n"})
    monkeypatch.setattr(host_bridge, "host_service_status", lambda *args, **kwargs: {"loaded": True, "ok": True})
    monkeypatch.setattr(host_bridge, "host_browser_status", lambda *args, **kwargs: {"ready": False, "ok": False})
    monkeypatch.setattr(host_bridge, "host_browser_tab_info", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(host_bridge, "host_browser_execute_javascript", lambda *args, **kwargs: {"ok": True})

    result = host_bridge.host_doctor()

    assert result["ok"] is True
    assert result["summary"]["host_exec_ok"] is True
    assert result["summary"]["browser_control_ok"] is True
    assert result["summary"]["browser_cdp_ready"] is False


def test_host_cursor_position_parses_cliclick_output(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "_resolve_cliclick_path", lambda: Path("/tmp/cliclick"))
    monkeypatch.setattr(
        host_bridge,
        "_run_host_args",
        lambda *args, **kwargs: {"ok": True, "stdout": "321,654\n", "stderr": "", "exit_code": 0},
    )

    result = host_bridge.host_cursor_position()

    assert result["ok"] is True
    assert result["x"] == 321
    assert result["y"] == 654


def test_host_click_uses_cliclick(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "_resolve_cliclick_path", lambda: Path("/tmp/cliclick"))
    captured = {}

    def _fake_run(args, **kwargs):
        captured["args"] = args
        return {"ok": True, "stdout": "", "stderr": "", "exit_code": 0}

    monkeypatch.setattr(host_bridge, "_run_host_args", _fake_run)

    result = host_bridge.host_click(10, 20)

    assert result["ok"] is True
    assert captured["args"][-1] == "c:10,20"


def test_host_hotkey_uses_modifier_chord(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "_resolve_cliclick_path", lambda: Path("/tmp/cliclick"))
    captured = {}

    def _fake_run(args, **kwargs):
        captured["args"] = args
        return {"ok": True, "stdout": "", "stderr": "", "exit_code": 0}

    monkeypatch.setattr(host_bridge, "_run_host_args", _fake_run)

    result = host_bridge.host_hotkey(["cmd", "shift"], "s")

    assert result["ok"] is True
    assert captured["args"][-3:] == ["kd:cmd,shift", "t:s", "ku:cmd,shift"]




def test_host_browser_window_screenshot_uses_sck_capture(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")

    def _fake_capture(app, target):
        Path(target).write_bytes(b"png")
        return {"ok": True, "stdout": "OK", "stderr": "", "exit_code": 0}

    monkeypatch.setattr(host_bridge, "_capture_browser_window_with_sck", _fake_capture)

    result = host_bridge.host_browser_window_screenshot(str(tmp_path / "shot.png"))

    assert result["ok"] is True
    assert result["bytes"] == 3
    assert result["path"].endswith("shot.png")




def test_host_browser_window_screenshot_surfaces_tcc_denial(monkeypatch, tmp_path):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "HOST_BRIDGE_DIR", tmp_path / "state")
    monkeypatch.setattr(
        host_bridge,
        "_capture_browser_window_with_sck",
        lambda app, target: {
            "ok": False,
            "blocked": True,
            "blocked_reason": "macOS ScreenCaptureKit permission denied for application/window/display capture",
        },
    )

    result = host_bridge.host_browser_window_screenshot(str(tmp_path / "shot.png"))

    assert result["ok"] is False
    assert result["blocked"] is True
    assert "ScreenCaptureKit permission denied" in result["blocked_reason"]

def test_host_ui_snapshot_includes_browser_window_screenshot(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(
        host_bridge,
        "host_applescript",
        lambda *args, **kwargs: {"ok": True, "stdout": "Google Chrome\n"},
    )
    monkeypatch.setattr(host_bridge, "host_cursor_position", lambda: {"ok": True, "x": 1, "y": 2})
    monkeypatch.setattr(host_bridge, "host_screenshot", lambda path=None: {"ok": True, "path": "/tmp/screen.png", "bytes": 123})
    monkeypatch.setattr(host_bridge, "host_browser_tab_info", lambda: {"ok": True, "title": "Example Domain"})
    monkeypatch.setattr(host_bridge, "host_browser_window_screenshot", lambda path=None, app="Google Chrome": {"ok": True, "path": "/tmp/browser.png", "bytes": 55})

    result = host_bridge.host_ui_snapshot()

    assert result["ok"] is True
    assert result["browser_window_screenshot"]["path"] == "/tmp/browser.png"

def test_host_ui_snapshot_includes_browser_tab_when_chrome_frontmost(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(
        host_bridge,
        "host_applescript",
        lambda *args, **kwargs: {"ok": True, "stdout": "Google Chrome\n"},
    )
    monkeypatch.setattr(host_bridge, "host_cursor_position", lambda: {"ok": True, "x": 1, "y": 2})
    monkeypatch.setattr(host_bridge, "host_screenshot", lambda path=None: {"ok": True, "path": "/tmp/screen.png", "bytes": 123})
    monkeypatch.setattr(host_bridge, "host_browser_tab_info", lambda: {"ok": True, "title": "Example Domain"})

    result = host_bridge.host_ui_snapshot()

    assert result["ok"] is True
    assert result["frontmost_app"] == "Google Chrome"
    assert result["browser_tab"]["title"] == "Example Domain"


def test_host_gui_doctor_summarizes_gui_checks(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "host_applescript", lambda *args, **kwargs: {"ok": True, "stdout": ""})
    monkeypatch.setattr(host_bridge, "host_cursor_position", lambda: {"ok": True, "x": 100, "y": 200, "permission_warning": False})
    monkeypatch.setattr(host_bridge, "host_move_mouse", lambda *args, **kwargs: {"ok": True, "permission_warning": False})
    monkeypatch.setattr(host_bridge, "host_click", lambda *args, **kwargs: {"ok": True, "permission_warning": False})
    monkeypatch.setattr(host_bridge, "host_type", lambda *args, **kwargs: {"ok": True, "permission_warning": False})
    monkeypatch.setattr(host_bridge, "host_press_key", lambda *args, **kwargs: {"ok": True, "permission_warning": False})
    monkeypatch.setattr(host_bridge, "host_hotkey", lambda *args, **kwargs: {"ok": True, "permission_warning": False})
    monkeypatch.setattr(host_bridge, "host_ui_snapshot", lambda *args, **kwargs: {"ok": True})

    result = host_bridge.host_gui_doctor()

    assert result["ok"] is True
    assert result["summary"]["accessibility_granted"] is True
    assert result["summary"]["click_ok"] is True
    assert result["summary"]["type_ok"] is True



def test_host_type_preserves_blank_lines_without_empty_cliclick_commands(monkeypatch):
    import tools.host_bridge as host_bridge

    monkeypatch.setattr(host_bridge, "_resolve_cliclick_path", lambda: Path("/tmp/cliclick"))
    captured = {}

    def _fake_run(args, **kwargs):
        captured["args"] = args
        return {"ok": True, "stdout": "", "stderr": "", "exit_code": 0}

    monkeypatch.setattr(host_bridge, "_run_host_args", _fake_run)

    result = host_bridge.host_type("line1\n\nline3")

    assert result["ok"] is True
    assert captured["args"][-4:] == ["t:line1", "kp:return", "kp:return", "t:line3"]


def test_host_paste_restores_clipboard(monkeypatch):
    import tools.host_bridge as host_bridge

    clipboard = {"value": "before"}
    monkeypatch.setattr(host_bridge, "_clipboard_read", lambda: clipboard["value"])
    monkeypatch.setattr(host_bridge, "_clipboard_write", lambda text: clipboard.__setitem__("value", text))
    monkeypatch.setattr(host_bridge, "host_hotkey", lambda *args, **kwargs: {"ok": True})

    result = host_bridge.host_paste("hello world", preserve_clipboard=True)

    assert result["ok"] is True
    assert clipboard["value"] == "before"


def test_host_browser_x_reply_uses_exact_paste_and_submit(monkeypatch):
    import tools.host_bridge as host_bridge

    states = [
        {
            "ok": True,
            "value": {
                "action": "ready",
                "active": {
                    "composer": {"center": {"x": 100, "y": 200}},
                    "button": {"center": {"x": 300, "y": 400}, "disabled": "false"},
                    "current_text": "",
                },
            },
        },
        {
            "ok": True,
            "value": {
                "action": "ready",
                "active": {
                    "composer": {"center": {"x": 100, "y": 200}},
                    "button": {"center": {"x": 300, "y": 400}, "disabled": "false"},
                    "current_text": "exact reply",
                },
            },
        },
    ]
    monkeypatch.setattr(host_bridge, "host_open_url", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(host_bridge, "_host_browser_eval_json", lambda *args, **kwargs: states.pop(0))
    monkeypatch.setattr(host_bridge, "host_click", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(host_bridge, "host_hotkey", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(host_bridge, "host_press_key", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(host_bridge, "host_paste", lambda *args, **kwargs: {"ok": True})

    result = host_bridge.host_browser_x_reply("https://x.com/example/status/123", "exact reply", submit=True)

    assert result["ok"] is True
    assert result["submitted"] is True
