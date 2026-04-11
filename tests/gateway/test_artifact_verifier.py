from gateway.artifact_verifier import verify_local_image


def test_generated_artifact_blocked_for_proof_request(tmp_path):
    img = tmp_path / "x_page_rendered.png"
    img.write_bytes(b"fake")

    result = verify_local_image(str(img), "send proof screenshot of submission")

    assert result.allowed is False
    assert result.reason == "generated-artifact-blocked"


def test_non_proof_request_skips_verification(tmp_path):
    img = tmp_path / "normal.png"
    img.write_bytes(b"fake")

    result = verify_local_image(str(img), "send this image")

    assert result.allowed is True
    assert result.reason == "no-proof-claim"


def test_ocr_mismatch_blocked_for_proof_request(tmp_path, monkeypatch):
    img = tmp_path / "actual.png"
    img.write_bytes(b"fake")

    monkeypatch.setattr("gateway.artifact_verifier._extract_text_macos_vision", lambda _p: "Desktop Downloads Documents")
    monkeypatch.setattr("gateway.artifact_verifier._expected_tokens", lambda _t: {"appeal", "suspended", "help.x.com"})

    result = verify_local_image(str(img), "get screenshot of the X appeal page proof of submission")

    assert result.allowed is False
    assert result.reason == "ocr-mismatch-blocked"


def test_ocr_match_allowed_for_proof_request(tmp_path, monkeypatch):
    img = tmp_path / "actual.png"
    img.write_bytes(b"fake")

    monkeypatch.setattr(
        "gateway.artifact_verifier._extract_text_macos_vision",
        lambda _p: "Appeal a locked or suspended account help.x.com Account Access",
    )
    monkeypatch.setattr("gateway.artifact_verifier._expected_tokens", lambda _t: {"appeal", "suspended", "help.x.com"})

    result = verify_local_image(str(img), "get screenshot of the X appeal page proof of submission")

    assert result.allowed is True
    assert result.mode == "ocr"
