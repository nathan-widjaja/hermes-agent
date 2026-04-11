"""Outbound artifact verification for messaging delivery.

Prevents obviously wrong screenshot/proof artifacts from being sent as if they
were trustworthy evidence. The goal is to block mismatches before the user has
to catch them.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import re
import subprocess
import tempfile
from typing import Optional

_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
_PROOF_RE = re.compile(
    r"\b(screenshot|proof|submission|submitted|receipt|page|tab|form|browser|what you send|what you sent)\b",
    re.IGNORECASE,
)
_GENERATED_NAME_RE = re.compile(
    r"(rendered|artifact|summary|mock|placeholder|synthetic)", re.IGNORECASE
)
_STOPWORDS = {
    "about", "account", "after", "appeal", "browser", "click", "content", "contact",
    "filled", "form", "from", "help", "image", "locked", "page", "please", "proof",
    "review", "send", "sent", "screenshot", "show", "submission", "submit", "support",
    "suspended", "that", "this", "what", "with", "would", "your",
}


@dataclass
class VerificationResult:
    allowed: bool
    reason: str
    mode: str = "skip"


def _looks_like_proof_request(user_text: str | None, response_text: str | None = None) -> bool:
    haystack = f"{user_text or ''}\n{response_text or ''}"
    return bool(_PROOF_RE.search(haystack))


def _is_local_image(path: str) -> bool:
    return Path(path).suffix.lower() in _IMAGE_EXTS


def _looks_generated(path: str) -> bool:
    return bool(_GENERATED_NAME_RE.search(Path(path).name))


def _extract_text_macos_vision(image_path: str) -> str:
    """Extract OCR text with macOS Vision; empty string on failure."""
    swift = f'''
import Foundation
import Vision
import AppKit
let path = {json.dumps(image_path)}
let url = URL(fileURLWithPath: path)
guard let image = NSImage(contentsOf: url) else {{ print(""); exit(0) }}
var rect = NSRect(origin: .zero, size: image.size)
guard let cgImage = image.cgImage(forProposedRect: &rect, context: nil, hints: nil) else {{ print(""); exit(0) }}
let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.usesLanguageCorrection = true
let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
do {{
  try handler.perform([request])
  let observations = request.results ?? []
  for obs in observations {{
    if let top = obs.topCandidates(1).first {{
      print(top.string)
    }}
  }}
}} catch {{
  print("")
}}
'''
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".swift", delete=False) as f:
            f.write(swift)
            tmp = f.name
        proc = subprocess.run(
            ["/usr/bin/swift", tmp],
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
        )
        return (proc.stdout or "").strip()
    except Exception:
        return ""


def _get_active_chrome_context() -> tuple[str, str]:
    """Return (title, url) for active Chrome tab; empty strings on failure."""
    script = '''
set _title to ""
set _url to ""
try
  tell application "Google Chrome"
    set _title to title of active tab of front window
    set _url to URL of active tab of front window
  end tell
end try
return _title & linefeed & _url
'''
    try:
        proc = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
        out = (proc.stdout or "").splitlines()
        title = out[0].strip() if out else ""
        url = out[1].strip() if len(out) > 1 else ""
        return title, url
    except Exception:
        return "", ""


def _expected_tokens(user_text: str | None) -> set[str]:
    title, url = _get_active_chrome_context()
    source = " ".join(filter(None, [user_text or "", title, url]))
    tokens = set(re.findall(r"[a-z0-9][a-z0-9._-]{2,}", source.lower()))
    cleaned = {
        t.strip("._-")
        for t in tokens
        if t.strip("._-") and t.strip("._-") not in _STOPWORDS
    }
    return {t for t in cleaned if len(t) >= 3}


def verify_local_image(path: str, user_text: str | None, response_text: str | None = None) -> VerificationResult:
    """Best-effort proof/screenshot verification.

    Blocks obviously generated artifacts for proof-like requests, and on macOS
    attempts OCR/title matching against the live browser context.
    """
    if not _is_local_image(path):
        return VerificationResult(True, "non-image", mode="skip")
    if not _looks_like_proof_request(user_text, response_text):
        return VerificationResult(True, "no-proof-claim", mode="skip")
    if _looks_generated(path):
        return VerificationResult(False, "generated-artifact-blocked", mode="filename")

    ocr_text = _extract_text_macos_vision(path)
    if not ocr_text:
        return VerificationResult(True, "ocr-unavailable", mode="best-effort")

    expected = _expected_tokens(user_text)
    if not expected:
        return VerificationResult(True, "no-expected-tokens", mode="best-effort")

    haystack = ocr_text.lower()
    hits = [token for token in expected if token in haystack]
    if hits:
        return VerificationResult(True, f"matched:{','.join(sorted(hits)[:5])}", mode="ocr")
    return VerificationResult(False, "ocr-mismatch-blocked", mode="ocr")
