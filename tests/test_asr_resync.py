#!/usr/bin/env python3
"""Short healthCheckTimeout resync against an already running InferSwap.

The process must already have been started with a short healthCheckTimeout.
This script does not restart InferSwap or a runtime, and it does not call
control load or unload. The first ASR request is expected to be HTTP 502
LOAD_FAILED while reserved_bytes stays at the configured peak. After the
runtime reports ready and resident, the retry must be HTTP 200 without a
new POST /control/load in the ASR access log.

Writes tests/outputs/asr_resync.json.
ASR_LOG is the ASR access log. FA_LOG is optional.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import live


INFER_BASE_URL = live.read_env("INFER_BASE_URL", "http://127.0.0.1:8095").rstrip("/")
ASR_BASE = live.read_env("ASR_BASE", "http://127.0.0.1:8080").rstrip("/")
FA_BASE = live.read_env("FA_BASE", "http://127.0.0.1:8090").rstrip("/")
READY_WAIT = int(live.read_env("READY_WAIT", "180"))
REQUEST_TIMEOUT_SECONDS = int(live.read_env("STT_REQUEST_TIMEOUT_SECONDS", "7200"))
REPORT_PATH = live.output_dir() / "asr_resync.json"


def log_path(name: str) -> Path | None:
    raw = live.read_env(name, "")
    if not raw:
        return None
    return Path(raw)


def body_has(raw: bytes, token: str) -> bool:
    return token in raw.decode("utf-8", errors="replace")


def parsed_body(raw: bytes) -> Any:
    text = raw.decode("utf-8", errors="replace")
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def build_silence(path: Path) -> None:
    live.ensure_command("ffmpeg")
    proc = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=16000:cl=mono",
            "-t",
            "1",
            "-c:a",
            "pcm_s16le",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"could not build a short wav: {detail}")


def post_asr(body: bytes) -> tuple[int, bytes, float]:
    try:
        return live.post_body(
            f"{INFER_BASE_URL}/infer?model=qwen-asr",
            body,
            "multipart/form-data; boundary=InferSwapResync",
            REQUEST_TIMEOUT_SECONDS,
        )
    except RuntimeError as exc:
        raise RuntimeError(f"ASR request failed to connect: {exc}") from exc


def main() -> int:
    live.load_dotenv()
    report: dict[str, Any] = {
        "ok": False,
        "at": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "infer_base_url": INFER_BASE_URL,
        "container_id": live.read_env("CONTAINER_ID", "") or None,
        "failure": None,
    }
    asr_log = log_path("ASR_LOG")
    fa_log = log_path("FA_LOG")
    try:
        cfg = live.load_config()
        asr_account = live.model_account(cfg, "qwen-asr")
        configured_timeout = cfg.get("healthCheckTimeout", "unknown")
        report["health_check_timeout"] = configured_timeout
        report["health_check_timeout_note"] = (
            "confirm this matches the already running process; this script does not restart it"
        )
        report["expected_peak"] = asr_account["peak_bytes"]

        health_status, _health, _text = live.get_json(f"{INFER_BASE_URL}/health")
        if health_status != 200:
            raise RuntimeError(f"InferSwap /health HTTP {health_status}")
        asr_before_status, asr_before = live.try_get_json(f"{ASR_BASE}/control/status")
        fa_before_status, fa_before = live.try_get_json(f"{FA_BASE}/control/status")
        report["control_before"] = {
            "asr_http": asr_before_status,
            "fa_http": fa_before_status,
            "asr": asr_before,
            "fa": fa_before,
        }
        if asr_before_status == 0 and fa_before_status == 0:
            raise RuntimeError("both control endpoints refused the connection")
        if asr_before_status != 200:
            raise RuntimeError(f"ASR control status HTTP {asr_before_status}")

        with tempfile.TemporaryDirectory(prefix="asr-resync-") as tmp_name:
            wav_path = Path(tmp_name) / "short.wav"
            build_silence(wav_path)
            wav_bytes = wav_path.read_bytes()
            wav_info = live.inspect_pcm_wav(wav_path)
            report["wav"] = {"bytes": len(wav_bytes), **wav_info}
        asr_body = live.build_multipart(
            "InferSwapResync",
            [
                ("file", "file", "short.wav", "audio/wav", wav_bytes),
                ("field", "response_format", "verbose_json"),
                ("field", "include_chunks", "true"),
            ],
        )

        before = live.count_control_loads(asr_log)
        first_started = time.time()
        first_http, first_raw, _elapsed = post_asr(asr_body)
        first_ended = time.time()
        models_status, models_after_first, _models_text = live.get_json(f"{INFER_BASE_URL}/v1/models")
        if models_status != 200:
            raise RuntimeError(f"models after the first request HTTP {models_status}")
        asr_after_status, asr_after_first = live.try_get_json(f"{ASR_BASE}/control/status")
        local = live.model_status(models_after_first, "qwen-asr")
        remote_state = asr_after_first.get("state") if isinstance(asr_after_first, dict) else None
        remote_residency = asr_after_first.get("residency") if isinstance(asr_after_first, dict) else None
        report["first"] = {
            "started": first_started,
            "ended": first_ended,
            "http": first_http,
            "local_state": local.get("state"),
            "reserved_bytes": local.get("reserved_bytes"),
            "remote_state": remote_state,
            "remote_residency": remote_residency,
            "remote_http": asr_after_status,
            "body": parsed_body(first_raw),
            "load_serial": "not exposed by the public API; unit tests cover the clear",
        }
        if first_http != 502:
            raise RuntimeError(f"expected the original ASR request to be HTTP 502 LOAD_FAILED, got {first_http}")
        if not body_has(first_raw, "LOAD_FAILED"):
            raise RuntimeError("502 body is not LOAD_FAILED")
        if local.get("state") != "failed" or local.get("reserved_bytes") != asr_account["peak_bytes"]:
            raise RuntimeError(
                f"after 502 state={local.get('state')} reserved={local.get('reserved_bytes')} "
                f"peak={asr_account['peak_bytes']}"
            )

        pending_http: int | str = "skipped"
        pending_body: Any = None
        if remote_state == "loading":
            pending_http, pending_raw, _pending_elapsed = post_asr(asr_body)
            pending_body = parsed_body(pending_raw)
            if pending_http != 503:
                raise RuntimeError(f"loading retry HTTP {pending_http}")
            if not body_has(pending_raw, "LIFECYCLE_PENDING"):
                raise RuntimeError("503 body is not LIFECYCLE_PENDING")
        else:
            report["pending_unverified"] = "remote was not loading after the 502; LIFECYCLE_PENDING was not observed"
        report["pending"] = {"http": pending_http, "body": pending_body}

        deadline = time.monotonic() + READY_WAIT
        ready = False
        last_remote: dict[str, Any] = {}
        while time.monotonic() < deadline:
            _status, last_remote = live.try_get_json(f"{ASR_BASE}/control/status")
            if not isinstance(last_remote, dict):
                last_remote = {}
            if last_remote.get("state") == "ready" and last_remote.get("residency") == "resident":
                ready = True
                break
            if last_remote.get("state") == "failed" and last_remote.get("residency") == "resident":
                raise RuntimeError(
                    "ASR remained failed and resident; continued rejection is occupancy protection, not recovery success"
                )
            time.sleep(2)
        report["wait"] = {"ready": ready, "remote": last_remote, "ready_wait_sec": READY_WAIT}
        if not ready:
            raise RuntimeError(f"ASR did not become ready+resident within {READY_WAIT}s")

        mid = live.count_control_loads(asr_log)
        second_started = time.time()
        second_http, second_raw, _second_elapsed = post_asr(asr_body)
        second_ended = time.time()
        after = live.count_control_loads(asr_log)
        _asr_final_status, asr_final = live.try_get_json(f"{ASR_BASE}/control/status")
        models_second_status, models_second, _models_second_text = live.get_json(f"{INFER_BASE_URL}/v1/models")
        report["second"] = {
            "http": second_http,
            "started": second_started,
            "ended": second_ended,
            "body": parsed_body(second_raw),
        }
        report["loads"] = {
            "before": before,
            "before_ready_request": mid,
            "after": after,
            "asr_log": str(asr_log) if asr_log is not None else None,
        }
        report["control_after"] = {
            "asr": asr_final,
            "models": models_second if models_second_status == 200 else {"http": models_second_status},
        }
        if second_http != 200:
            raise RuntimeError(f"ready retry HTTP {second_http}")
        if before < 0 or after < 0:
            raise RuntimeError("ASR access log missing; POST /control/load count is unverified")
        if after != mid:
            raise RuntimeError(f"ready request added POST /control/load entries ({mid} -> {after})")

        fa_before = live.count_control_loads(fa_log)
        fa_started = time.time()
        fa_request = live.build_multipart(
            "InferSwapResyncFA",
            [
                ("file", "file", "short.wav", "audio/wav", wav_bytes),
                ("field", "payload", second_raw.decode("utf-8", errors="replace")),
            ],
        )
        fa_http = 0
        fa_raw = b""
        try:
            fa_http, fa_raw, _fa_elapsed = live.post_body(
                f"{INFER_BASE_URL}/infer?model=qwen-fa",
                fa_request,
                "multipart/form-data; boundary=InferSwapResyncFA",
                REQUEST_TIMEOUT_SECONDS,
            )
        except RuntimeError:
            fa_http = 0
        fa_ended = time.time()
        fa_after = live.count_control_loads(fa_log)
        if fa_http == 200:
            fa_note = "fa_completed_within_the_running_process_timeout=yes"
        elif fa_http == 502:
            fa_note = (
                "InferSwap returned 502; record that it started FA /control/load when the access log "
                "shows a new POST. This is not an ASR resync failure."
            )
        else:
            fa_note = "recorded only; ASR resync success does not depend on this response"
        _fa_status, fa_remote = live.try_get_json(f"{FA_BASE}/control/status")
        report["fa"] = {
            "http": fa_http,
            "started": fa_started,
            "ended": fa_ended,
            "loads_before": fa_before,
            "loads_after": fa_after,
            "note": fa_note,
            "body": parsed_body(fa_raw),
            "remote": fa_remote,
        }
        report["ok"] = True
        print("[SUCCESS] ASR resync completed.")
        return 0
    except Exception as exc:  # noqa: BLE001
        report["failure"] = f"{type(exc).__name__}: {exc}"
        print(f"[FAIL] {report['failure']}")
        return 1
    finally:
        live.write_report(REPORT_PATH, report)


if __name__ == "__main__":
    sys.exit(main())
