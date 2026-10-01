#!/usr/bin/env python3
"""VLM, then ASR, then FA, then LLM, through a running InferSwap.

Attaches to InferSwap and the three runtimes. Does not start them and does
not call control load or unload. VLM and LLM share the InferSwap id qwen-vlm.
The JSON model field stays the llama-server name qwen3.8-27b. ASR and
FA use one YouTube WAV. Host memory samples in the report are observations,
not a resourceProfile.

Writes tests/outputs/vlm_asr_fa_llm-<timestamp>.json and does not overwrite it.
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import live
import test_asr_fa_swap as asr_fa


YOUTUBE_URL = asr_fa.YOUTUBE_URL
INFER_BASE_URL = live.read_env("INFER_BASE_URL", "http://127.0.0.1:8095").rstrip("/")
LLAMA_BASE = live.read_env("LLAMA_BASE", "http://127.0.0.1:8000").rstrip("/")
ASR_BASE = live.read_env("ASR_BASE", "http://127.0.0.1:8080").rstrip("/")
FA_BASE = live.read_env("FA_BASE", "http://127.0.0.1:8090").rstrip("/")
VLM_ID = "qwen-vlm"
LLAMA_MODEL = "qwen3.8-27b"
ASR_ID = "qwen-asr"
FA_ID = "qwen-fa"
REQUEST_TIMEOUT_SECONDS = int(live.read_env("STT_REQUEST_TIMEOUT_SECONDS", "7200"))
IMAGE_FILE = Path(
    live.read_env(
        "IMAGE_FILE",
        "/home/kjh/workspace/llama-cpp-docker-config/local/test-cat.jpg",
    )
)
LLM_PROMPT = "Reply with the single word: pong"
VLM_PROMPT = "Describe this image in one sentence. Include the kind of animal and its color."

ENDPOINTS = {
    VLM_ID: LLAMA_BASE,
    ASR_ID: ASR_BASE,
    FA_ID: FA_BASE,
}


def report_path(stamp: str) -> Path:
    return live.output_dir() / f"vlm_asr_fa_llm-{stamp}.json"


def chat_content(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return ""
    message = choices[0].get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    return content if isinstance(content, str) else ""


def other_hold(state: str, reserved: Any, account: dict[str, Any]) -> int:
    if reserved not in (0, None):
        return live.as_int(reserved, "reserved_bytes")
    if state in ("ready", "starting", "stopping"):
        return account["peak_bytes"]
    if state == "stopped":
        return account["unloaded_residual_bytes"]
    return -1


def rest_hold(state: str, reserved: Any, account: dict[str, Any]) -> int:
    if state in ("unknown", "failed", "shutdown"):
        return -1
    if (
        state == "stopped"
        and reserved == account["peak_bytes"]
        and account["peak_bytes"] > account["unloaded_residual_bytes"]
    ):
        return account["peak_bytes"]
    return account["unloaded_residual_bytes"]


def admission(
    target: str,
    accounts: dict[str, dict[str, Any]],
    locals_by_id: dict[str, dict[str, Any]],
    remotes: dict[str, Any],
    margin: int,
    free: int,
    total: int,
) -> dict[str, Any]:
    holds: dict[str, int] = {}
    rests: dict[str, int] = {}
    victims: list[str] = []
    for model_id, account in accounts.items():
        if model_id == target:
            continue
        local = locals_by_id[model_id]
        state = str(local.get("state"))
        reserved = local.get("reserved_bytes")
        holds[model_id] = other_hold(state, reserved, account)
        rests[model_id] = rest_hold(state, reserved, account)
        remote = remotes.get(model_id)
        active = remote.get("active_requests") if isinstance(remote, dict) else None
        if state == "ready" and active == 0:
            victims.append(model_id)
    other_sum = sum(holds.values()) if holds and all(value >= 0 for value in holds.values()) else -1
    rest_sum = sum(rests.values()) if rests and all(value >= 0 for value in rests.values()) else -1
    peak = accounts[target]["peak_bytes"]
    return {
        "target": target,
        "target_peak": peak,
        "other_holds": holds,
        "rest_holds": rests,
        "safety_margin": margin,
        "free": free,
        "total": total,
        "fits": other_sum >= 0 and free - margin >= peak + other_sum,
        "fits_if_rest_residual": rest_sum >= 0 and total - margin >= peak + rest_sum,
        "idle_ready_victim": bool(victims),
        "idle_ready_victims": victims,
    }


def read_models() -> dict[str, Any]:
    status, body, _text = live.get_json(f"{INFER_BASE_URL}/v1/models")
    if status != 200 or not isinstance(body, dict):
        raise RuntimeError(f"GET /v1/models HTTP {status}")
    gpu = body.get("gpu")
    if not isinstance(gpu, dict) or gpu.get("fresh") is not True:
        raise RuntimeError(f"gpu sample is not fresh: {gpu}")
    return body


def control_snapshot() -> dict[str, Any]:
    out: dict[str, Any] = {}
    refused = 0
    for model_id, base in ENDPOINTS.items():
        status, body = live.try_get_json(f"{base}/control/status")
        if status == 0:
            refused += 1
        out[model_id] = {"http": status, "body": body if isinstance(body, dict) else {}}
    if refused == len(ENDPOINTS):
        raise RuntimeError("all control endpoints refused the connection")
    return out


def residency_of(remote: dict[str, Any]) -> Any:
    body = remote.get("body")
    if remote.get("http") != 200 or not isinstance(body, dict):
        return None
    return body.get("residency")


def judge(step: str, http_status: str | int, raw_text: str, budget: dict[str, Any], after_control: dict[str, Any]) -> None:
    if live.contains_oom(raw_text):
        raise RuntimeError(f"{step} profile peak exceeded")
    target = budget["target"]
    if budget["fits"]:
        if http_status != 200:
            raise RuntimeError(f"{step} fits=true but HTTP {http_status}: {raw_text[:1000]}")
        return
    if budget["fits_if_rest_residual"] and budget["idle_ready_victim"]:
        if http_status != 200:
            raise RuntimeError(f"{step} eviction headroom but HTTP {http_status}: {raw_text[:1000]}")
        target_res = residency_of(after_control[target])
        if target_res != "resident":
            raise RuntimeError(f"{step} target residency={target_res}")
        for model_id in budget["idle_ready_victims"]:
            other_res = residency_of(after_control[model_id])
            if other_res is None:
                raise RuntimeError(f"{step} {model_id} residency is null and is not not_resident")
        if other_res != "not_resident":
            raise RuntimeError(f"{step} victim {model_id} residency={other_res}")
        return
    if budget["fits_if_rest_residual"] and not budget["idle_ready_victim"]:
        if http_status != 200:
            raise RuntimeError(f"{step} idle GPU headroom but HTTP {http_status}: {raw_text[:1000]}")
        target_res = residency_of(after_control[target])
        if target_res != "resident":
            raise RuntimeError(f"{step} target residency={target_res}")
        return
    if http_status != 503:
        raise RuntimeError(f"{step} expected HTTP 503 when the budget cannot admit, got {http_status}")
    raise RuntimeError(
        f"{step} admission could not complete: "
        f"fits={budget['fits']} rest={budget['fits_if_rest_residual']} "
        f"victim={budget['idle_ready_victim']} http={http_status}"
    )


def vlm_body(image_bytes: bytes) -> dict[str, Any]:
    import base64

    encoded = base64.b64encode(image_bytes).decode("ascii")
    return {
        "model": LLAMA_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": VLM_PROMPT},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{encoded}"}},
                ],
            }
        ],
        "max_tokens": 256,
        "temperature": 0,
        "chat_template_kwargs": {"enable_thinking": False},
    }


def llm_body() -> dict[str, Any]:
    return {
        "model": LLAMA_MODEL,
        "messages": [{"role": "user", "content": LLM_PROMPT}],
        "max_tokens": 16,
        "temperature": 0,
        "chat_template_kwargs": {"enable_thinking": False},
    }


def post_json(model_id: str, payload: dict[str, Any]) -> tuple[int, bytes, float, bytes]:
    raw = json.dumps(payload).encode("utf-8")
    status, body, elapsed = live.post_body(
        f"{INFER_BASE_URL}/infer?model={model_id}",
        raw,
        "application/json",
        REQUEST_TIMEOUT_SECONDS,
    )
    return status, body, elapsed, raw


def main() -> int:
    live.load_dotenv()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = report_path(stamp)
    if path.exists():
        raise SystemExit(f"refusing to overwrite {path}")
    report: dict[str, Any] = {
        "ok": False,
        "at": stamp,
        "youtube_url": YOUTUBE_URL,
        "infer_base_url": INFER_BASE_URL,
        "profile_note": "nvidia samples in this file are not a resourceProfile",
        "failure": None,
        "steps": [],
    }
    try:
        if not IMAGE_FILE.is_file():
            raise RuntimeError(f"image file is missing: {IMAGE_FILE}")
        image_bytes = IMAGE_FILE.read_bytes()
        cfg = live.load_config()
        gpu = live.gpu_account(cfg)
        accounts = {
            VLM_ID: live.model_account(cfg, VLM_ID),
            ASR_ID: live.model_account(cfg, ASR_ID),
            FA_ID: live.model_account(cfg, FA_ID),
        }
        host = live.nvidia_gpus()[0]
        report["environment"] = {
            "commit": live.git_commit(),
            "config": str(live.config_path()),
            "gpu_uuid": host["uuid"],
            "config_device": gpu["device"],
            "profiles": {model_id: account["profile_id"] for model_id, account in accounts.items()},
            "measured_on": {model_id: account["measured_on"] for model_id, account in accounts.items()},
            "image_file": str(IMAGE_FILE),
            "image_bytes": len(image_bytes),
            "nvidia": host,
            "speech_note": "operator must confirm the utterance; this script does not detect language",
            "peak_note": "this run does not replace configured peaks",
        }
        if host["uuid"] != gpu["device"] or any(account["measured_on"] != host["uuid"] for account in accounts.values()):
            raise RuntimeError(f"GPU UUID does not match gpu.device and profile measuredOn values (saw {host['uuid']})")

        health_status, _health, _text = live.get_json(f"{INFER_BASE_URL}/health")
        if health_status != 200:
            raise RuntimeError(f"InferSwap /health HTTP {health_status}")
        before_controls = control_snapshot()
        for model_id, remote in before_controls.items():
            if remote["http"] != 200:
                raise RuntimeError(f"{model_id} control status precondition HTTP {remote['http']}")

        def run_step(name: str, model_id: str, sender) -> tuple[int, bytes]:
            models_before = read_models()
            controls_before = control_snapshot()
            nvidia_before = live.nvidia_gpus()[0]
            sample = live.gpu_sample(models_before)
            locals_by_id = {mid: live.model_status(models_before, mid) for mid in accounts}
            remotes = {mid: controls_before[mid]["body"] for mid in accounts}
            budget = admission(
                model_id,
                accounts,
                locals_by_id,
                remotes,
                gpu["safety_margin_bytes"],
                sample["free"],
                sample["total"],
            )
            status, raw, elapsed, request_bytes = sender()
            text = raw.decode("utf-8", errors="replace")
            step = {
                "name": name,
                "model": model_id,
                "http": status,
                "elapsed_sec": elapsed,
                "request_bytes": len(request_bytes),
                "budget": budget,
                "nvidia_before": nvidia_before,
                "models_before": models_before,
                "control_before": controls_before,
            }
            report["steps"].append(step)
            models_after = read_models()
            controls_after = control_snapshot()
            step["nvidia_after"] = live.nvidia_gpus()[0]
            step["models_after"] = models_after
            step["control_after"] = controls_after
            judge(name, status, text, budget, controls_after)
            return status, raw

        vlm_payload = vlm_body(image_bytes)
        if len(json.dumps(vlm_payload).encode("utf-8")) > accounts[VLM_ID]["max_body_bytes"]:
            raise RuntimeError("VLM body exceeds qwen-vlm maxBodyBytes")
        _vlm_status, vlm_raw = run_step("vlm", VLM_ID, lambda: post_json(VLM_ID, vlm_payload))
        vlm_json = live.decode_json_body(vlm_raw, "VLM")
        if not chat_content(vlm_json).strip():
            raise RuntimeError("VLM content is empty")
        report["vlm_content"] = chat_content(vlm_json)

        with tempfile.TemporaryDirectory(prefix="vlm-asr-fa-llm-") as tmp_name:
            tmp = Path(tmp_name)
            video_id, source = asr_fa.download_audio(YOUTUBE_URL, tmp)
            wav_path = tmp / "canonical.wav"
            wav = asr_fa.normalize_pcm_wav(source, wav_path)
            duration = wav["num_samples"] / wav["sample_rate"]
            report["source"] = {
                "video_id": video_id,
                "wav_bytes": wav_path.stat().st_size,
                "duration_sec": duration,
                **wav,
            }
            if not 0 < duration <= asr_fa.MAX_DURATION_SECONDS:
                raise RuntimeError(
                    f"duration {duration} is outside 0 < duration <= {asr_fa.MAX_DURATION_SECONDS}; not trimmed"
                )
            wav_bytes = wav_path.read_bytes()

        asr_request = live.build_multipart(
            "InferSwapASR",
            [
                ("file", "file", "canonical.wav", "audio/wav", wav_bytes),
                ("field", "response_format", "verbose_json"),
                ("field", "include_chunks", "true"),
            ],
        )
        if len(asr_request) > accounts[ASR_ID]["max_body_bytes"]:
            raise RuntimeError(f"ASR multipart bytes {len(asr_request)} exceed maxBodyBytes")

        def send_asr() -> tuple[int, bytes, float, bytes]:
            status, raw, elapsed = live.post_body(
                f"{INFER_BASE_URL}/infer?model={ASR_ID}",
                asr_request,
                "multipart/form-data; boundary=InferSwapASR",
                REQUEST_TIMEOUT_SECONDS,
            )
            return status, raw, elapsed, asr_request

        _asr_status, asr_raw = run_step("asr", ASR_ID, send_asr)
        asr_json = live.decode_json_body(asr_raw, "ASR")
        if not isinstance(asr_json, dict):
            raise RuntimeError("ASR JSON is not an object")
        asr_fa.require_asr_chunks(asr_json, wav["num_samples"])
        report["asr_response"] = asr_json

        fa_request = live.build_multipart(
            "InferSwapFA",
            [
                ("file", "file", "canonical.wav", "audio/wav", wav_bytes),
                ("field", "payload", asr_raw.decode("utf-8")),
            ],
        )
        if len(fa_request) > accounts[FA_ID]["max_body_bytes"]:
            raise RuntimeError(f"FA multipart bytes {len(fa_request)} exceed maxBodyBytes")

        def send_fa() -> tuple[int, bytes, float, bytes]:
            status, raw, elapsed = live.post_body(
                f"{INFER_BASE_URL}/infer?model={FA_ID}",
                fa_request,
                "multipart/form-data; boundary=InferSwapFA",
                REQUEST_TIMEOUT_SECONDS,
            )
            return status, raw, elapsed, fa_request

        _fa_status, fa_raw = run_step("fa", FA_ID, send_fa)
        fa_json = live.decode_json_body(fa_raw, "FA")
        if not isinstance(fa_json, dict):
            raise RuntimeError("FA JSON is not an object")
        asr_fa.assert_alignment(asr_json, fa_json, duration)
        report["alignment"] = fa_json

        llm_payload = llm_body()
        _llm_status, llm_raw = run_step("llm", VLM_ID, lambda: post_json(VLM_ID, llm_payload))
        llm_json = live.decode_json_body(llm_raw, "LLM")
        if not chat_content(llm_json).strip():
            raise RuntimeError("LLM content is empty")
        report["llm_content"] = chat_content(llm_json)
        report["ok"] = True
        print("[SUCCESS] VLM, ASR, FA, LLM swap completed.")
        return 0
    except Exception as exc:  # noqa: BLE001
        report["failure"] = f"{type(exc).__name__}: {exc}"
        print(f"[FAIL] {report['failure']}")
        return 1
    finally:
        live.write_report(path, report)


if __name__ == "__main__":
    sys.exit(main())
