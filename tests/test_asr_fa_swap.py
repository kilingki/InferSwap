#!/usr/bin/env python3
"""ASR then FA through a running InferSwap, with the admission record.

Attaches to InferSwap and the two runtimes. Does not start them and does not
call control load or unload. The YouTube step only builds a 16 kHz mono
PCM16 WAV. Duration must be within 1800 seconds, the same bound as the
previous integration script. The default URL is the clip already run under
that bound.

Writes tests/outputs/asr_fa_swap.json.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import live


# 217s clip. tests/test_asr_then_fa.py uses a longer URL that this bound rejects.
YOUTUBE_URL = live.read_env(
    "YOUTUBE_URL",
    "https://www.youtube.com/watch?v=cqklrHin_XA",
)
INFER_BASE_URL = live.read_env("INFER_BASE_URL", "http://127.0.0.1:8095").rstrip("/")
ASR_BASE = live.read_env("ASR_BASE", "http://127.0.0.1:8080").rstrip("/")
FA_BASE = live.read_env("FA_BASE", "http://127.0.0.1:8090").rstrip("/")
REQUEST_TIMEOUT_SECONDS = int(live.read_env("STT_REQUEST_TIMEOUT_SECONDS", "7200"))
MAX_DURATION_SECONDS = 1800
REPORT_PATH = live.output_dir() / "asr_fa_swap.json"


def download_audio(youtube_url: str, target_dir: Path) -> tuple[str, Path]:
    live.ensure_command("yt-dlp")
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--no-progress",
        "-f",
        "ba/b",
        "-o",
        str(target_dir / "source.%(ext)s"),
        "--print",
        "after_move:id",
        youtube_url,
    ]
    print("[INFO] Downloading audio with yt-dlp...")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"yt-dlp failed with exit code {proc.returncode}: {detail}")
    video_id = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else ""
    if not video_id:
        raise RuntimeError("video id is empty")
    sources = [
        path
        for path in target_dir.iterdir()
        if path.is_file() and path.name.startswith("source.")
    ]
    if not sources:
        raise RuntimeError("downloaded media is missing")
    print(f"[INFO] video_id={video_id} source={sources[0]}")
    return video_id, sources[0]


def normalize_pcm_wav(src: Path, dst: Path) -> dict[str, int]:
    live.ensure_command("ffmpeg")
    proc = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(src),
            "-ac",
            "1",
            "-ar",
            "16000",
            "-c:a",
            "pcm_s16le",
            str(dst),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"ffmpeg failed with exit code {proc.returncode}: {detail}")
    return live.inspect_pcm_wav(dst)


def require_asr_chunks(asr_payload: dict[str, Any], num_samples: int) -> list[dict[str, Any]]:
    audio = asr_payload.get("audio")
    chunks = asr_payload.get("chunks")
    if not isinstance(audio, dict) or not isinstance(chunks, list):
        raise RuntimeError("ASR JSON has no audio or chunks")
    if (
        audio.get("sample_rate") != live.PCM_SAMPLE_RATE
        or audio.get("channels") != live.PCM_CHANNELS
        or audio.get("num_samples") != num_samples
    ):
        raise RuntimeError(
            "ASR audio metadata does not match the wav: "
            f"sample_rate={audio.get('sample_rate')} channels={audio.get('channels')} "
            f"num_samples={audio.get('num_samples')} expected_samples={num_samples}"
        )
    texts: list[str] = []
    for position, chunk in enumerate(chunks):
        if not isinstance(chunk, dict) or not isinstance(chunk.get("text"), str):
            raise RuntimeError(f"chunks[{position}].text must be a string")
        texts.append(chunk["text"].strip())
    if not texts or all(text == "" for text in texts):
        raise RuntimeError("every ASR chunk text is empty")
    for position, chunk in enumerate(chunks):
        if texts[position] == "":
            continue
        language = chunk.get("language")
        if not isinstance(language, str) or language.strip() == "":
            raise RuntimeError(f"chunks[{position}] has text but no language; FA will return 400")
    return chunks


def assert_alignment(asr_payload: dict[str, Any], fa_body: dict[str, Any], duration: float) -> None:
    if fa_body.get("time_reference") != "audio_start":
        raise RuntimeError(f"time_reference={fa_body.get('time_reference')!r}")
    if fa_body.get("overlap_deduplicated") is not False:
        raise RuntimeError("overlap_deduplicated must stay false")
    asr_chunks = asr_payload.get("chunks")
    fa_chunks = fa_body.get("chunks")
    if not isinstance(asr_chunks, list) or not isinstance(fa_chunks, list):
        raise RuntimeError("FA chunks do not match ASR")
    if len(fa_chunks) != len(asr_chunks):
        raise RuntimeError(f"chunk count mismatch: asr={len(asr_chunks)} fa={len(fa_chunks)}")

    for src, dst in zip(asr_chunks, fa_chunks):
        if not isinstance(src, dict) or not isinstance(dst, dict):
            raise RuntimeError("FA chunks do not match ASR")
        text = src.get("text")
        if not isinstance(text, str):
            raise RuntimeError("ASR chunk text must be a string")
        for field in ("index", "start_sample", "end_sample", "text"):
            if dst.get(field) != src.get(field):
                raise RuntimeError(
                    f"chunks[{src.get('index')}].{field} mismatch: "
                    f"asr={src.get(field)!r} fa={dst.get(field)!r}"
                )
        items = dst.get("items")
        if not isinstance(items, list):
            raise RuntimeError(f"chunks[{src.get('index')}].items must be a list")
        if text.strip() == "":
            if dst.get("status") != "skipped_empty_text" or items != []:
                raise RuntimeError(f"chunks[{src.get('index')}] empty text was not skipped")
            continue
        if dst.get("status") != "aligned":
            raise RuntimeError(f"chunks[{src.get('index')}] status={dst.get('status')!r}")
        language = dst.get("language")
        if not isinstance(language, str) or language.strip() == "":
            raise RuntimeError(f"chunks[{src.get('index')}] missing canonical language")
        if not items:
            raise RuntimeError(f"chunks[{src.get('index')}] has no aligned items")
        start_sample = live.as_int(src.get("start_sample"), "start_sample")
        end_sample = live.as_int(src.get("end_sample"), "end_sample")
        lo = start_sample / live.PCM_SAMPLE_RATE
        hi = end_sample / live.PCM_SAMPLE_RATE
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("text"), str) or item["text"] == "":
                raise RuntimeError(f"chunks[{src.get('index')}] empty aligned item")
            start_time = item.get("start_time")
            end_time = item.get("end_time")
            if not isinstance(start_time, (int, float)) or isinstance(start_time, bool):
                raise RuntimeError(f"chunks[{src.get('index')}] invalid start_time")
            if not isinstance(end_time, (int, float)) or isinstance(end_time, bool):
                raise RuntimeError(f"chunks[{src.get('index')}] invalid end_time")
            if not (
                float(start_time) >= 0
                and float(start_time) <= float(end_time) <= duration
                and float(start_time) >= lo - 0.001
                and float(end_time) <= hi + 0.001
            ):
                raise RuntimeError(
                    f"chunks[{src.get('index')}] item time {start_time}..{end_time} "
                    f"outside [{lo}, {hi}] duration={duration}"
                )


def log_note(name: str) -> dict[str, Any]:
    raw = live.read_env(name, "")
    if not raw:
        return {"path": None, "present": False, "oom": False}
    path = Path(raw)
    oom = False
    if path.is_file():
        oom = live.contains_oom(path.read_text(encoding="utf-8", errors="replace"))
    return {"path": str(path), "present": path.is_file(), "oom": oom}


def admission(
    asr_state: str,
    asr_reserved: Any,
    asr_active: Any,
    asr_account: dict[str, Any],
    fa_peak: int,
    margin: int,
    free: int,
    total: int,
) -> dict[str, Any]:
    reserved_missing = asr_reserved in (0, None)
    if reserved_missing:
        if asr_state in ("ready", "starting", "stopping"):
            other = asr_account["peak_bytes"]
        elif asr_state == "stopped":
            other = asr_account["unloaded_residual_bytes"]
        else:
            other = -1
    else:
        other = live.as_int(asr_reserved, "qwen-asr reserved_bytes")

    rest_other = asr_account["unloaded_residual_bytes"]
    if asr_state in ("unknown", "failed", "shutdown"):
        rest_other = -1
    elif (
        asr_state == "stopped"
        and asr_reserved == asr_account["peak_bytes"]
        and asr_account["peak_bytes"] > asr_account["unloaded_residual_bytes"]
    ):
        rest_other = asr_account["peak_bytes"]

    victim = asr_state == "ready" and asr_active == 0
    fits = other >= 0 and free - margin >= fa_peak + other
    rest = rest_other >= 0 and total - margin >= fa_peak + rest_other
    return {
        "fa_load_bound": fa_peak,
        "other_hold": other,
        "safety_margin": margin,
        "free": free,
        "total": total,
        "fits": fits,
        "fits_if_rest_residual": rest,
        "idle_ready_victim": victim,
        "peak_note": (
            "example peaks stay at the 30-minute silence measurement; "
            "this duration is not that proof"
        ),
    }


def main() -> int:
    live.load_dotenv()
    report: dict[str, Any] = {
        "ok": False,
        "at": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "youtube_url": YOUTUBE_URL,
        "infer_base_url": INFER_BASE_URL,
        "failure": None,
    }
    try:
        cfg = live.load_config()
        gpu = live.gpu_account(cfg)
        asr_account = live.model_account(cfg, "qwen-asr")
        fa_account = live.model_account(cfg, "qwen-fa")
        host = live.nvidia_gpus()[0]
        report["environment"] = {
            "commit": live.git_commit(),
            "config": str(live.config_path()),
            "gpu_uuid": host["uuid"],
            "config_device": gpu["device"],
            "asr_profile": asr_account["profile_id"],
            "asr_measured_on": asr_account["measured_on"],
            "fa_profile": fa_account["profile_id"],
            "fa_measured_on": fa_account["measured_on"],
            "speech_note": live.read_env(
                "SPEECH_NOTE",
                "operator must confirm the utterance; this script does not detect language",
            ),
            "peak_note": (
                "this run's duration is not a 30-minute speech peak proof; "
                "example peaks are 30-minute silence and are not lowered"
            ),
            "nvidia": host,
        }
        if host["uuid"] != gpu["device"] or host["uuid"] != asr_account["measured_on"] or host["uuid"] != fa_account["measured_on"]:
            raise RuntimeError(
                "GPU UUID does not match gpu.device and both profile measuredOn values "
                f"(saw {host['uuid']})"
            )

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
        if asr_before_status != 200 or fa_before_status != 200:
            raise RuntimeError(
                f"control status precondition failed asr={asr_before_status} fa={fa_before_status}"
            )

        with tempfile.TemporaryDirectory(prefix="asr-fa-swap-") as tmp_name:
            tmp = Path(tmp_name)
            video_id, source = download_audio(YOUTUBE_URL, tmp)
            wav_path = tmp / "canonical.wav"
            wav = normalize_pcm_wav(source, wav_path)
            duration = wav["num_samples"] / wav["sample_rate"]
            report["source"] = {
                "video_id": video_id,
                "wav_bytes": wav_path.stat().st_size,
                "duration_sec": duration,
                **wav,
            }
            if not 0 < duration <= MAX_DURATION_SECONDS:
                raise RuntimeError(
                    f"duration {duration} is outside 0 < duration <= {MAX_DURATION_SECONDS}; not trimmed"
                )
            wav_bytes = wav_path.read_bytes()

        asr_body = live.build_multipart(
            "InferSwapASR",
            [
                ("file", "file", "canonical.wav", "audio/wav", wav_bytes),
                ("field", "response_format", "verbose_json"),
                ("field", "include_chunks", "true"),
            ],
        )
        report["asr_multipart_bytes"] = len(asr_body)
        if len(asr_body) > asr_account["max_body_bytes"]:
            raise RuntimeError(
                f"multipart bytes {len(asr_body)} exceed qwen-asr maxBodyBytes {asr_account['max_body_bytes']}"
            )

        models_before_status, models_before, _models_text = live.get_json(f"{INFER_BASE_URL}/v1/models")
        if models_before_status != 200:
            raise RuntimeError(f"GET /v1/models HTTP {models_before_status}")
        nvidia_before = live.nvidia_gpus()[0]
        asr_status, asr_raw, asr_elapsed = live.post_body(
            f"{INFER_BASE_URL}/infer?model=qwen-asr",
            asr_body,
            "multipart/form-data; boundary=InferSwapASR",
            REQUEST_TIMEOUT_SECONDS,
        )
        asr_text = asr_raw.decode("utf-8", errors="replace")
        report["asr"] = {"http": asr_status, "elapsed_sec": asr_elapsed}
        if asr_status != 200:
            raise RuntimeError(f"ASR HTTP {asr_status}: {asr_text[:1000]}")
        if live.contains_oom(asr_text):
            raise RuntimeError("profile peak exceeded")
        asr_payload = live.decode_json_body(asr_raw, "ASR")
        if not isinstance(asr_payload, dict):
            raise RuntimeError("ASR JSON is not an object")
        require_asr_chunks(asr_payload, wav["num_samples"])
        report["asr_response"] = asr_payload

        models_after_status, models_after_asr, _after_text = live.get_json(f"{INFER_BASE_URL}/v1/models")
        if models_after_status != 200:
            raise RuntimeError(f"GET /v1/models after ASR HTTP {models_after_status}")
        asr_remote_status, asr_remote, _remote_text = live.get_json(f"{ASR_BASE}/control/status")
        if asr_remote_status != 200:
            raise RuntimeError(f"ASR status after ASR HTTP {asr_remote_status}")
        fa_remote_status, fa_remote, _fa_remote_text = live.get_json(f"{FA_BASE}/control/status")
        nvidia_after_asr = live.nvidia_gpus()[0]
        asr_state_body = live.model_status(models_after_asr, "qwen-asr")
        sample = live.gpu_sample(models_after_asr)
        budget = admission(
            str(asr_state_body.get("state")),
            asr_state_body.get("reserved_bytes"),
            asr_remote.get("active_requests") if isinstance(asr_remote, dict) else None,
            asr_account,
            fa_account["peak_bytes"],
            gpu["safety_margin_bytes"],
            sample["free"],
            sample["total"],
        )
        budget.update(
            {
                "url": YOUTUBE_URL,
                "video_id": report["source"]["video_id"],
                "at": report["at"],
                "commit": report["environment"]["commit"],
                "gpu": host["uuid"],
                "asr_profile": asr_account["profile_id"],
                "fa_profile": fa_account["profile_id"],
                "duration_sec": duration,
                "wav_bytes": report["source"]["wav_bytes"],
                "asr_multipart_bytes": len(asr_body),
            }
        )
        report["budget"] = budget

        fa_body = live.build_multipart(
            "InferSwapFA",
            [
                ("file", "file", "canonical.wav", "audio/wav", wav_bytes),
                ("field", "payload", asr_text),
            ],
        )
        report["fa_multipart_bytes"] = len(fa_body)
        budget["fa_multipart_bytes"] = len(fa_body)
        if len(fa_body) > fa_account["max_body_bytes"]:
            raise RuntimeError(
                f"multipart bytes {len(fa_body)} exceed qwen-fa maxBodyBytes {fa_account['max_body_bytes']}"
            )

        fa_status, fa_raw, fa_elapsed = live.post_body(
            f"{INFER_BASE_URL}/infer?model=qwen-fa",
            fa_body,
            "multipart/form-data; boundary=InferSwapFA",
            REQUEST_TIMEOUT_SECONDS,
        )
        fa_text = fa_raw.decode("utf-8", errors="replace")
        report["fa"] = {"http": fa_status, "elapsed_sec": fa_elapsed}
        if live.contains_oom(fa_text):
            raise RuntimeError("profile peak exceeded")

        models_final_status, models_final, _final_text = live.get_json(f"{INFER_BASE_URL}/v1/models")
        asr_after_status, asr_after, _asr_after_text = live.get_json(f"{ASR_BASE}/control/status")
        fa_after_status, fa_after, _fa_after_text = live.get_json(f"{FA_BASE}/control/status")
        nvidia_after = live.nvidia_gpus()[0]
        report["models"] = {
            "before": models_before,
            "after_asr": models_after_asr,
            "after": models_final if models_final_status == 200 else {"http": models_final_status},
        }
        report["control"] = {
            "after_asr": {"asr_http": asr_remote_status, "asr": asr_remote, "fa_http": fa_remote_status, "fa": fa_remote},
            "after_fa": {"asr_http": asr_after_status, "asr": asr_after, "fa_http": fa_after_status, "fa": fa_after},
        }
        report["nvidia"] = {"before": nvidia_before, "after_asr": nvidia_after_asr, "after": nvidia_after}
        report["transitions"] = {
            "before": {
                "local_asr": live.model_status(models_before, "qwen-asr").get("state"),
                "remote_asr": asr_before.get("state") if isinstance(asr_before, dict) else None,
                "residency_asr": asr_before.get("residency") if isinstance(asr_before, dict) else None,
            },
            "after_asr": {
                "local_asr": asr_state_body.get("state"),
                "remote_asr": asr_remote.get("state") if isinstance(asr_remote, dict) else None,
                "residency_asr": asr_remote.get("residency") if isinstance(asr_remote, dict) else None,
                "remote_fa": fa_remote.get("state") if isinstance(fa_remote, dict) else None,
                "residency_fa": fa_remote.get("residency") if isinstance(fa_remote, dict) else None,
            },
            "after_fa": {
                "local_asr": live.model_status(models_final, "qwen-asr").get("state") if models_final_status == 200 else None,
                "local_fa": live.model_status(models_final, "qwen-fa").get("state") if models_final_status == 200 else None,
                "remote_asr": asr_after.get("state") if isinstance(asr_after, dict) else None,
                "residency_asr": asr_after.get("residency") if isinstance(asr_after, dict) else None,
                "remote_fa": fa_after.get("state") if isinstance(fa_after, dict) else None,
                "residency_fa": fa_after.get("residency") if isinstance(fa_after, dict) else None,
            },
            "note": "models residency is remote only after a successful status read; null is not not_resident",
        }
        report["logs"] = {
            "infer": log_note("INFER_LOG"),
            "asr": log_note("ASR_LOG"),
            "fa": log_note("FA_LOG"),
        }

        if budget["fits"]:
            if fa_status != 200:
                raise RuntimeError(f"fits=true but FA HTTP {fa_status}: {fa_text[:1000]}")
        elif budget["fits_if_rest_residual"] and budget["idle_ready_victim"]:
            if fa_status != 200:
                raise RuntimeError(f"eviction headroom but FA HTTP {fa_status}: {fa_text[:1000]}")
            asr_res = asr_after.get("residency") if isinstance(asr_after, dict) else None
            fa_res = fa_after.get("residency") if isinstance(fa_after, dict) else None
            if asr_res != "not_resident" or fa_res != "resident":
                raise RuntimeError(f"after eviction ASR residency={asr_res} FA residency={fa_res}")
        else:
            if fa_status != 503:
                raise RuntimeError(f"expected FA 503 when the budget cannot admit, got {fa_status}")
            raise RuntimeError(
                "ASR to FA admission could not complete: "
                f"fits={budget['fits']} rest={budget['fits_if_rest_residual']} "
                f"victim={budget['idle_ready_victim']} FA={fa_status}"
            )

        fa_payload = live.decode_json_body(fa_raw, "FA")
        if not isinstance(fa_payload, dict):
            raise RuntimeError("FA JSON is not an object")
        assert_alignment(asr_payload, fa_payload, duration)
        report["alignment"] = fa_payload
        report["ok"] = True
        print("[SUCCESS] ASR then FA swap completed.")
        return 0
    except Exception as exc:  # noqa: BLE001
        report["failure"] = f"{type(exc).__name__}: {exc}"
        print(f"[FAIL] {report['failure']}")
        return 1
    finally:
        live.write_report(REPORT_PATH, report)


if __name__ == "__main__":
    sys.exit(main())
