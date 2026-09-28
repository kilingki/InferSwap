"""Helpers for live InferSwap checks.

These scripts attach to an already running InferSwap. They do not start
processes and they do not call control load or unload.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import wave
from pathlib import Path
from typing import Any
from urllib import error, request

import yaml


PCM_SAMPLE_RATE = 16000
PCM_CHANNELS = 1
PCM_SAMPLE_WIDTH = 2
OOM_MARKERS = ("out of memory", "cuda out of memory", "oom")


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def output_dir() -> Path:
    path = project_root() / "tests" / "outputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_dotenv(path: Path | None = None) -> None:
    env_path = path or (project_root() / ".env")
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        val = value.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        os.environ[key] = val


def read_env(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    return value if value else default


def ensure_command(name: str) -> None:
    if shutil.which(name):
        return
    raise RuntimeError(f"Required command not found in PATH: {name}")


def config_path() -> Path:
    raw = os.environ.get("INFER_CONFIG", "").strip()
    if raw:
        return Path(raw)
    local = project_root() / "config.yaml"
    if local.is_file():
        return local
    return project_root() / "config.example.yaml"


def load_config(path: Path | None = None) -> dict[str, Any]:
    chosen = path or config_path()
    if not chosen.is_file():
        raise RuntimeError(f"config not found: {chosen}")
    loaded = yaml.safe_load(chosen.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise RuntimeError(f"config is not a mapping: {chosen}")
    return loaded


def as_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{field} must be an integer, got {value!r}")
    return value


def model_account(cfg: dict[str, Any], model_id: str) -> dict[str, Any]:
    models = cfg.get("models")
    if not isinstance(models, dict) or not isinstance(models.get(model_id), dict):
        raise RuntimeError(f"could not read model {model_id}")
    model = models[model_id]
    profile = model.get("resourceProfile")
    if not isinstance(profile, dict):
        raise RuntimeError(f"could not read {model_id} resourceProfile")
    limits = profile.get("limits")
    if not isinstance(limits, dict):
        limits = {}
    account = {
        "profile_id": profile.get("profileId"),
        "measured_on": limits.get("measuredOn"),
        "load_peak_bytes": profile.get("loadPeakBytes"),
        "inference_peak_bytes": profile.get("inferencePeakBytes"),
        "unloaded_residual_bytes": profile.get("unloadedResidualBytes"),
        "max_body_bytes": model.get("maxBodyBytes"),
    }
    for key, value in account.items():
        if value is None or value == "":
            raise RuntimeError(f"could not read {model_id} {key}")
        if key.endswith("_bytes"):
            account[key] = as_int(value, f"{model_id}.{key}")
    account["peak_bytes"] = max(account["load_peak_bytes"], account["inference_peak_bytes"])
    return account


def gpu_account(cfg: dict[str, Any]) -> dict[str, Any]:
    gpu = cfg.get("gpu")
    if not isinstance(gpu, dict):
        raise RuntimeError("could not read gpu")
    device = gpu.get("device")
    margin = gpu.get("safetyMarginBytes")
    if not isinstance(device, str) or not device.strip():
        raise RuntimeError("could not read gpu.device")
    return {"device": device.strip(), "safety_margin_bytes": as_int(margin, "gpu.safetyMarginBytes")}


def git_commit(root: Path | None = None) -> str:
    proc = subprocess.run(
        ["git", "-C", str(root or project_root()), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return "unknown"
    return proc.stdout.strip() or "unknown"


def nvidia_gpus() -> list[dict[str, Any]]:
    ensure_command("nvidia-smi")
    proc = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=uuid,memory.total,memory.used,memory.free",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"nvidia-smi failed: {detail}")
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 4 or not parts[0]:
            continue
        rows.append(
            {
                "uuid": parts[0],
                "memory_total_mib": int(parts[1]),
                "memory_used_mib": int(parts[2]),
                "memory_free_mib": int(parts[3]),
            }
        )
    if not rows:
        raise RuntimeError("nvidia-smi returned no GPU rows")
    return rows


def contains_oom(raw: str) -> bool:
    lowered = raw.lower()
    return any(marker in lowered for marker in OOM_MARKERS)


def count_control_loads(path: Path | None) -> int:
    if path is None or not path.is_file():
        return -1
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if "POST /control/load" in line:
                count += 1
    return count


def build_multipart(boundary: str, parts: list[tuple[Any, ...]]) -> bytes:
    chunks: list[bytes] = []
    for part in parts:
        kind = part[0]
        header = f"--{boundary}\r\n"
        if kind == "file":
            _, name, filename, content_type, payload = part
            header += (
                f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
                f"Content-Type: {content_type}\r\n\r\n"
            )
            chunks.append(header.encode("utf-8"))
            chunks.append(payload)
            chunks.append(b"\r\n")
        elif kind == "field":
            _, name, value = part
            header += f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            chunks.append(header.encode("utf-8"))
            chunks.append(str(value).encode("utf-8"))
            chunks.append(b"\r\n")
        else:
            raise RuntimeError(f"unknown multipart part {kind}")
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(chunks)


def _read_response(url: str, req: request.Request, timeout: float) -> tuple[int, bytes, float]:
    started = time.perf_counter()
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), time.perf_counter() - started
    except error.HTTPError as exc:
        return exc.code, exc.read(), time.perf_counter() - started
    except error.URLError as exc:
        raise RuntimeError(f"request failed: {url}: {exc.reason}") from exc


def try_get_json(url: str, timeout: float = 30) -> tuple[int, Any]:
    try:
        status, payload, _text = get_json(url, timeout)
    except RuntimeError as exc:
        if str(exc).startswith("request failed:"):
            return 0, {}
        raise
    return status, payload


def get_json(url: str, timeout: float = 30) -> tuple[int, Any, str]:
    status, raw, _elapsed = _read_response(url, request.Request(url, method="GET"), timeout)
    text = raw.decode("utf-8", errors="replace")
    if not text:
        return status, {}, text
    try:
        return status, json.loads(text), text
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"response from {url} is not JSON: {text[:500]}") from exc


def post_body(url: str, body: bytes, content_type: str, timeout: float) -> tuple[int, bytes, float]:
    req = request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": content_type},
    )
    return _read_response(url, req, timeout)


def decode_json_body(raw: bytes, label: str) -> Any:
    text = raw.decode("utf-8", errors="replace")
    if not text:
        raise RuntimeError(f"{label} body is empty")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{label} body is not JSON: {text[:500]}") from exc
    return payload


def model_status(models_body: dict[str, Any], model_id: str) -> dict[str, Any]:
    data = models_body.get("data")
    if not isinstance(data, list):
        raise RuntimeError("GET /v1/models has no data list")
    for item in data:
        if isinstance(item, dict) and item.get("id") == model_id:
            status = item.get("status")
            if not isinstance(status, dict):
                raise RuntimeError(f"{model_id} status is missing")
            return status
    raise RuntimeError(f"{model_id} is missing from GET /v1/models")


def gpu_sample(models_body: dict[str, Any]) -> dict[str, int]:
    gpu = models_body.get("gpu")
    if not isinstance(gpu, dict):
        raise RuntimeError("gpu.free or gpu.total is missing")
    return {
        "free": as_int(gpu.get("free"), "gpu.free"),
        "total": as_int(gpu.get("total"), "gpu.total"),
    }


def inspect_pcm_wav(path: Path) -> dict[str, int]:
    try:
        with wave.open(str(path), "rb") as wav:
            channels = wav.getnchannels()
            sample_width = wav.getsampwidth()
            sample_rate = wav.getframerate()
            num_samples = wav.getnframes()
            comptype = wav.getcomptype()
    except (wave.Error, EOFError) as exc:
        raise RuntimeError(f"Not a PCM WAV: {path}") from exc
    if (
        channels != PCM_CHANNELS
        or sample_width != PCM_SAMPLE_WIDTH
        or sample_rate != PCM_SAMPLE_RATE
        or comptype != "NONE"
        or num_samples <= 0
    ):
        raise RuntimeError(
            "WAV is not 16 kHz mono signed PCM16. "
            f"Got channels={channels}, sample_width={sample_width}, "
            f"sample_rate={sample_rate}, comptype={comptype}, num_samples={num_samples}."
        )
    return {
        "sample_rate": sample_rate,
        "channels": channels,
        "num_samples": num_samples,
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[INFO] report={path}")
