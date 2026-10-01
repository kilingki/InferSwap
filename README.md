# InferSwap

InferSwap is a single inference proxy for services that use several models on one GPU. The caller names a model and sends a request. InferSwap waits, admits the load against the configured GPU budget, and forwards the request. When the budget is short, it selects one other ready model, waits until that model is idle, unloads it, and tries again.

Load and unload are not caller APIs. The caller does not manage model lifecycle.

Three roles stay separate:

| Role | Responsibility |
|---|---|
| Caller | Which model, and the request body. InferSwap does not interpret the output. |
| InferSwap | Queue, cancellation, order, and GPU admission. It does not own runtime processes and does not branch on engine name. |
| Model project | Containers, dependencies, and the real load, unload, and inference. InferSwap drives it through the [control contract](#model-control-contract). |

A model is ready when control status reports `state=ready` and `residency=resident`.

The models in the example do not fit on one GPU together. The caller still names one model. InferSwap holds each configured peak against the latest GPU sample and unloads another ready model when that budget is short.

```text
Caller  -- POST /infer?model=... -->  InferSwap :8095
                                         | queue, FIFO, GPU budget
                                         v
                              one model project (control + inference)
```

## Requirements

- Go 1.25.3 or newer, as in `go.mod`
- An NVIDIA GPU that NVML can read. `gpu.device` is that device's UUID
- One model project per configured model, each implementing the control contract. [`config.example.yaml`](config.example.yaml) points at the three projects below. Another implementation can be registered under the same contract.

## Example model projects

`config.example.yaml` is the RTX 3090 profile for three separate model projects. Each one implements the control contract, serves inference on the same base URL, and provides `prepare-inferswap`. That command starts the container when the control endpoint is down. It does not load weights. InferSwap still sends `POST /control/load` and `POST /control/unload`.

| Model id | Project | Port | `inferencePath` |
|---|---|---|---|
| `qwen-asr` | [qwen-asr-vllm-docker-config](https://github.com/kilingki/qwen-asr-vllm-docker-config) | `:8080` | `/v1/audio/transcriptions` |
| `qwen-fa` | [qwen-fa-docker-config](https://github.com/kilingki/qwen-fa-docker-config) | `:8090` | `/align` |
| `qwen-vlm` | [llama-cpp-docker-config](https://github.com/kilingki/llama-cpp-docker-config) | `:8000` | `/v1/chat/completions` |

`qwen-asr` is Qwen3-ASR-1.7B in one vLLM container. `qwen-fa` is Qwen3-ForcedAligner-0.6B. Its align request is the same WAV plus the unmodified ASR JSON (`response_format=verbose_json`, `include_chunks=true`). `qwen-vlm` is the llama.cpp slot for the Qwen3.8-27B GGUF and its mmproj. Text chat and one image both use that id. `?model=qwen3.8-27b` is an alias of `qwen-vlm`. The JSON `"model"` field is forwarded unchanged and stays the llama-server name `qwen3.8-27b`.

Docker, the NVIDIA Container Toolkit, and the weight files belong to those projects. The example peaks were measured with these three builds on one RTX 3090. The three models do not fit together. The numbers are in [docs/measurements.md](docs/measurements.md).

## Quick start

This repository is the proxy. It does not start the model processes. Those containers come from the [example model projects](#example-model-projects). `config.example.yaml` keeps their measured RTX 3090 peaks, and InferSwap listens on `:8095`. `gpu.device`, `prepare.argv`, and `measuredOn` in that file are placeholders. Before starting, set at least:

- `gpu.device` to this machine's NVML UUID (`nvidia-smi -L`)
- each `models.<id>.baseURL`
- each `models.<id>.prepare.argv`, or omit `prepare` if the control endpoint is already up. `argv[0]` must be an absolute path
- each `models.<id>.resourceProfile` from a measurement on this GPU. An unmeasured model left `unknown` blocks every new load. See [docs/measurements.md](docs/measurements.md)

```bash
cp config.example.yaml config.yaml
go run ./cmd/inferswap -config config.yaml
```

On start, InferSwap reads the configured GPU, reconciles each model's control status, then loads ids listed in `preload` through the same admission path. Reconcile does not run `prepare`. When a load finds the control endpoint down and `prepare.argv` is set, that command runs. The working directory is the directory of `argv[0]`, and the environment is inherited from InferSwap. While that child is still running, another prepare is not started.

## Configuration

Integer timeouts are seconds. When a key is omitted, the defaults are listen `:8080`, `logLevel` `info`, `healthCheckTimeout` 120, `unloadTimeout` 30, `queueTimeout` 180, `prepareTimeout` 60, `statusTimeout` 5, `drainTimeout` 180, `shutdownTimeout` 60, `maxQueueSize` 256, `concurrencyLimit` 1, `gpu.safetyMarginBytes` 0, `gpu.maxObservationAge` 5. [`config.example.yaml`](config.example.yaml) sets `listen` to `:8095` and `gpu.safetyMarginBytes` to 1 GiB. `logLevel` is `debug`, `info`, `warn`, or `error` and sets the process slog level.

One model, with the measured peaks left in the example file:

```yaml
listen: ":8095"
gpu:
  device: "GPU-00000000-0000-0000-0000-000000000000"
  safetyMarginBytes: 1073741824
models:
  qwen-asr:
    baseURL: "http://127.0.0.1:8080"
    prepare:
      argv: ["/absolute/path/prepare-inferswap"]
    resourceProfile:
      profileId: measured-on-this-gpu
      loadPeakBytes: 22060990464
      inferencePeakBytes: 22060990464
      unloadedResidualBytes: 0
      maxConcurrency: 1
    inferencePath: /v1/audio/transcriptions
    maxBodyBytes: 1073741824
```

| Key | Role |
|---|---|
| `listen` | HTTP listen address. |
| `gpu.device` | NVML UUID of the GPU InferSwap accounts for. Required. |
| `gpu.safetyMarginBytes` | Bytes of free memory kept unused. |
| `gpu.maxObservationAge` | Maximum age of a GPU sample, in seconds. An older sample is not fresh. |
| `models.<id>.baseURL` | Control and inference base URL. Required. |
| `models.<id>.name` | Display name. Defaults to the model id. |
| `models.<id>.aliases` | Caller-facing names for the canonical id. |
| `models.<id>.concurrencyLimit` | In-flight requests allowed while that model is ready. Cannot exceed `resourceProfile.maxConcurrency`. |
| `models.<id>.unlisted` | Omit the model from `GET /v1/models`. |
| `models.<id>.prepare.argv` | Optional command when a load finds the control endpoint down. |
| `models.<id>.resourceProfile` | Required measured load peak, inference peak, residual after unload, and the concurrency used for that measurement. The byte fields are that host's `nvidia-smi` total used peaks, not per-process VRAM. |
| `models.<id>.inferencePath` | Exactly one internal path InferSwap forwards to after `POST /infer`. Empty, relative, `..`, query, and control paths are rejected when the config is loaded. `inferencePaths` is not accepted. |
| `models.<id>.maxBodyBytes` | Maximum request body forwarded to the model. Required. |
| `preload` | Model ids to load after reconcile, through the same admission path. |
| `maxQueueSize` | Waiting requests before HTTP 429. |
| `queueTimeout` | How long a request may wait in the queue before HTTP 504. |
| `drainTimeout` | How long a selected ready model may stay busy before the waiting request is HTTP 504. Unload does not start on expiry. |
| `shutdownTimeout` | Deadline shared by HTTP server shutdown and router shutdown. |
| `statusTimeout` | Deadline for one control status read. |
| `healthCheckTimeout` | Deadline for the load and ready wait after the control endpoint answers. |
| `prepareTimeout` | Deadline for one `prepare.argv` run. |
| `unloadTimeout` | Deadline for one unload call. |

Per-model `timeouts` may override only `prepareTimeout`, `healthCheckTimeout`, and `unloadTimeout`.

## API

Public routes are `POST /infer`, `GET /v1/models`, and `GET /health`. Any other path is HTTP 404.

The example curls use the model project's own body. InferSwap forwards those bytes unchanged. The port and ids match `config.example.yaml`.

```bash
curl -fsS 'http://127.0.0.1:8095/infer?model=qwen-asr' \
  -F 'file=@canonical.wav;type=audio/wav' \
  -F 'response_format=verbose_json' \
  -F 'include_chunks=true' > asr.json

curl -fsS 'http://127.0.0.1:8095/infer?model=qwen-fa' \
  -F 'file=@canonical.wav;type=audio/wav' \
  -F 'payload=<asr.json' > alignment.json

curl -fsS 'http://127.0.0.1:8095/infer?model=qwen-vlm' \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3.8-27b","messages":[{"role":"user","content":"Hello"}],"max_tokens":64,"chat_template_kwargs":{"enable_thinking":false}}'
```

`POST /infer?model=<id or alias>` is the only public inference route. The model comes from the query. A missing or empty value is HTTP 400, and an unknown id or alias is HTTP 404. InferSwap removes that query before forwarding and rewrites the path to the selected model's `inferencePath`. Other query parameters and the body bytes are forwarded unchanged. Paths such as `/v1/audio/transcriptions` and `/align` stay inside the model process.

A body larger than that model's `maxBodyBytes` is HTTP 413. InferSwap errors are JSON:

```json
{"error": "gpu budget is not available", "src": "inferswap", "code": "RESOURCES"}
```

| HTTP | `code` | When |
|---|---|---|
| 400 | `BAD_REQUEST` | Missing model query, or a body InferSwap must parse and cannot. |
| 404 | `NOT_FOUND` | Unknown model, or any path other than `POST /infer`, `GET /v1/models`, and `GET /health`. |
| 413 | `BODY_TOO_LARGE` | Body exceeds that model's `maxBodyBytes`. |
| 429 | `QUEUE_FULL` | `maxQueueSize` waiting requests. `Retry-After: 1`. |
| 502 | `LOAD_FAILED` | The control load did not reach ready. |
| 502 | `UNLOAD_FAILED` | Unload failed. The next load stays blocked. |
| 502 | `BACKEND` | A failure that has no more specific code. |
| 503 | `RESOURCES` | The configured GPU budget cannot admit the target. |
| 503 | `NOT_READY` | GPU observation is not ready, or the runtime is not ready to forward. |
| 503 | `LIFECYCLE_PENDING` | A model lifecycle is still pending. |
| 503 | `SHUTDOWN` | The router is shutting down. |
| 504 | `QUEUE_TIMEOUT` | The request waited in the queue until `queueTimeout`. |
| 504 | `DRAIN_TIMEOUT` | The selected model stayed busy until `drainTimeout`. Unload does not start. |

Text and one image on `qwen-vlm` use that same URL. An image request puts `image_url` content parts in the JSON body. InferSwap forwards those bytes unchanged.

`GET /v1/models` lists registered models. `unlisted: true` is omitted.

```json
{
  "object": "list",
  "queue_depth": 0,
  "gpu": {
    "observed_at": "2026-10-01T00:00:00.000000000Z",
    "fresh": true,
    "total": 25769803776,
    "free": 1073741824
  },
  "data": [
    {
      "id": "qwen-asr",
      "object": "model",
      "owned_by": "inferswap",
      "name": "Qwen ASR",
      "status": {
        "state": "ready",
        "ready": true,
        "residency": "resident",
        "reason": "",
        "reserved_bytes": 22060990464,
        "last_error": null,
        "remote_state": "ready"
      }
    }
  ]
}
```

For `unknown`, `ready` and `residency` are null. A failed status read does not replace a previous residency with `not_resident`. A successful live status sets `status.remote_state`.

`GET /health` is process liveness and returns HTTP 200 with an empty body. Model readiness is control status.

## Model control contract

InferSwap calls these routes on `models.<id>.baseURL`. Callers do not.

| Method | Path | Body |
|---|---|---|
| `GET` | `/control/status` | none |
| `POST` | `/control/load` | `{}` |
| `POST` | `/control/unload` | `{}` |

A successful status, load, or unload response is JSON with all four fields. `active_requests` is a non-negative integer. The model project counts it during inference. Drain waits until it is 0 before unload starts. `last_error` is `null` or an object with `code` and `message`.

```json
{
  "state": "ready",
  "residency": "resident",
  "active_requests": 0,
  "last_error": null
}
```

`state` is `unloaded`, `loading`, `ready`, `unloading`, or `failed`. `residency` is `resident`, `not_resident`, or `unknown`. `ready` requires `residency=resident`. `unloaded` requires `residency=not_resident` and `active_requests=0`. A successful unload response is that unloaded status.

A non-200 control response uses this shape. InferSwap reads `error.code`, not the message.

```json
{"error": {"code": "BUSY", "message": "runtime has active inference requests"}}
```

## Limits

- One configured GPU. Admission uses the configured peaks and the latest fresh NVML sample. NVML does not attribute process bytes, so a hold is not reduced when that memory is already absent from free.
- Waiting requests stay in FIFO order. A later request does not pass an earlier one because its model is already ready.
- A model whose state is unknown, failed, or shutdown has no residual bound. Without a reservation, a new load is refused.
- Cancelling the HTTP client does not force-cancel work inside the model process.
- InferSwap does not invoke `docker`. The optional `prepare.argv` may run a command that starts containers with Compose when the control endpoint is down.

The admission rules and the RTX 3090 measurements are in [docs/measurements.md](docs/measurements.md).

## Development

```bash
go test ./...
go test -race ./...
```

`tests/*.py` attach to an already running InferSwap and the three model projects. They do not start those processes.

## License

[MIT](LICENSE). Selected concurrency behavior is adapted from llama-swap. GPU admission, exclusive unload, and this config schema are InferSwap's. Provenance is in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
