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

## Requirements

- Go 1.25.3 or newer, as in `go.mod`
- An NVIDIA GPU that NVML can read. `gpu.device` is that device's UUID
- One model project per configured model, each implementing the control contract

## Quick start

`config.example.yaml` keeps the measured peaks from an RTX 3090 profile (ASR on `:8080`, FA on `:8090`, InferSwap on `:8095`). `gpu.device`, `prepare.argv`, and `measuredOn` in that file are placeholders. Before starting, set at least:

- `gpu.device` to this machine's NVML UUID (`nvidia-smi -L`)
- each `models.<id>.baseURL`
- each `models.<id>.prepare.argv`, or omit `prepare` if the control endpoint is already up. `argv[0]` must be an absolute path
- each `models.<id>.resourceProfile` from a measurement on this GPU. An unmeasured model left `unknown` blocks every new load. See [docs/measurements.md](docs/measurements.md)

```bash
go test ./...
cp config.example.yaml config.yaml
go run ./cmd/inferswap -config config.yaml
```

On start, InferSwap reads the configured GPU, reconciles each model's control status, then loads ids listed in `preload` through the same admission path. Reconcile does not run `prepare`. When a load finds the control endpoint down and `prepare.argv` is set, that command runs. The working directory is the directory of `argv[0]`, and the environment is inherited from InferSwap. While that child is still running, another prepare is not started.

## Configuration

Integer timeouts are seconds. Defaults: listen `:8080`, `logLevel` `info`, `healthCheckTimeout` 120, `unloadTimeout` 30, `queueTimeout` 180, `prepareTimeout` 60, `statusTimeout` 5, `drainTimeout` 180, `shutdownTimeout` 60, `maxQueueSize` 256, `concurrencyLimit` 1, `gpu.safetyMarginBytes` 0, `gpu.maxObservationAge` 5. `logLevel` is `debug`, `info`, `warn`, or `error` and sets the process slog level.

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
| `models.<id>.resourceProfile` | Required measured load peak, inference peak, residual after unload, and the concurrency used for that measurement. |
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

Public inference is `POST /infer?model=<id or alias>` only. Any other path is HTTP 404. The model comes from the query. A missing or empty value is HTTP 400, and an unknown id or alias is HTTP 404. InferSwap removes that query before forwarding and rewrites the path to the selected model's `inferencePath`. Other query parameters and the body bytes are forwarded unchanged. Paths such as `/v1/audio/transcriptions` and `/align` stay inside the model process.

A body larger than that model's `maxBodyBytes` is HTTP 413. InferSwap errors are JSON:

```json
{"error": "gpu budget is not available", "src": "inferswap", "code": "RESOURCES"}
```

| HTTP | `code` | When |
|---|---|---|
| 400 | `BAD_REQUEST` | Missing model query, or a body InferSwap must parse and cannot. |
| 404 | `NOT_FOUND` | Unknown model, or any path other than the three public routes. |
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

- `GET /v1/models` lists registered models. `unlisted: true` is omitted. The list includes `queue_depth` and `gpu` (`observed_at`, `fresh`, `total`, `free`). Each item includes `status.state`, `status.ready`, `status.residency`, `status.reason`, `status.reserved_bytes`, and `status.last_error`. A successful live status also sets `status.remote_state`. For `unknown`, `ready` and `residency` are null. A failed status read does not replace a previous residency with `not_resident`.
- `GET /health` is process liveness and returns HTTP 200 with an empty body. Model readiness is control status.

```bash
curl -fsS 'http://127.0.0.1:8095/infer?model=qwen-asr' \
  -F 'file=@canonical.wav;type=audio/wav' \
  -F 'response_format=verbose_json' \
  -F 'include_chunks=true' > asr.json

curl -fsS 'http://127.0.0.1:8095/infer?model=qwen-fa' \
  -F 'file=@canonical.wav;type=audio/wav' \
  -F 'payload=<asr.json' > alignment.json
```

The port and model ids above match `config.example.yaml`.

## Model control contract

InferSwap calls these routes on `models.<id>.baseURL`. Callers do not.

| Method | Path | Body |
|---|---|---|
| `GET` | `/control/status` | none |
| `POST` | `/control/load` | `{}` |
| `POST` | `/control/unload` | `{}` |

A successful status, load, or unload response is JSON with all four fields. `active_requests` is a non-negative integer. `last_error` is `null` or an object with `code` and `message`.

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
- InferSwap does not start containers except the optional `prepare.argv` when the control endpoint is down.

The admission rules and the RTX 3090 measurements are in [docs/measurements.md](docs/measurements.md).

## Development

```bash
go test ./...
go test -race ./...
```

## License

[MIT](LICENSE). Selected llama-swap provenance is in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
