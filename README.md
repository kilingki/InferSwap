# InferSwap

InferSwap is a single OpenAI-compatible inference backend for services that use several Deep Learning models. The caller picks a model and a request. If that model is not ready, InferSwap waits, unloads whatever else is ready, then forwards the request.

Load and unload are not caller APIs. Seamless does not mean switching is free. It means the caller does not manage model lifecycle.

InferSwap does not own runtime processes. Containers, dependencies, and real load/unload belong to each model project. InferSwap drives a project through `GET /control/status`, `POST /control/load`, and `POST /control/unload`. A model is ready when status reports `state=ready` and `residency=resident`.

## Status

Routing, FIFO wait, exclusive swap, cancellation, and per-model concurrency limits are implemented. At most one managed model is ready. That exclusive policy is temporary.

VRAM accounting, multi-model residency, and a built-in vLLM or llama.cpp lifecycle are still out of scope. A model project that implements the control API above can be registered by `baseURL`. `go test ./...` covers this behavior. `config.example.yaml` is a shape example, not a live GPU setup.

## Quick start

Requires Go 1.25+.

```bash
go test ./...
cp config.example.yaml config.yaml
go run ./cmd/inferswap -config config.yaml
```

On start, InferSwap reconciles each model's control status, then loads ids listed in `preload`. If the control endpoint is down and `prepare.argv` is set, that command runs once. `argv[0]` must be an absolute path. The working directory is the directory of that executable, and the environment is inherited from InferSwap.

## Configuration

Timeouts in `config.example.yaml` are seconds. Defaults: listen `:8080`, `maxQueueSize` 256, `queueTimeout` 180.

| Key | Role |
|---|---|
| `models.<id>.baseURL` | Control and inference base URL. Required. |
| `models.<id>.aliases` | Caller-facing names for the canonical id. |
| `models.<id>.concurrencyLimit` | In-flight requests allowed while that model is ready. Default 1. |
| `models.<id>.unlisted` | Omit the model from `GET /v1/models`. |
| `models.<id>.prepare.argv` | Optional command when the control endpoint is down. |
| `preload` | Model ids to load after reconcile. |
| `maxQueueSize` | Waiting requests before HTTP 429. |
| `queueTimeout` | How long a request may wait before HTTP 504. |

Per-model `timeouts` may override `prepareTimeout`, `healthCheckTimeout`, and `unloadTimeout`. `healthCheckTimeout` is the deadline from load start until status reports ready.

## API

- `POST /v1/chat/completions`
- `POST /v1/completions`
- `POST /v1/embeddings`
- `POST /v1/audio/transcriptions`
- `GET /v1/models` — registered models. `unlisted: true` is omitted. Each item includes `status.state`, `status.ready`, and `status.residency`.
- `GET /health` — InferSwap process liveness. Model readiness is control status.

`model` may be a canonical id or an alias, in JSON or `multipart/form-data`. Bodies larger than 32 MiB are rejected. InferSwap errors are JSON with `error`, `src` (`inferswap`), and `code`.

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen","messages":[{"role":"user","content":"hi"}]}'
```

Selected llama-swap provenance is in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
