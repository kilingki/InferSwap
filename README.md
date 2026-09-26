# InferSwap

InferSwap is a single OpenAI-compatible inference backend for services that use several Deep Learning models. The caller picks a model and a request. InferSwap waits, admits the load against the configured GPU budget, and forwards the request. When the budget is short, it selects one other ready model, waits until that model is idle, unloads it, and tries again.

Load and unload are not caller APIs. The caller does not manage model lifecycle.

InferSwap does not own runtime processes. Containers, dependencies, and real load/unload belong to each model project. InferSwap drives a project through `GET /control/status`, `POST /control/load`, and `POST /control/unload`. A model is ready when status reports `state=ready` and `residency=resident`.

## Status

Routing, FIFO wait, cancellation, per-model concurrency, and GPU admission are implemented. A new load runs only when the latest sample of the configured GPU is fresh and `free` minus `safetyMarginBytes` covers the sum of holds. The target's hold is its configured peak, the larger of `loadPeakBytes` and `inferencePeakBytes`. A ready, starting, or stopping model holds that same peak. A stopped model holds `unloadedResidualBytes`. NVML does not attribute process bytes, so a hold is not reduced when that memory is already absent from free. A model whose state is unknown, failed, or shutdown has no residual bound. With no reservation, admission fails. With a reservation, that reservation stays in the sum, and the model is not an unload candidate, so a new load proceeds only when those current holds already fit. A failed or device-mismatched sample clears the previous sample immediately. A sample older than `gpu.maxObservationAge` is not fresh and also blocks a new load; that stored sample stays until a later observation replaces it. An existing reservation is kept.

If the target still cannot fit, InferSwap checks whether device total minus the safety margin could hold the target peak with every other model at its residual. That check fails when the bytes do not fit, and also when any other model is unknown, failed, or shutdown. The request is then HTTP 503 and no unload starts. When the check passes, InferSwap selects one other ready model, the one with the oldest last use. Unknown, starting, stopping, and failed models are not candidates. A ready model that still has local work is still the candidate: its gate closes and the drain deadline starts immediately, including while that work is in flight. Unload starts only after that local work is gone, then waits until status reports `active_requests=0` and the model is not loading or unloading. Drain expiry is HTTP 504 and does not start unload. After a confirmed unload, the next admission waits for a GPU sample taken after that unload and tries again, until the target fits or no ready candidate remains.

Shutdown waits while a model still holds a local execution slot, until the earlier of `drainTimeout` and the shutdown context deadline, then unloads only a model that is idle on the same terms. `cmd/inferswap` passes one `shutdownTimeout` context to HTTP server shutdown and then to router shutdown, so an HTTP drain can consume the deadline before idle unload runs. A model that is still busy at that deadline, or whose state is unknown, failed, starting, or stopping, is reported unconfirmed and is not treated as released.

Models are separate projects. InferSwap selects one only by `baseURL` and does not branch on engine name.

`config.example.yaml` records a measurement on this host. `nvidia-smi -L` reports `NVIDIA GeForce RTX 3090`, UUID `GPU-c47f1d0d-ea5d-4e6c-da7c-c9ad18b1b978`, 24576 MiB. NVML reads that device's total, free, and used bytes. The configured peaks were not lowered to force a pass.

Measured on 2026-09-26 with host `nvidia-smi` total used, three load/inference/unload rounds. The example adds 1 GiB to each observed maximum:

| Model | Profile | Observed load | Observed inference | Residual | Configured peak |
|---|---|---|---|---|---|
| `qwen-asr` | `rtx3090-asr-2026-09-26` | 20011 MiB | 20015 MiB | 0 | 21039 MiB |
| `qwen-fa` | `rtx3090-fa-2026-09-26` | 3229 MiB | 3256 MiB | 0 | 4280 MiB |

Unload returned to the host baseline near 1.4–1.5 GiB and did not grow across the three rounds, so residual 0 is a measurement. `maxConcurrency` is 1, matching those runs. ASR input was 1 second of 16 kHz silence. FA input was one Korean chunk on the same kind of audio. `qwen3.8-27b` in the example is still an unmeasured placeholder. If its endpoint is down, reconcile leaves it `unknown`, and that blocks every new load.

Admission uses those configured peaks plus `gpu.safetyMarginBytes` of 1 GiB. 21039 + 4280 + 1024 MiB is 26343 MiB, which does not fit in 24576 MiB. Coexistence was rejected: the two models were not both ready. On the InferSwap path the caller did not call load or unload. A short ASR transcription returned HTTP 200 and left FA unloaded. The following `POST /align?model=qwen-fa` returned HTTP 200 and left ASR unloaded. A 33,603,052-byte WAV, above 32 MiB and under the 64 MiB `maxBodyBytes`, was forwarded and returned HTTP 200.

Also observed on this GPU, not by replaying mocks:

- Restart while ASR was resident reconstructed that model as ready and did not admit a second load as if the GPU were empty.
- `healthCheckTimeout` of 2 seconds marked ASR `failed` while the runtime was still `loading`. A following FA request returned HTTP 503 and FA stayed unloaded. After the runtime later became ready, InferSwap still showed ASR failed and resident, and FA remained unloaded.
- `unloadTimeout` of 1 second returned HTTP 502 `UNLOAD_FAILED` while ASR was still unloading and resident. The retry returned HTTP 503, and FA stayed unloaded.
- A configured endpoint on a closed port stayed `unknown` and a FA request returned HTTP 503 without loading FA.
- SIGTERM during an ASR load waited out `shutdownTimeout` (60 seconds) and exited 1 with `context deadline exceeded remaining=[qwen-asr]`. That was not recorded as a successful release.
- An in-flight large transcription kept running in vLLM after InferSwap had exited, until the engine returned HTTP 200. A separate early client cancel has also been seen to drop ASR `active_requests` to 0. InferSwap does not force-cancel runtime work.

`go test ./...` and `go test -race ./...` passed on 2026-09-26.

## Quick start

Requires Go 1.25.3 or newer, as in `go.mod`.

```bash
go test ./...
cp config.example.yaml config.yaml
go run ./cmd/inferswap -config config.yaml
```

On start, InferSwap reads the configured GPU, reconciles each model's control status, then loads ids listed in `preload` through the same admission path. Reconcile does not run `prepare`. When a load finds the control endpoint down and `prepare.argv` is set, that command runs. `argv[0]` must be an absolute path. The working directory is the directory of that executable, and the environment is inherited from InferSwap. While that child is still running, another prepare is not started.

## Configuration

Integer timeouts are seconds. Defaults: listen `:8080`, `logLevel` `info`, `healthCheckTimeout` 120, `unloadTimeout` 30, `queueTimeout` 180, `prepareTimeout` 60, `statusTimeout` 5, `drainTimeout` 180, `shutdownTimeout` 60, `maxQueueSize` 256, `concurrencyLimit` 1, `gpu.safetyMarginBytes` 0, `gpu.maxObservationAge` 5. `logLevel` is accepted and not read by the process. The example listens on `:8095` so it can run beside ASR `:8080` and FA `:8090` on this host.

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
| `models.<id>.inferencePaths` | POST paths this model accepts. Control paths are rejected when the config is loaded. |
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

- `POST /v1/chat/completions`
- `POST /v1/completions`
- `POST /v1/embeddings`
- `POST /v1/audio/transcriptions`
- `POST /align?model=<id or alias>` — the `model` query selects the route and is removed before the body is forwarded to `/align`.
- `GET /v1/models` — registered models. `unlisted: true` is omitted. Each item includes `status.state`, `status.ready`, and `status.residency`. For `unknown`, `ready` and `residency` are null.
- `GET /health` — InferSwap process liveness. Model readiness is control status.

`model` may be a canonical id or an alias, in JSON, `multipart/form-data`, or the query string. A body larger than that model's `maxBodyBytes` is rejected with HTTP 413. A path outside `inferencePaths` is HTTP 404. InferSwap errors are JSON with `error`, `src` (`inferswap`), and `code`.

```bash
curl -s http://127.0.0.1:8095/v1/audio/transcriptions \
  -F 'file=@sample.wav;type=audio/wav' \
  -F 'model=qwen3-asr' \
  -F 'response_format=json'
```

Selected llama-swap provenance is in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
