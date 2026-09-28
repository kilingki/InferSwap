# Measurements

This file records how admission behaves and what was measured on one host. Usage is in the [README](../README.md).

## Admission

Routing, FIFO wait, cancellation, per-model concurrency, and GPU admission are implemented. A new load runs only when the latest sample of the configured GPU is fresh and `free` minus `safetyMarginBytes` covers the sum of holds. The target's hold is its configured peak, the larger of `loadPeakBytes` and `inferencePeakBytes`. A ready, starting, or stopping model holds that same peak. A stopped model holds `unloadedResidualBytes`. NVML does not attribute process bytes, so a hold is not reduced when that memory is already absent from free. A model whose state is unknown, failed, or shutdown has no residual bound. With no reservation, admission fails. With a reservation, that reservation stays in the sum, and the model is not an unload candidate, so a new load proceeds only when those current holds already fit. A failed or device-mismatched sample clears the previous sample immediately. A sample older than `gpu.maxObservationAge` is not fresh and also blocks a new load; that stored sample stays until a later observation replaces it. An existing reservation is kept.

If the target still cannot fit, InferSwap checks whether device total minus the safety margin could hold the target peak with every other model at its residual. That check fails when the bytes do not fit, and also when any other model is unknown, failed, or shutdown. The request is then HTTP 503 and no unload starts. When the check passes, InferSwap selects one other ready model, the one with the oldest last use. Unknown, starting, stopping, and failed models are not candidates. A ready model that still has local work is still the candidate: its gate closes and the drain deadline starts immediately, including while that work is in flight. Unload starts only after that local work is gone, then waits until status reports `active_requests=0` and the model is not loading or unloading. Drain expiry is HTTP 504 and does not start unload. After a confirmed unload, the next admission waits for a GPU sample taken after that unload and tries again, until the target fits or no ready candidate remains.

Shutdown waits while a model still holds a local execution slot, until the earlier of `drainTimeout` and the shutdown context deadline, then unloads only a model that is idle on the same terms. `cmd/inferswap` passes one `shutdownTimeout` context to HTTP server shutdown and then to router shutdown, so an HTTP drain can consume the deadline before idle unload runs. A model that is still busy at that deadline, or whose state is unknown, failed, starting, or stopping, is reported unconfirmed and is not treated as released.

Models are separate projects. InferSwap selects one only by `baseURL` and does not branch on engine name.

## Host profile

The peaks in `config.example.yaml` were measured on one RTX 3090. `nvidia-smi -L` reported 24576 MiB. NVML read that device's total, free, and used bytes. The configured peaks were not lowered to force a pass. The published example does not include that machine's NVML UUID or local prepare paths.

Measured on 2026-09-26 with host `nvidia-smi` total used. The example adds 1 GiB to the higher observed maximum and does not lower a peak that a later run did not exceed:

| Model | Profile | Observed load | Observed inference | Residual | Configured peak |
|---|---|---|---|---|---|
| `qwen-asr` | `rtx3090-asr-2026-09-26` | 20011 MiB on 1s silence; 19583 MiB on 30min | 20015 MiB on 1s silence; 19682 MiB on 30min | 0 | 21039 MiB |
| `qwen-fa` | `rtx3090-fa-2026-09-26` | 2793 MiB while loading | 5322 MiB | 0 | 6346 MiB |

The 30min input was 16 kHz mono PCM16, 57,600,044 bytes, three load/inference/unload rounds, external concurrency 1. ASR split it into 120s chunks and the engine log showed `Running: 2 reqs` while control `active_requests` stayed 1. FA aligned the same WAV as ten 180s Korean chunks with batch 4. Each of those six requests returned HTTP 200. Unload returned to the host baseline near 1.0–1.1 GiB and did not grow, so residual 0 remains a measurement. The example registers only these two measured models. An unmeasured model left `unknown` still has no residual bound and blocks every new load.

Admission uses those configured peaks plus `gpu.safetyMarginBytes` of 1 GiB. 21039 + 6346 + 1024 MiB is 28409 MiB, which does not fit in 24576 MiB. Coexistence was rejected: the two models were not both ready. On the InferSwap path the caller did not call load or unload. A short ASR transcription returned HTTP 200 and left FA unloaded. The following FA request returned HTTP 200 and left ASR unloaded. The 30min rounds did the same swap. A 33,603,052-byte WAV, above 32 MiB and under the 64 MiB `maxBodyBytes`, was forwarded and returned HTTP 200.

Also observed on this GPU, not by replaying mocks:

- Restart while ASR was resident reconstructed that model as ready and did not admit a second load as if the GPU were empty.
- `healthCheckTimeout` of 2 seconds marked ASR `failed` while the runtime was still `loading`. A following FA request returned HTTP 503 and FA stayed unloaded. After the runtime later became ready, InferSwap still showed ASR failed and resident, and FA remained unloaded.
- `unloadTimeout` of 1 second returned HTTP 502 `UNLOAD_FAILED` while ASR was still unloading and resident. The retry returned HTTP 503, and FA stayed unloaded.
- A configured endpoint on a closed port stayed `unknown` and a FA request returned HTTP 503 without loading FA.
- SIGTERM during an ASR load waited out `shutdownTimeout` (60 seconds) and exited 1 with `context deadline exceeded remaining=[qwen-asr]`. That was not recorded as a successful release.
- An in-flight large transcription kept running in vLLM after InferSwap had exited, until the engine returned HTTP 200. A separate early client cancel has also been seen to drop ASR `active_requests` to 0. InferSwap does not force-cancel runtime work.

`go test ./...` and `go test -race ./...` passed on 2026-09-26.
