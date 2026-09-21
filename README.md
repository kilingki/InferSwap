# InferSwap

InferSwap is a single OpenAI-compatible inference backend for services that use several Deep Learning models. The caller picks a model and a request. If that model is not ready, InferSwap waits, evacuates whatever is occupying the GPU slot, then forwards the request.

Load and unload are not caller APIs. Seamless does not mean switching is free. It means the caller does not manage model lifecycle.

InferSwap does not own runtime processes. Containers, dependencies, and real load/unload belong to each model project. InferSwap talks to them through a shared control contract.

## Status (v0)

v0 is a small server that proves request routing, FIFO wait, exclusive swap, cancellation, and busy protection against mock models A and B. There is no real VRAM accounting, no multi-model residency, and no vLLM / llama.cpp connection yet.

Exclusive residency (at most one managed model ready) is a temporary v0 policy, not the end state.

## Quick start

Requires Go 1.25+.

```bash
go test ./...
cp config.example.yaml config.yaml
go run ./cmd/inferswap -config config.yaml
```

`config.example.yaml` is a shape example. Each model is a `baseURL` of a model project that is already up, or that InferSwap can start once via `prepare.argv`. v0 behavior is covered by tests, not by running that example against a live GPU.

## API

- `POST /v1/chat/completions`
- `POST /v1/completions`
- `POST /v1/embeddings`
- `POST /v1/audio/transcriptions`
- `GET /v1/models`
- `GET /health` — InferSwap process liveness, not “this model is ready”

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen","messages":[{"role":"user","content":"hi"}]}'
```

Selected llama-swap provenance is in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
