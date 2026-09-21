# Third-party notices

InferSwap is an independently designed project. Selected concurrency behavior and tests are adapted from llama-swap under the MIT License. llama-swap is not copied as a whole.

## llama-swap

- Source: https://github.com/mostlygeek/llama-swap
- Baseline commit: `96e6f94c018b51ae9cfc420b3166cfcace5bf97a`
- License: MIT
- Copyright: Copyright (c) 2024 Benson Wong

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### Adapted InferSwap files

- `internal/runtime/runtime.go` — interface shape from `internal/process/process.go`
- `internal/runtime/client.go` — single-writer run loop, readiness polling, reverse proxy, `http.ErrAbortHandler` recovery, and SSE `X-Accel-Buffering: no` from `internal/process/process_command.go`
- `internal/router/scheduler/scheduler.go` — event/Effects skeleton from `internal/router/scheduler/scheduler.go`
- `internal/router/base.go` — run loop and grant/tracked-serve pattern from `internal/router/base.go`
- `internal/proxy/request.go` — JSON/multipart model extraction and body restore from `internal/swaputil/http.go`
- `internal/proxy/error.go` — HTTP error mapping and 429 `Retry-After` from `internal/swaputil/httperror.go`
- `cmd/inferswap/main.go` — listen/signal/shutdown from `llama-swap.go`

FIFO approval policy, exclusive residency, config schema, and mock runtime are new InferSwap code. llama-swap `fifo.go` ready fast path, batch GrantServe, and immediate concurrency 429 were not ported.
