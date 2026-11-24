#!/usr/bin/env bash
# Start the InferSwap proxy. Model projects are not started here.
set -euo pipefail
cd "$(dirname "$0")"
exec go run ./cmd/inferswap -config config.vllm.yaml
