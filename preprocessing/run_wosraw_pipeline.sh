#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Compatibility entry point. The pipeline itself requires only Python.
exec "${PYTHON:-python}" "$SCRIPT_DIR/run_wosraw_pipeline.py" "$@"
