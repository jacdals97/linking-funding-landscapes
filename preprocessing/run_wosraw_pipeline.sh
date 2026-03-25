#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: bash preprocessing/run_wosraw_pipeline.sh <wos_raw_dir> <output_dir> [extra wosraw-pipeline args...]" >&2
    exit 1
fi

WOS_RAW_DIR="$1"
OUTPUT_DIR="$2"
shift 2

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/worker_logs"

wosraw-pipeline "$WOS_RAW_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --ncpu 16 \
    --max-piece-size 60000 \
    --xml-timeout-seconds 20000 \
    --heartbeat-seconds 60 \
    --worker-log-dir "$OUTPUT_DIR/worker_logs" \
    "$@"