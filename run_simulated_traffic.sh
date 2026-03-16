#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/log_parser/runs/simulated_traffic"
LOG_FILE="$LOG_DIR/test.log"

mkdir -p "$LOG_DIR"

echo "Running simulated_traffic test..."
echo "Logs -> $LOG_FILE"

# Run only the simulated_traffic test; capture stderr (where slog writes) to log file.
# --nocapture lets stdout through so Rust's print!/println! are visible too.
cargo test \
  --features "logging,toml_config" \
  -p omnipaxos \
  --test dom_integration \
  simulated_traffic \
  -- --nocapture \
  2>"$LOG_FILE"

echo ""
echo "Parsing logs..."
python3 "$SCRIPT_DIR/log_parser/parse_log.py" "$LOG_FILE"
