#!/usr/bin/env bash
# run_dom_tests.sh  –  run dom_integration N times and summarise results
# Usage: ./run_dom_tests.sh <N>

usage() {
    echo "Usage: $0 <N>" >&2
    echo "  Runs 'cargo test --test dom_integration -- --nocapture' N times" >&2
    echo "  and reports the result summary line from each run." >&2
    exit 1
}

[[ $# -ne 1 || ! "$1" =~ ^[1-9][0-9]*$ ]] && usage

N=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/dom_test_logs"
RESULTS=()
FAIL_LOGS=()

cd "$SCRIPT_DIR"
mkdir -p "$LOG_DIR"

for ((i = 1; i <= N; i++)); do
    echo "========================================"
    echo "  Run $i / $N"
    echo "========================================"

    tmpfile=$(mktemp /tmp/dom_run_XXXXXX)

    # Stream output to terminal; also save to tmpfile for later parsing.
    # '|| true' prevents a non-zero cargo-test exit from aborting the loop.
    cargo test --features "logging, toml_config" --test dom_integration -- --nocapture 2>&1 | tee "$tmpfile" || true
    # PIPESTATUS[0] is valid here because the pipeline ran in the current shell.
    cargo_exit=${PIPESTATUS[0]}

    # Extract the canonical "test result: …" line (last occurrence wins in
    # case cargo prints multiple test-suite blocks).
    summary_line=$(grep -o 'test result:.*' "$tmpfile" | tail -1)

    if [[ -z "$summary_line" ]]; then
        summary_line="(no 'test result:' line found — cargo exit code $cargo_exit)"
    fi

    if [[ "$summary_line" == *"test result: ok"* ]]; then
        # Clean run — logs not needed.
        rm -f "$tmpfile"
    else
        # Failing run — persist the log with a timestamped name.
        logfile="$LOG_DIR/run_${i}.log"
        mv "$tmpfile" "$logfile"
        FAIL_LOGS+=("Run $i → $logfile")
    fi

    RESULTS+=("Run $i: $summary_line")
    echo
done

echo "========================================"
echo "  Summary of all $N runs"
echo "========================================"
for r in "${RESULTS[@]}"; do
    echo "  $r"
done

# Aggregate counts (using arithmetic that is safe even when starting at 0).
ok_runs=0
fail_runs=0
for r in "${RESULTS[@]}"; do
    if [[ "$r" == *"test result: ok"* ]]; then
        ok_runs=$((ok_runs + 1))
    else
        fail_runs=$((fail_runs + 1))
    fi
done

echo "----------------------------------------"
echo "  PASSED runs : $ok_runs / $N"
echo "  FAILED runs : $fail_runs / $N"
if [[ ${#FAIL_LOGS[@]} -gt 0 ]]; then
    echo "----------------------------------------"
    echo "  Logs saved for failing runs:"
    for fl in "${FAIL_LOGS[@]}"; do
        echo "    $fl"
    done
fi
echo "========================================"
