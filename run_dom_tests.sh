#!/usr/bin/env bash
# run_dom_tests.sh  –  run dom_integration N times and summarise results
# Usage: ./run_dom_tests.sh [-j <jobs>] <N> [test_name]
#   -j <jobs>   – number of runs to execute in parallel (default: 1)
#   N           – total number of runs
#   test_name   – (optional) run only this test from the dom_integration suite

usage() {
    echo "Usage: $0 [-j <jobs>] <N> [test_name]" >&2
    echo "  -j <jobs>   number of runs to execute in parallel (default: 1)" >&2
    echo "  N           total number of runs" >&2
    echo "  test_name   (optional) run only this test from the dom_integration suite" >&2
    exit 1
}

JOBS=1
while getopts ":j:" opt; do
    case $opt in
        j) JOBS="$OPTARG" ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))

[[ $# -lt 1 || $# -gt 2 || ! "$1" =~ ^[1-9][0-9]*$ ]] && usage
[[ ! "$JOBS" =~ ^[1-9][0-9]*$ ]] && { echo "Error: -j must be a positive integer" >&2; usage; }

N=$1
TEST_FILTER="${2:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/dom_test_logs"

cd "$SCRIPT_DIR"
mkdir -p "$LOG_DIR"

# Pre-build once so parallel runs don't all race on the cargo build lock.
if [[ $JOBS -gt 1 ]]; then
    echo "Pre-building test binary..."
    cargo test --features "logging, toml_config" --test dom_integration --no-run 2>&1
    echo
fi

# run_one <run_num> <result_file>
# Executes one test iteration and writes a result line to <result_file>:
#   "ok||<summary>"  on success
#   "fail|<logfile>|<summary>"  on failure
run_one() {
    local i="$1"
    local result_file="$2"
    local tmpfile
    tmpfile=$(mktemp /tmp/dom_run_XXXXXX)

    if [[ $JOBS -gt 1 ]]; then
        # Parallel: capture all output to avoid interleaved terminal noise.
        cargo test --features "logging, toml_config" --test dom_integration \
            -- --nocapture ${TEST_FILTER:+--exact "$TEST_FILTER"} >"$tmpfile" 2>&1 || true
    else
        # Sequential: stream to terminal as before.
        cargo test --features "logging, toml_config" --test dom_integration \
            -- --nocapture ${TEST_FILTER:+--exact "$TEST_FILTER"} 2>&1 | tee "$tmpfile" || true
    fi

    local summary
    summary=$(grep -o 'test result:.*' "$tmpfile" | tail -1)
    [[ -z "$summary" ]] && summary="(no 'test result:' line found)"

    if [[ "$summary" == *"test result: ok"* ]]; then
        rm -f "$tmpfile"
        printf 'ok||%s\n' "Run $i: $summary" >"$result_file"
    else
        local logfile="$LOG_DIR/run_${i}.log"
        mv "$tmpfile" "$logfile"
        printf 'fail|%s|%s\n' "$logfile" "Run $i: $summary" >"$result_file"
    fi
}

# Allocate a result file for each run upfront so we can read them in order later.
result_files=()
for ((i = 1; i <= N; i++)); do
    result_files+=("$(mktemp /tmp/dom_result_XXXXXX)")
done

if [[ $JOBS -eq 1 ]]; then
    # Sequential path: original streaming behaviour.
    for ((i = 1; i <= N; i++)); do
        echo "========================================"
        echo "  Run $i / $N"
        echo "========================================"
        run_one "$i" "${result_files[$((i-1))]}"
        echo
    done
else
    # Parallel path: maintain a sliding window of at most JOBS live background
    # jobs.  We wait for the oldest outstanding job before launching a new one,
    # which keeps exactly JOBS slots busy and works on bash 3.x (no wait -n).
    echo "Running $N iterations with -j $JOBS..."
    echo
    active_pids=()

    for ((i = 1; i <= N; i++)); do
        # If the window is full, wait for the oldest job to free a slot.
        if [[ ${#active_pids[@]} -ge $JOBS ]]; then
            wait "${active_pids[0]}" 2>/dev/null || true
            active_pids=("${active_pids[@]:1}")
        fi

        run_one "$i" "${result_files[$((i-1))]}" &
        new_pid=$!
        active_pids+=($new_pid)
        echo "  Run $i started (pid $new_pid)"
    done

    # Drain remaining jobs.
    for pid in "${active_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo
fi

# Collect and print results in run order.
RESULTS=()
FAIL_LOGS=()
for ((i = 1; i <= N; i++)); do
    rf="${result_files[$((i-1))]}"
    line=$(cat "$rf" 2>/dev/null)
    rm -f "$rf"

    status="${line%%|*}"
    rest="${line#*|}"
    logfile="${rest%%|*}"
    summary="${rest#*|}"

    RESULTS+=("$summary")
    if [[ "$status" == "fail" ]]; then
        FAIL_LOGS+=("Run $i → $logfile")
    fi
done

echo "========================================"
echo "  Summary of all $N runs"
echo "========================================"
for r in "${RESULTS[@]}"; do
    echo "  $r"
done

ok_runs=0; fail_runs=0
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
