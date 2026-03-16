#!/usr/bin/env bash
# Benchmark: simulated_traffic test across three clock quality configurations.
#
# Clock parameters (all in microseconds):
#   CLOCK_UNCERTAINTY    — max absolute jitter applied to each timestamp
#   CLOCK_SYNC_FREQUENCY — sync interval (modulo period for drift computation)
#   CLOCK_DRIFT          — drift rate multiplier (kept at 0 to isolate quality)
#   CLOCK_OFFSET         — constant base offset (kept at 0)
#
# Configurations:
#   high     ±10 μs    uncertainty, 1 ms   sync interval
#   medium   ±100 μs   uncertainty, 10 ms  sync interval
#   low      ±1000 μs  uncertainty, 100ms  sync interval
#   terrible ±10000 μs uncertainty, 1000ms sync interval

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$SCRIPT_DIR/log_parser/runs/benchmark_clock_quality"
PARSER="$SCRIPT_DIR/log_parser/parse_log.py"

mkdir -p "$BENCH_DIR"

# ── configuration table ────────────────────────────────────────────────────────
# Each row: name  uncertainty(μs)  sync_frequency(μs)  drift  offset
declare -a NAMES=("high_quality" "medium_quality" "low_quality" "terrible_quality")
declare -a UNCERTAINTY=(10       100              1000          10000)
declare -a SYNC_FREQ=(1001       10001            100001        1000001)
declare -a DRIFT=(0              0                0             0)
declare -a OFFSET=(1             10               100           1000)

# ── helpers ────────────────────────────────────────────────────────────────────
SEP="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

print_header() {
    echo ""
    echo "$SEP"
    printf "  %-20s  uncertainty=%-8s  sync_freq=%-10s  drift=%s  offset=%s\n" \
        "$1" "${2}μs" "${3}μs" "$4" "$5"
    echo "$SEP"
}

# ── results storage (parallel arrays indexed by run) ──────────────────────────
declare -a RES_MATCHED=()
declare -a RES_PROPOSALS=()
declare -a RES_COMPLETED=()
declare -a RES_MEAN_LAT=()
declare -a RES_FAST=()
declare -a RES_SLOW=()
declare -a RES_THROUGHPUT=()
declare -a RES_RUNTIME=()
declare -a RES_HASH_ERRORS=()

# ── run each configuration ─────────────────────────────────────────────────────
for i in "${!NAMES[@]}"; do
    name="${NAMES[$i]}"
    unc="${UNCERTAINTY[$i]}"
    freq="${SYNC_FREQ[$i]}"
    drift="${DRIFT[$i]}"
    offset="${OFFSET[$i]}"

    log_dir="$BENCH_DIR/$name"
    log_file="$log_dir/test.log"
    parse_out="$log_dir/parse_output.txt"
    mkdir -p "$log_dir"

    print_header "$name" "$unc" "$freq" "$drift" "$offset"
    echo "  Log → $log_file"
    echo ""

    export CLOCK_UNCERTAINTY="$unc"
    export CLOCK_SYNC_FREQUENCY="$freq"
    export CLOCK_DRIFT="$drift"
    export CLOCK_OFFSET="$offset"

    cargo test \
        --features "logging,toml_config" \
        -p omnipaxos \
        --test dom_integration \
        simulated_traffic \
        -- --nocapture \
        2>"$log_file" || echo "  [WARN] test exited with non-zero status for $name"

    unset CLOCK_UNCERTAINTY CLOCK_SYNC_FREQUENCY CLOCK_DRIFT CLOCK_OFFSET

    echo "  Parsing..."
    python3 "$PARSER" "$log_file" 2>/dev/null | tee "$parse_out"

    # Extract fields from parse output for the summary table
    matched=$(grep  "Number matched"    "$parse_out" | grep -oE '[0-9]+' | head -1 || echo "?")
    proposals=$(grep "Proposals with"  "$parse_out" | grep -oE '^[0-9]+'           || echo "?")
    completed=$(grep "Proposals with"  "$parse_out" | sed 's/.*over \([0-9]*\) completed.*/\1/'     || echo "?")
    mean_lat=$(grep  "Proposals with"  "$parse_out" | sed 's/.*latency of \([0-9.]*\).*/\1/'        || echo "?")
    fast=$(grep      "Fast Path"       "$parse_out" | sed 's/^\([0-9]*\) Fast.*/\1/'                || echo "?")
    slow=$(grep      "Slow Path"       "$parse_out" | sed 's/.*:: \([0-9]*\) Slow.*/\1/'            || echo "?")
    throughput=$(grep "Mean Throughput" "$parse_out" | sed 's/.*Throughput: \([0-9.]*\).*/\1/'       || echo "?")
    runtime=$(grep   "Mean Throughput" "$parse_out" | sed 's/.*Runtime: \([0-9.]*\).*/\1/'          || echo "?")
    hash_err=$(grep  "Hash Errors"     "$parse_out" | grep -oE '[0-9]+$'                            || echo "?")

    RES_MATCHED+=("$matched")
    RES_PROPOSALS+=("$proposals")
    RES_COMPLETED+=("$completed")
    RES_MEAN_LAT+=("$mean_lat")
    RES_FAST+=("$fast")
    RES_SLOW+=("$slow")
    RES_THROUGHPUT+=("$throughput")
    RES_RUNTIME+=("$runtime")
    RES_HASH_ERRORS+=("$hash_err")
done

# ── summary table ──────────────────────────────────────────────────────────────
echo ""
echo "$SEP"
echo "  BENCHMARK SUMMARY"
echo "$SEP"
printf "  %-20s  %9s  %9s  %11s  %7s  %7s  %14s  %10s  %11s\n" \
    "Config" "Matched" "Completed" "Mean Lat(ms)" "Fast" "Slow" "Throughput(r/s)" "Runtime(s)" "Hash Errors"
echo "  $(printf '%.0s─' {1..100})"

for i in "${!NAMES[@]}"; do
    printf "  %-20s  %9s  %9s  %11s  %7s  %7s  %14s  %10s  %11s\n" \
        "${NAMES[$i]}" \
        "${RES_MATCHED[$i]}" \
        "${RES_COMPLETED[$i]}" \
        "${RES_MEAN_LAT[$i]}" \
        "${RES_FAST[$i]}" \
        "${RES_SLOW[$i]}" \
        "${RES_THROUGHPUT[$i]}" \
        "${RES_RUNTIME[$i]}" \
        "${RES_HASH_ERRORS[$i]}"
done

echo "$SEP"
echo ""
echo "  Raw logs and parse output saved under: $BENCH_DIR"
echo ""
