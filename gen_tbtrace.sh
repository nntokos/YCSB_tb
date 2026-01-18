#!/usr/bin/env bash
set -euo pipefail

# =======================
# User parameters
# =======================

# YCSB repo root
YCSB_ROOT="$HOME/Dev/tools/YCSB"

# Treebeard experiment directory
EXP_DIR="$HOME/Dev/Research/treebeard/experiments/new_exp"

# Output trace (Treebeard expects this exact name)
TRACE_FILE="$EXP_DIR/trace.txt"

# Workload definition
WORKLOAD="$YCSB_ROOT/workloads/workloada"

# YCSB workload parameters
RECORDCOUNT=2000000          # keyspace size
OPERATIONCOUNT=3000000       # number of ops in run phase
READPROP=0.8
UPDATEPROP=0.2
REQUESTDIST=zipfian
ZIPFUAN_CONSTANT=0.99

# Determinism & ordering
THREADS=1

# ORAM / Treebeard alignment
VALUE_BYTES=1024             # block payload size
VALUE_SEED=1337
KEY_PREFIX="user"

# =======================
# Safety checks
# =======================

cd "$YCSB_ROOT"

if [[ ! -x "$YCSB_ROOT/bin/ycsb.sh" ]]; then
  echo "ERROR: ycsb.sh not found or not executable at $YCSB_ROOT/bin/ycsb.sh"
  exit 1
fi

mkdir -p "$EXP_DIR"

# =======================
# Generate trace
# =======================

echo "Generating YCSB trace at: $TRACE_FILE"

./bin/ycsb.sh run tbtrace \
  -threads "$THREADS" \
  -P "$WORKLOAD" \
  -p recordcount="$RECORDCOUNT" \
  -p operationcount="$OPERATIONCOUNT" \
  -p requestdistribution="$REQUESTDIST" \
  -p readproportion="$READPROP" \
  -p updateproportion="$UPDATEPROP" \
  -p zipfianconstant="$ZIPFUAN_CONSTANT" \
  -p tbtrace.file="$TRACE_FILE" \
  -p tbtrace.keyprefix="$KEY_PREFIX" \
  -p tbtrace.value.bytes="$VALUE_BYTES" \
  -p tbtrace.opcase=upper \
#   -p tbtrace.value.seed="$VALUE_SEED" \
#   -p tbtrace.flush.every=20000

# =======================
# Post-run sanity checks
# =======================

echo "Trace generated. Sanity checks:"

echo "Total lines:"
wc -l "$TRACE_FILE"

echo "First 5 lines:"
head -n 5 "$TRACE_FILE"

echo "Last 5 lines:"
tail -n 5 "$TRACE_FILE"

echo "Invalid lines (should be empty):"
grep -vE '^(GET|SET) ' "$TRACE_FILE" | head || true

echo "DONE"
