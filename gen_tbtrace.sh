#!/usr/bin/env bash
set -euo pipefail

# =======================
# User parameters
# =======================

usage() {
  cat <<'EOF'
Usage:
  ./gen_tbtrace.sh <workload_path> [--exp-dir DIR | --output-dir DIR]

Arguments:
  workload_path  Path to a YCSB workload file (.properties-style) that includes
                 ALL operation-stream params AND tbtrace.* settings.

Options:
  --exp-dir DIR, --output-dir DIR, -o DIR
                 Output directory (default: $HOME/Dev/Research/treebeard/experiments/example-experiment)
  -h, --help     Show this help.

Notes:
  - This script runs YCSB from exp_dir so a relative tbtrace.file (e.g. trace.txt)
    in the workload lands in that directory.
  - YCSB_ROOT is auto-detected as the directory containing this script.
EOF
}

EXP_DIR_DEFAULT="$HOME/Dev/Research/treebeard/experiments/example-experiment"
EXP_DIR="$EXP_DIR_DEFAULT"
WORKLOAD=""

# Parse flags, leaving a single required positional arg: workload
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --exp-dir|--output-dir)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: Missing value for $1" >&2
        usage
        exit 2
      fi
      EXP_DIR="$2"
      shift 2
      ;;
    -o)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: Missing value for -o" >&2
        usage
        exit 2
      fi
      EXP_DIR="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "ERROR: Unknown option: $1" >&2
      usage
      exit 2
      ;;
    *)
      if [[ -n "$WORKLOAD" ]]; then
        echo "ERROR: Unexpected extra argument: $1" >&2
        usage
        exit 2
      fi
      WORKLOAD="$1"
      shift
      ;;
  esac
done

# Allow workload after -- (still positional)
if [[ -z "$WORKLOAD" && $# -gt 0 ]]; then
  WORKLOAD="$1"
  shift
fi

if [[ -z "$WORKLOAD" || $# -gt 0 ]]; then
  if [[ -z "$WORKLOAD" ]]; then
    echo "ERROR: Missing required argument: workload_path" >&2
  else
    echo "ERROR: Too many arguments" >&2
  fi
  usage
  exit 2
fi

# YCSB repo root = directory containing this script
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
YCSB_ROOT="$SCRIPT_DIR"

# Output trace (Treebeard expects this exact name)
# NOTE: This is not passed on the CLI; it must match tbtrace.file in the workload.
TRACE_FILE="$EXP_DIR/trace.txt"

# =======================
# Safety checks
# =======================

if [[ ! -x "$YCSB_ROOT/bin/ycsb.sh" ]]; then
  echo "ERROR: ycsb.sh not found or not executable at $YCSB_ROOT/bin/ycsb.sh"
  exit 1
fi

if [[ ! -f "$WORKLOAD" ]]; then
  echo "ERROR: Workload file not found at: $WORKLOAD"
  exit 1
fi

# Resolve workload to an absolute path so it still works after we cd to EXP_DIR.
WORKLOAD_ABS="$(cd -- "$(dirname -- "$WORKLOAD")" && pwd)/$(basename -- "$WORKLOAD")"

mkdir -p "$EXP_DIR"

# Snapshot the workload used for this trace (reproducibility).
TS="$(date +%Y%m%d_%H%M%S)"
WORKLOAD_SNAPSHOT="$EXP_DIR/workload_${TS}_$(basename -- "$WORKLOAD_ABS")"
cp -f -- "$WORKLOAD_ABS" "$WORKLOAD_SNAPSHOT"
echo "Workload snapshot saved at: $WORKLOAD_SNAPSHOT"

echo "Generating YCSB trace at: $TRACE_FILE"

# Run from EXP_DIR so relative tbtrace.file (e.g. trace.txt) lands in the experiment folder.
cd "$EXP_DIR"

"$YCSB_ROOT/bin/ycsb.sh" run tbtrace \
  -P "$WORKLOAD_ABS"

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