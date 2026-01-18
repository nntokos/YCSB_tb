#!/usr/bin/env bash
set -euo pipefail

# =======================
# Config
# =======================

YCSB_ROOT="$HOME/Dev/tools/YCSB_tb"
BINDING_NAME="tbtrace"

# =======================
# Helpers
# =======================

die() {
  echo "ERROR: $1" >&2
  exit 1
}

# =======================
# Checks
# =======================

[[ -d "$YCSB_ROOT" ]] || die "YCSB_ROOT does not exist: $YCSB_ROOT"
cd "$YCSB_ROOT"

[[ -x bin/ycsb.sh ]] || die "bin/ycsb.sh missing or not executable"

# =======================
# Check if tbtrace is available
# =======================

if bin/ycsb.sh | grep -q "$BINDING_NAME"; then
  echo "tbtrace binding already available"
  exit 0
fi

echo "tbtrace binding not found — building YCSB..."

# =======================
# Build YCSB (including tbtrace)
# =======================

mvn -DskipTests -Dcheckstyle.skip=true -pl tbtrace -am package

# =======================
# Re-check
# =======================

if bin/ycsb.sh | grep -q "$BINDING_NAME"; then
  echo "tbtrace binding successfully built and available"
  exit 0
fi

die "tbtrace binding still not available after build"
