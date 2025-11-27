#!/usr/bin/env bash
#
# Create the HBase table for Customer 360.
# Usage:
#   ./20_hbase_customer360.sh
#
# Requires: hbase shell on PATH.

set -euo pipefail

cat <<'EOF' | hbase shell
create 'customer360', { NAME => 'f', VERSIONS => 1 }
EOF

