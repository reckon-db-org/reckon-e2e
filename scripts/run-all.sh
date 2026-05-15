#!/usr/bin/env bash
#
# Run the full reckon-e2e torture suite.
#
# Each axis is its own app under apps/. Currently only
# integrity_torture is implemented; the script will pick up new
# axes automatically as they land in ct_opts dirs.

set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Compiling..."
rebar3 compile

echo
echo "==> Running CT suites..."
rebar3 ct

echo
echo "==> Done."
