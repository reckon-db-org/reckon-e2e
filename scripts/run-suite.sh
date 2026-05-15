#!/usr/bin/env bash
#
# Run a single torture axis by name.
#
# Usage:
#   ./scripts/run-suite.sh integrity_torture
#   ./scripts/run-suite.sh integrity_torture/concurrent
#   ./scripts/run-suite.sh integrity_torture/tamper
#
# Without a subpath, runs every CT suite under apps/<axis>/test/.
# With a subpath, runs the matching SUITE.

set -euo pipefail

cd "$(dirname "$0")/.."

if [ $# -lt 1 ]; then
    echo "usage: $0 <axis>[/<scenario>]" >&2
    exit 2
fi

ARG="$1"
AXIS="${ARG%%/*}"
SCENARIO=""
if [[ "$ARG" == */* ]]; then
    SCENARIO="${ARG#*/}"
fi

DIR="apps/${AXIS}/test"
if [ ! -d "$DIR" ]; then
    echo "!! No such axis: ${AXIS} (expected ${DIR})" >&2
    exit 1
fi

if [ -z "$SCENARIO" ]; then
    echo "==> Running every suite under ${DIR}..."
    rebar3 ct --dir "$DIR"
else
    SUITE="${DIR}/${AXIS}_${SCENARIO}_SUITE"
    if [ ! -f "${SUITE}.erl" ]; then
        echo "!! No such scenario: ${SUITE}.erl" >&2
        exit 1
    fi
    echo "==> Running ${SUITE}..."
    rebar3 ct --suite="$SUITE"
fi
