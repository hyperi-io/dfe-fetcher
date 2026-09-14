#!/usr/bin/env bash
# Project:   dfe-fetcher
# File:      scripts/generics-gates.sh
# Purpose:   Run the generics acceptance gates (scripts/generics_gates.py)
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED

set -euo pipefail
command -v python3 >/dev/null 2>&1 || { echo "Python 3 required" >&2; exit 2; }
exec python3 "$(dirname "$0")/generics_gates.py" "$@"
