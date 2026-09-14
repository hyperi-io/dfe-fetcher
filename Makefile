# Project:   dfe-fetcher
# File:      Makefile
# Purpose:   Standard CI targets
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED

.PHONY: check quality test build gates ci

check: gates
	hyperi-ci check

quality:
	hyperi-ci run quality

test:
	hyperi-ci run test

build:
	hyperi-ci run build

# The generics acceptance gates: every provider-shaped mechanism at one
# framework site, the crate dependency direction, and the per-hook line
# budgets. Deterministic and dependency-free, so it runs beside the linters.
gates:
	bash scripts/generics-gates.sh

ci: gates quality test build
