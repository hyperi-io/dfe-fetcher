# Project:   dfe-fetcher
# File:      Makefile
# Purpose:   Standard CI targets
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED

.PHONY: check quality test build ci

check:
	hyperi-ci check

quality:
	hyperi-ci run quality

test:
	hyperi-ci run test

build:
	hyperi-ci run build

ci: quality test build
