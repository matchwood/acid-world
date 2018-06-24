#!/bin/bash
stack build  --test --no-run-tests --bench --no-run-benchmarks --file-watch --ghc-options="-Wwarn"
#
