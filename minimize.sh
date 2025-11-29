#!/bin/bash

# Script for finding a minimum repro.

N=1
while true; do
    echo "Testing N=$N"
    RUST_BACKTRACE=1 RUST_LOG=info \
        ./target/release/rollup_emulator \
        --number-of-blocks=20 \
        --fast-sequencers=1 \
        --sleepy-sequencers=0 \
        --storage-path=/tmp \
        --seed "$N"

    status=$?
    if [ $status -ne 0 ]; then
        echo "❌ Failed at N=$N (exit code $status)"
        exit 1
    fi

    N=$((N+1))
done
