#!/bin/bash

while true; do
    echo "Running cargo x scheck..."
    if ! cargo x scheck; then
        echo "Deadlock detected or test failed!"
        break
    fi
    echo "No deadlock found. Retrying..."
done