#!/usr/bin/env bash
set -euo pipefail

FULL="test_full.jar"
TARGET="output.jar"
OUTPUT="test.jar"

FULL_SIZE=$(stat -c%s "$FULL")
SIZE=1000  # starting subset size in bytes; bump this if 1 byte is too trivial

echo "Full source size: $FULL_SIZE bytes"
echo "=========================================="

while true; do
    # cap subset size at the full file size on the final iteration
    if (( SIZE > FULL_SIZE )); then
        SIZE=$FULL_SIZE
    fi

    echo ""
    echo "--- Iteration: subset size = $SIZE bytes ---"

    # take the first $SIZE bytes of the full jar, overwrite the target
    head -c "$SIZE" "$FULL" > "$TARGET"

    cd client
    # run the actual transfer/build
    cargo run
    cd ..

    # compare jar_output.jar against the subset we just sent
    OUT_SIZE=$(stat -c%s "$OUTPUT" 2>/dev/null || echo 0)
    echo "Sent: $SIZE bytes | Output: $OUT_SIZE bytes"

    if cmp -s "$TARGET" "$OUTPUT"; then
        echo "RESULT: MATCH (identical)"
    else
        echo "RESULT: MISMATCH"
        # show first differing byte without dumping the whole diff
        cmp "$TARGET" "$OUTPUT" 2>&1 | head -1 || true
        diffs=$(cmp -l "$TARGET" "$OUTPUT" 2>/dev/null | wc -l || echo "N/A")
        echo "Differing byte positions (within overlap): $diffs"
    fi

    # stop after we've tested the full file size
    if (( SIZE >= FULL_SIZE )); then
        echo ""
        echo "=========================================="
        echo "Reached full file size. Stopping."
        break
    fi

    # exponential growth
    SIZE=$(( SIZE * 2 ))
done