#!/usr/bin/env bash
set -e

FILE="zenoh/src/net/tests/tables.rs"

# Function to add declares vec before each declare_subscription that has the spawn pattern
# We'll do this manually for each instance

# Read the file
content=$(cat "$FILE")

# Count instances
count=$(grep -c "tokio::spawn(async move {" "$FILE" || true)
echo "Found $count instances to fix"

# For now, let's just manually fix the critical ones
# Let me find the line numbers first
grep -n "declare_subscription" "$FILE" | grep -v "undeclare"

