#!/usr/bin/env bash
FILE="zenoh/src/net/tests/tables.rs"

# Add "let mut declares = vec![];" before each declare_subscription that doesn't have it
# and has declares.push in its callback

# Backup
cp "$FILE" "${FILE}.bak2"

# Use awk to add the vec declaration
awk '
/declare_subscription/ {
    # Check if previous 3 lines contain "let mut declares"
    has_vec = 0
    for (i = 1; i <= 3 && NR-i > 0; i++) {
        if (prev[NR-i] ~ /let mut declares/) {
            has_vec = 1
            break
        }
    }
    
    # Print the vec declaration if needed
    if (!has_vec) {
        # Check if this declare_subscription will use declares.push
        # For now, just add it before every declare_subscription
        print "    let mut declares = vec![];"
    }
}
{
    prev[NR] = $0
    print
}
' "${FILE}.bak2" > "$FILE"

echo "Added declares vec"
