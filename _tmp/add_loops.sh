#!/usr/bin/env bash
FILE="zenoh/src/net/tests/tables.rs"

cp "$FILE" "${FILE}.bak3"

# After each declare_subscription...await; add the send loop if not already there
awk '
BEGIN { in_declare = 0 }
/declare_subscription/ { in_declare = 1 }
in_declare && /^    \.await;$/ {
    print
    # Check next few lines for the send loop
    getline next1
    if (next1 !~ /for \(p, msg\) in declares/) {
        # Add the send loop
        print "    for (p, msg) in declares {"
        print "        let _ = p.send_declare(msg).await;"
        print "    }"
    }
    print next1
    in_declare = 0
    next
}
{ print }
' "${FILE}.bak3" > "$FILE"

echo "Added send loops"
