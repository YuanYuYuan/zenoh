#!/usr/bin/env bash

FILE="zenoh/src/net/tests/tables.rs"
BACKUP="${FILE}.bak"

# Create backup
cp "$FILE" "$BACKUP"

# Pattern 1: Replace the callback body
sed -i '
/declare_subscription/,/\.await;/ {
    /&mut |p, m| {/ {
        N
        N
        N
        N
        N
        N
        s/&mut |p, m| {\n            let p = p.clone();\n            let msg = m.msg.clone();\n            tokio::spawn(async move {\n                let _ = p.send_declare(msg).await;\n            });\n        },/\&mut |p, m| {\n            declares.push((p.clone(), m.msg.clone()));\n        },/
    }
}
' "$FILE"

# Pattern 2: Add the send loop after .await;
# This is trickier - we need to add it only where we changed the callback
# For now, let's do it manually or with a different approach

echo "Phase 1 done - replaced callback bodies"

