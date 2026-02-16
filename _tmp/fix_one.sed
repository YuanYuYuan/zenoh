# Multi-line pattern to replace callback
/declare_subscription/,/\.await;/ {
    # Match the start of the callback pattern
    /&mut |p, m| {$/ {
        # Read the next 6 lines
        N;N;N;N;N;N
        # Replace the whole pattern
        s/&mut |p, m| {\n            let p = p\.clone();\n            let msg = m\.msg\.clone();\n            tokio::spawn(async move {\n                let _ = p\.send_declare(msg)\.await;\n            });\n        },/\&mut |p, m| {\n            declares.push((p.clone(), m.msg.clone()));\n        },/
    }
}
