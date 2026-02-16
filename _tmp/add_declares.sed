# Before each declare_subscription that uses declares.push, add the vec declaration
# and after .await; add the send loop

# Look for patterns where we have declare_subscription followed by declares.push
/declare_subscription/,/\.await;/ {
    # If we see declares.push in the callback, we need to add the vec and loop
    /declares\.push/ {
        # Mark that this block needs fixing
        h
    }
}

# After .await; if we marked the block, add the send loop
/^    \.await;$/ {
    # Get the hold space to check if we need to add
    x
    /declares\.push/ {
        # Add the send loop
        s/.*/    .await;\n    for (p, msg) in declares.drain(..) {\n        let _ = p.send_declare(msg).await;\n    }/
        x
        s/^/X/
        x
    }
    x
}
