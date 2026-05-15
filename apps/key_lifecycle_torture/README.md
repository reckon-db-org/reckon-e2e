# key_lifecycle_torture

Key rotation / absence / invalidation torture.

Scoped — see `docs/TORTURE_TAXONOMY.md#key_lifecycle_torture`. No Erlang code yet.

Planned scenarios:
- Hot rotation: writers continue across a key-change boundary; verify reads with old key on pre-rotation events, new key on post-rotation events
- Missing key: start a store with no key, then enable integrity mid-flight; verify lazy enablement watermark behaviour
- Invalid key on read: deliberately load the wrong key into the reader; verify all post-rotation events surface integrity violations
