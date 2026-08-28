## Problem

During normal Agent usage, the flow silently stops mid-conversation. The AI says "now I'll read the files" and then never continues. No tool call, no more text, no error. It just freezes.

## Root Causes

### 1. Actor not emitting end-stream on channel close (actor.rs)
When the command channel closes or receives Abort, the actor only called handle.cancel() which sets the cancellation token but never emits an end-stream frame. The SSE stream hangs indefinitely.

### 2. lifecycle encoding failure skips close_output() (lifecycle.rs)
Both fail() and cancel() used the ? operator on encode_error_end_stream(), meaning if serialization fails, close_output() is never called. The stream stays open silently.

### 3. Context/blob sync timeouts too aggressive (context_sync.rs, blob_sync.rs)
Hardcoded 15-second timeouts kill sessions silently on slow networks or with proxy configurations.

### 4. Bash tool not recognized (dispatch/mod.rs)
Cursor IDE sends "Bash" tool calls but the server only recognized "Shell", causing "unsupported tool: Bash" errors.

## Changes

- actor.rs: Use lifecycle::cancel() instead of handle.cancel()
- lifecycle.rs: Use match instead of ? with fallback end-stream frame
- sessions.rs: Document correct Notify pattern
- context_sync.rs: 15s to 60s timeout
- blob_sync.rs: 15s to 60s timeout (SET and GET)
- dispatch/mod.rs: Add Bash tool alias for Shell
- openai_chat.rs: Add detailed debug logging
- router.rs: Add detailed debug logging

## Testing

- All existing tests pass
- cargo check compiles cleanly
- Both debug and release builds succeed
- Tested with Cursor BYOK desktop app