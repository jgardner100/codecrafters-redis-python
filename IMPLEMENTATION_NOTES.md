# XREAD Indefinite Blocking Implementation

## Overview
This document describes the implementation of indefinite blocking support for the XREAD command in the Redis server implementation.

## Feature Description
The XREAD command now supports blocking indefinitely when `BLOCK 0` is specified. The client will wait without timing out until new data is added to the specified stream(s).

## Implementation Details

### 1. Timeout Value Conversion
**Location:** `app/main.py`, lines 1107-1113

The critical change is in how the timeout value is converted:

```python
if block_timeout_ms == 0:
    block_timeout = None  # Pass None to event.wait() for indefinite blocking
else:
    block_timeout = block_timeout_ms / 1000.0
```

**Why this works:**
- Python's `threading.Event.wait(timeout=None)` blocks indefinitely
- When `timeout=None`, the thread waits forever or until the event is set
- For any non-zero value, the timeout is converted from milliseconds to seconds

### 2. Event Waiting
**Location:** `app/main.py`, line 1228

```python
event.wait(timeout=block_timeout)
```

This single call handles all cases:
- `timeout=None` → waits indefinitely (BLOCK 0)
- `timeout=float` → waits that many seconds (BLOCK N where N > 0)

### 3. Notification Mechanism
**Location:** `app/main.py`, lines 328-342

The existing `notify_blocked_streams()` function wakes up blocked clients when new data arrives:

```python
def notify_blocked_streams(key):
    # Finds all waiting clients for the stream key
    # Sets their events to wake them up
    # Clients then re-read and return new data
```

## Test Case Walkthrough

### Setup Phase
1. Tester executes: `redis-cli XADD stream_key 0-1 temperature 96`
   - Creates a stream with one entry

### Blocking Phase
2. Tester sends: `redis-cli XREAD BLOCK 0 STREAMS stream_key 0-1`
   - Server parses: `block_timeout_ms = 0`
   - Conversion: `block_timeout = None`
   - No data matches criteria (ID must be > 0-1)
   - Client registers for blocking: `blocked_streams['stream_key'].append((conn, event))`
   - Calls: `event.wait(timeout=None)` → **waits indefinitely**

### Waiting Phase
3. Tester pauses for 1000 milliseconds
   - Client is still blocked (timeout=None, so no timeout)
   - No response sent back yet

### Unblocking Phase
4. Tester adds new data: `redis-cli XADD stream_key 0-2 temperature 95`
   - Server creates entry with ID 0-2
   - Calls: `notify_blocked_streams('stream_key')`
   - This sets the event: `event.set()`
   - Blocked client wakes up: `event.is_set()` returns `True`

### Response Phase
5. Client re-reads the stream:
   - Looks for entries with ID > 0-1
   - Finds entry 0-2
   - Encodes response using `encode_streams_response()`
   - Sends RESP-formatted response:
     ```
     [
       [
         "stream_key",
         [
           [
             "0-2",
             [
               "temperature",
               "95"
             ]
           ]
         ]
       ]
     ]
     ```

## Behavior Summary

| Command | Behavior |
|---------|----------|
| `XREAD BLOCK 0 STREAMS key id` | Blocks indefinitely until new data arrives |
| `XREAD BLOCK N STREAMS key id` (N > 0) | Blocks for up to N milliseconds |
| `XREAD STREAMS key id` (no BLOCK) | Returns immediately with available data or null |

## Thread Safety

The implementation maintains thread safety through:

1. **Locks on shared structures:**
   - `store_lock`: Protects the data store
   - `blocked_streams_lock`: Protects the blocked clients registry

2. **Event-based synchronization:**
   - Each blocked client has its own threading.Event
   - Events are signaled only when data becomes available
   - No busy-polling or race conditions

## Backward Compatibility

The change is fully backward compatible:
- Existing code with BLOCK N (N > 0) works unchanged
- Commands without BLOCK keyword are unaffected
- The change only affects the specific case of BLOCK 0

## Files Modified

- `app/main.py`: Updated XREAD command handler with indefinite blocking support

## Testing

The implementation passes the required test case:
1. ✓ Correctly blocks indefinitely on XREAD BLOCK 0
2. ✓ Remains blocked during the 1000ms pause
3. ✓ Wakes up immediately when new data arrives
4. ✓ Returns the new data with proper RESP encoding
5. ✓ No syntax errors or runtime issues

## Edge Cases Handled

1. **Multiple clients blocking on same stream:** Each gets its own event and is notified separately
2. **Multiple streams in XREAD:** Correctly handles BLOCK 0 with multiple streams
3. **Stream doesn't exist yet:** Client blocks and wakes when stream is created
4. **Data exists before XREAD:** Returns immediately without blocking
5. **Timeout expires (non-zero BLOCK):** Returns null array correctly
