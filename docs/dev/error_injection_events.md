# Error Injection Event Stream Implementation

## Overview

This implementation adds Server-Sent Events (SSE) support for error injection points, allowing tests to wait for injections to be triggered without log parsing.

## Architecture

### Backend (C++)

#### 1. Event Notification System (`utils/error_injection.hh`)

- **Callback Type**: `error_injection_event_callback` - function signature: `void(std::string_view injection_name, std::string_view injection_type)`
- **Storage**: Thread-local vector of callbacks (`_event_callbacks`)
- **Notification**: When any `inject()` method is called, `notify_event()` triggers all registered callbacks
- **Thread Safety**: Each shard has its own error_injection instance with its own callbacks
- **Cross-Shard**: Static methods use `smp::invoke_on_all()` to register callbacks on all shards

#### 2. SSE Endpoint (`api/error_injection.cc`)

```
GET /v2/error_injection/events
Content-Type: text/event-stream
```

**Flow**:
1. Client connects to SSE endpoint
2. Server creates a queue on the current shard
3. Callback registered on ALL shards that forwards events to this queue (using `smp::submit_to`)
4. Server streams events in SSE format: `data: {"injection":"name","type":"handler","shard":0}\n\n`
5. On disconnect (client closes or exception), callbacks are cleaned up

**Event Format**:
```json
{
  "injection": "injection_name",
  "type": "sleep|handler|exception|lambda",
  "shard": 0
}
```

### Python Client (`test/pylib/rest_client.py`)

#### InjectionEventStream Class

```python
async with injection_event_stream(node_ip) as stream:
    event = await stream.wait_for_injection("my_injection", timeout=30)
```

**Features**:
- Async context manager for automatic connection/disconnection
- Background task reads SSE events
- Queue-based event delivery
- `wait_for_injection()` method filters events by injection name

## Usage Examples

### Basic Usage

```python
async with injection_event_stream(server_ip) as event_stream:
    # Enable injection
    await api.enable_injection(server_ip, "my_injection", one_shot=True)
    
    # Trigger operation that hits injection
    # ... some operation ...
    
    # Wait for injection without log parsing!
    event = await event_stream.wait_for_injection("my_injection", timeout=30)
    logger.info(f"Injection hit on shard {event['shard']}")
```

### Old vs New Approach

**Old (Log Parsing)**:
```python
log = await manager.server_open_log(server_id)
mark = await log.mark()
await api.enable_injection(ip, "my_injection", one_shot=True)
# ... operation ...
mark, _ = await log.wait_for('my_injection: waiting', from_mark=mark)
```

**New (Event Stream)**:
```python
async with injection_event_stream(ip) as stream:
    await api.enable_injection(ip, "my_injection", one_shot=True)
    # ... operation ...
    event = await stream.wait_for_injection("my_injection", timeout=30)
```

## Benefits

1. **Performance**: No waiting for log flushes or buffer processing
2. **Reliability**: Direct event notifications, no regex matching failures
3. **Simplicity**: Clean async/await pattern
4. **Flexibility**: Can wait for multiple injections, get event metadata
5. **Backward Compatible**: Existing log-based tests continue to work

## Implementation Notes

### Thread Safety
- Each shard has independent error_injection instance
- Events from any shard are delivered to SSE client via `smp::submit_to`
- Queue operations are shard-local, avoiding cross-shard synchronization

### Cleanup
- Client disconnect triggers callback cleanup on all shards
- Cleanup happens automatically via RAII (try/finally in stream function)
- No callback leaks even if client disconnects abruptly

### Logging
- Injection triggers now log at INFO level (was DEBUG)
- This ensures events are visible in logs AND via SSE
- SSE provides machine-readable events, logs provide human-readable context

## Testing

See `test/cluster/test_error_injection_events.py` for example tests:
- `test_injection_event_stream_basic`: Basic functionality
- `test_injection_event_stream_multiple_injections`: Multiple injection tracking
- `test_injection_event_vs_log_parsing_comparison`: Old vs new comparison

## Future Enhancements

Possible improvements:
1. Filter events by injection name at server side (query parameter)
2. Include injection parameters in events
3. Add event timestamps
4. Support for event history/replay
5. WebSocket support (if bidirectional communication needed)
