#!/Users/johng/bin/python
#!/usr/bin/python

import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


# Global dictionary to store key-value pairs
store = {}
# Dictionary to store expiry times (in absolute time)
expiry_times = {}
store_lock = threading.Lock()

# Dictionary to track blocked clients waiting for lists
# Format: {key: [(connection, event), ...]}
blocked_clients = {}
blocked_clients_lock = threading.Lock()

# Dictionary to track blocked clients waiting for streams (XREAD BLOCK)
# Format: {key: [(connection, event, context), ...]} where context includes stream_id_str
blocked_streams = {}
blocked_streams_lock = threading.Lock()

# Dictionary to track transaction state per connection
# Format: {connection_id: {"in_transaction": bool, "queued_commands": [...]}}
transaction_states = {}
transaction_states_lock = threading.Lock()

# Dictionary to track watched keys per connection
# Format: {connection_id: set of watched keys}
watched_keys = {}
watched_keys_lock = threading.Lock()

# Metrics tracking
start_time = time.time()
active_connections = 0
connections_lock = threading.Lock()


def parse_resp(data):
    """
    Parse RESP protocol data and return a list of strings.
    Handles RESP Arrays.
    """
    data_str = data.decode('utf-8', errors='ignore')
    lines = data_str.split('\r\n')
    
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        if not line:
            i += 1
            continue
        
        if line.startswith('*'):
            # Array
            array_length = int(line[1:])
            i += 1
            for _ in range(array_length):
                # Skip empty lines
                while i < len(lines) and not lines[i]:
                    i += 1
                
                if i < len(lines) and lines[i].startswith('$'):
                    # Bulk string
                    bulk_length = int(lines[i][1:])
                    i += 1
                    # Skip empty lines
                    while i < len(lines) and not lines[i]:
                        i += 1
                    if i < len(lines):
                        result.append(lines[i])
                        i += 1
                elif i < len(lines) and lines[i].startswith('+'):
                    # Simple string
                    result.append(lines[i][1:])
                    i += 1
        elif line.startswith('$'):
            # Bulk string
            bulk_length = int(line[1:])
            i += 1
            if i < len(lines):
                result.append(lines[i])
                i += 1
        elif line.startswith('+'):
            # Simple string
            result.append(lines[i][1:])
            i += 1
        else:
            i += 1
    
    return result


def encode_bulk_string(s):
    """Encode a string as a RESP bulk string."""
    if isinstance(s, str):
        s_bytes = s.encode('utf-8')
    elif isinstance(s, bytes):
        s_bytes = s
    else:
        # Convert to string first
        s_bytes = str(s).encode('utf-8')
    
    return f"${len(s_bytes)}\r\n{s_bytes.decode('utf-8')}\r\n".encode('utf-8')


def encode_null_bulk_string():
    """Encode a null bulk string as per RESP protocol."""
    return b"$-1\r\n"


def encode_null_array():
    """Encode a null array as per RESP protocol."""
    return b"*-1\r\n"


def encode_integer(num):
    """Encode an integer as a RESP integer."""
    return f":{num}\r\n".encode('utf-8')


def encode_simple_string(s):
    """Encode a string as a RESP simple string."""
    return f"+{s}\r\n".encode('utf-8')


def encode_array(elements):
    """Encode a list of strings as a RESP array."""
    if not elements:
        return b"*0\r\n"
    
    response = f"*{len(elements)}\r\n".encode('utf-8')
    for element in elements:
        response += encode_bulk_string(element)
    
    return response


def encode_nested_array(nested_elements):
    """
    Encode a nested array (array of arrays) as a RESP array.
    Each element in nested_elements should be a list.
    
    For XRANGE response, each element should be [entry_id, [k1, v1, k2, v2, ...]].
    The inner array should be properly encoded as an array, not a string.
    """
    if not nested_elements:
        return b"*0\r\n"
    
    response = b""
    response += f"*{len(nested_elements)}\r\n".encode('utf-8')
    
    for element_list in nested_elements:
        if isinstance(element_list, list):
            # Encode the inner array with 2 elements: [id, [kv_pairs]]
            # element_list[0] is the entry_id (string)
            # element_list[1] is the kv_list (list of strings)
            response += b"*2\r\n"  # Array with 2 elements
            
            # First element: entry_id (bulk string)
            response += encode_bulk_string(element_list[0])
            
            # Second element: kv_list (array of bulk strings)
            kv_array = element_list[1]
            response += f"*{len(kv_array)}\r\n".encode('utf-8')
            for kv_item in kv_array:
                response += encode_bulk_string(kv_item)
        else:
            # Fallback for non-list elements
            response += encode_bulk_string(str(element_list))
    
    return response


def encode_streams_response(streams_data):
    """
    Encode a streams response for XREAD.
    
    streams_data is a list of [stream_key, entries] pairs
    where entries is a list of [entry_id, [k1, v1, k2, v2, ...]] pairs.
    
    Format:
    *<number of streams>\r\n
      *2\r\n
        $<length>\r\n<stream_key>\r\n
        *<number of entries>\r\n
          *2\r\n
            $<length>\r\n<entry_id>\r\n
            *<number of kv pairs>\r\n
              $<length>\r\n<key>\r\n
              $<length>\r\n<value>\r\n
              ...
    """
    if not streams_data:
        return encode_null_array()
    
    response = b""
    response += f"*{len(streams_data)}\r\n".encode('utf-8')
    
    for stream_key, entries in streams_data:
        # Each stream is [stream_key, [entries]]
        response += b"*2\r\n"
        
        # First: stream key
        response += encode_bulk_string(stream_key)
        
        # Second: array of entries
        response += f"*{len(entries)}\r\n".encode('utf-8')
        for entry_id, kv_list in entries:
            # Each entry is [entry_id, [k1, v1, ...]]
            response += b"*2\r\n"
            
            # Entry ID
            response += encode_bulk_string(entry_id)
            
            # Key-value pairs
            response += f"*{len(kv_list)}\r\n".encode('utf-8')
            for kv_item in kv_list:
                response += encode_bulk_string(kv_item)
    
    return response


def encode_error(error_message):
    """Encode an error message as a RESP simple error."""
    return f"-{error_message}\r\n".encode('utf-8')


def encode_response(response_obj):
    """
    Encode various response types into RESP format.
    
    response_obj can be:
    - bytes: raw RESP response
    - tuple: (type, data) where type is 'simple', 'integer', 'bulk', 'array', 'error', 'null'
    - string: treated as a bulk string
    - integer: treated as an integer response
    """
    if isinstance(response_obj, bytes):
        return response_obj
    elif isinstance(response_obj, tuple):
        resp_type, data = response_obj
        if resp_type == 'simple':
            return encode_simple_string(data)
        elif resp_type == 'integer':
            return encode_integer(data)
        elif resp_type == 'bulk':
            if data is None:
                return encode_null_bulk_string()
            else:
                return encode_bulk_string(data)
        elif resp_type == 'array':
            if data is None:
                return encode_null_array()
            else:
                return encode_array(data)
        elif resp_type == 'error':
            return encode_error(data)
        elif resp_type == 'null':
            return encode_null_bulk_string()
    elif isinstance(response_obj, str):
        return encode_bulk_string(response_obj)
    elif isinstance(response_obj, int):
        return encode_integer(response_obj)
    
    # Default fallback
    return encode_bulk_string(str(response_obj))


def execute_command(command_parts):
    """
    Execute a single command and return the response as a bytes object (raw RESP).
    
    Returns bytes suitable for sending directly to the client.
    """
    if not command_parts:
        return encode_error("ERR empty command")
    
    command = command_parts[0].upper()
    
    if command == "PING":
        return b"+PONG\r\n"
    elif command == "ECHO":
        if len(command_parts) > 1:
            return encode_bulk_string(command_parts[1])
        else:
            return b"-ERR wrong number of arguments for 'echo' command\r\n"
    elif command == "WATCH":
        if len(command_parts) < 2:
            return b"-ERR wrong number of arguments for 'watch' command\r\n"
        
        # For this stage, we only need to parse and respond with +OK
        # In later stages, we'll implement the actual watch tracking
        return b"+OK\r\n"
    elif command == "SET":
        if len(command_parts) < 3:
            return b"-ERR wrong number of arguments for 'set' command\r\n"
        
        key = command_parts[1]
        value = command_parts[2]
        expiry_ms = None
        
        # Parse optional arguments
        i = 3
        while i < len(command_parts):
            option = command_parts[i].upper()
            if option == "EX":
                if i + 1 < len(command_parts):
                    try:
                        seconds = int(command_parts[i + 1])
                        expiry_ms = seconds * 1000
                        i += 2
                    except ValueError:
                        return b"-ERR invalid expire time in set\r\n"
                else:
                    return b"-ERR syntax error\r\n"
            elif option == "PX":
                if i + 1 < len(command_parts):
                    try:
                        expiry_ms = int(command_parts[i + 1])
                        i += 2
                    except ValueError:
                        return b"-ERR invalid expire time in set\r\n"
                else:
                    return b"-ERR syntax error\r\n"
            else:
                i += 1
        
        with store_lock:
            store[key] = value
            if expiry_ms is not None:
                expiry_times[key] = time.time() * 1000 + expiry_ms
            else:
                expiry_times.pop(key, None)
        
        return b"+OK\r\n"
    elif command == "GET":
        if len(command_parts) < 2:
            return b"-ERR wrong number of arguments for 'get' command\r\n"
        
        key = command_parts[1]
        with store_lock:
            if is_key_expired(key):
                value = None
            else:
                value = store.get(key)
        
        if value is None:
            return encode_null_bulk_string()
        else:
            return encode_bulk_string(value)
    elif command == "INCR":
        if len(command_parts) < 2:
            return b"-ERR wrong number of arguments for 'incr' command\r\n"
        
        key = command_parts[1]
        
        with store_lock:
            if is_key_expired(key):
                value = None
            else:
                value = store.get(key)
            
            if value is None:
                store[key] = "1"
                new_value = 1
                expiry_times.pop(key, None)
            else:
                try:
                    int_value = int(value)
                    new_value = int_value + 1
                    store[key] = str(new_value)
                except ValueError:
                    return b"-ERR value is not an integer or out of range\r\n"
        
        return encode_integer(new_value)
    elif command == "TYPE":
        if len(command_parts) < 2:
            return b"-ERR wrong number of arguments for 'type' command\r\n"
        
        key = command_parts[1]
        key_type = get_key_type(key)
        return encode_simple_string(key_type)
    elif command == "RPUSH":
        if len(command_parts) < 3:
            return b"-ERR wrong number of arguments for 'rpush' command\r\n"
        
        key = command_parts[1]
        elements = command_parts[2:]
        
        with store_lock:
            if is_key_expired(key):
                store[key] = []
            
            if key not in store:
                store[key] = []
            
            current_value = store[key]
            
            if isinstance(current_value, list):
                for element in elements:
                    current_value.append(element)
                list_length = len(current_value)
            else:
                return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        
        notify_blocked_clients(key, elements[0])
        return encode_integer(list_length)
    elif command == "LPUSH":
        if len(command_parts) < 3:
            return b"-ERR wrong number of arguments for 'lpush' command\r\n"
        
        key = command_parts[1]
        elements = command_parts[2:]
        
        with store_lock:
            if is_key_expired(key):
                store[key] = []
            
            if key not in store:
                store[key] = []
            
            current_value = store[key]
            
            if isinstance(current_value, list):
                for element in elements:
                    current_value.insert(0, element)
                list_length = len(current_value)
            else:
                return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        
        notify_blocked_clients(key, elements[0])
        return encode_integer(list_length)
    elif command == "LRANGE":
        if len(command_parts) < 4:
            return b"-ERR wrong number of arguments for 'lrange' command\r\n"
        
        key = command_parts[1]
        
        try:
            start = int(command_parts[2])
            stop = int(command_parts[3])
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
        
        with store_lock:
            if is_key_expired(key):
                value = None
            else:
                value = store.get(key)
        
        if value is None or not isinstance(value, list):
            return encode_array([])
        else:
            list_data = value
            list_length = len(list_data)
            
            start = normalize_index(start, list_length)
            stop = normalize_index(stop, list_length)
            
            if start >= list_length or start > stop:
                return encode_array([])
            else:
                if stop >= list_length:
                    stop = list_length - 1
                
                result = list_data[start:stop + 1]
                return encode_array(result)
    elif command == "LLEN":
        if len(command_parts) < 2:
            return b"-ERR wrong number of arguments for 'llen' command\r\n"
        
        key = command_parts[1]
        
        with store_lock:
            if is_key_expired(key):
                value = None
            else:
                value = store.get(key)
        
        if value is None or not isinstance(value, list):
            return encode_integer(0)
        else:
            return encode_integer(len(value))
    elif command == "LPOP":
        if len(command_parts) < 2:
            return b"-ERR wrong number of arguments for 'lpop' command\r\n"
        
        key = command_parts[1]
        count = None
        
        if len(command_parts) >= 3:
            try:
                count = int(command_parts[2])
                if count < 0:
                    return b"-ERR count should be a positive integer\r\n"
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
        
        with store_lock:
            if is_key_expired(key):
                value = None
            else:
                value = store.get(key)
            
            if value is None or not isinstance(value, list):
                if count is None:
                    return encode_null_bulk_string()
                else:
                    return encode_array([])
            else:
                if len(value) == 0:
                    if count is None:
                        return encode_null_bulk_string()
                    else:
                        return encode_array([])
                else:
                    if count is None:
                        element = value.pop(0)
                        return encode_bulk_string(element)
                    else:
                        elements_to_remove = min(count, len(value))
                        removed_elements = []
                        for _ in range(elements_to_remove):
                            removed_elements.append(value.pop(0))
                        return encode_array(removed_elements)
    elif command == "BLPOP":
        if len(command_parts) < 3:
            return encode_error("ERR wrong number of arguments for 'blpop' command")

        keys = command_parts[1:-1]
        try:
            timeout = float(command_parts[-1])
        except ValueError:
            return encode_error("ERR timeout is not a float or out of range")

        if timeout < 0:
            return encode_error("ERR timeout is negative")

        # Fast path: check if any key already has an element
        with store_lock:
            for key in keys:
                if is_key_expired(key):
                    continue
                val = store.get(key)
                if isinstance(val, list) and len(val) > 0:
                    element = val.pop(0)
                    return encode_array([key, element])

        event = threading.Event()
        waiter = (threading.current_thread(), event)

        with blocked_clients_lock:
            for key in keys:
                blocked_clients.setdefault(key, []).append(waiter)

        woken = event.wait(timeout=timeout if timeout > 0 else None)

        with blocked_clients_lock:
            for key in keys:
                if key in blocked_clients:
                    blocked_clients[key] = [
                        w for w in blocked_clients[key] if w is not waiter
                    ]
                    if not blocked_clients[key]:
                        del blocked_clients[key]

        if not woken:
            return encode_null_array()

        with store_lock:
            for key in keys:
                if is_key_expired(key):
                    continue
                val = store.get(key)
                if isinstance(val, list) and len(val) > 0:
                    element = val.pop(0)
                    return encode_array([key, element])

        return encode_null_array()
    elif command == "XADD":
        if len(command_parts) < 4:
            return encode_error("ERR wrong number of arguments for 'xadd' command")
        
        key = command_parts[1]
        stream_id_str = command_parts[2]
        field_values = command_parts[3:]
        
        # Field-value pairs must be even
        if len(field_values) % 2 != 0:
            return encode_error("ERR wrong number of arguments for 'xadd' command")
        
        with store_lock:
            # Initialize stream if it doesn't exist
            if key not in store:
                store[key] = {
                    "_stream_entries": [],  # List of (entry_id, kv_dict)
                    "_last_id": None
                }
            
            stream_obj = store[key]
            
            # Validate it's actually a stream
            if not isinstance(stream_obj, dict) or "_stream_entries" not in stream_obj:
                return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
            
            # Get the last ID for validation
            last_id_str = stream_obj.get("_last_id")
            
            # Validate and generate the final stream ID
            is_valid, error_msg, final_id = is_stream_id_valid(stream_id_str, last_id_str)
            
            if not is_valid:
                return encode_error(error_msg)
            
            # Build the kv dictionary from pairs
            kv_dict = {}
            for i in range(0, len(field_values), 2):
                field = field_values[i]
                value = field_values[i + 1]
                kv_dict[field] = value
            
            # Add entry to stream
            stream_obj["_stream_entries"].append((final_id, kv_dict))
            stream_obj["_last_id"] = final_id
            
            # Notify any blocked XREAD clients
            notify_blocked_streams(key)
        
        return encode_bulk_string(final_id)
    elif command == "XRANGE":
        if len(command_parts) < 4:
            return encode_error("ERR wrong number of arguments for 'xrange' command")
        
        key = command_parts[1]
        start_str = command_parts[2]
        end_str = command_parts[3]
        
        # Parse optional COUNT argument
        count = None
        if len(command_parts) >= 6 and command_parts[4].upper() == "COUNT":
            try:
                count = int(command_parts[5])
            except ValueError:
                return encode_error("ERR value is not an integer or out of range")
        
        with store_lock:
            if key not in store:
                return encode_array([])
            
            stream_obj = store[key]
            if not isinstance(stream_obj, dict) or "_stream_entries" not in stream_obj:
                return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
            
            entries = stream_obj["_stream_entries"]
            
            # Parse start and end IDs
            try:
                # For start: "-" means minimum (0-0), explicit IDs or partial times
                if start_str == "-":
                    start_ms, start_seq = 0, 0
                else:
                    start_ms, start_seq = parse_range_id(start_str)
                    if start_seq is None:
                        # Partial range, default to 0 for start
                        start_seq = 0
                
                # For end: "+" means maximum
                if end_str == "+":
                    end_ms, end_seq = 2**63 - 1, 2**63 - 1
                else:
                    end_ms, end_seq = parse_range_id(end_str)
                    if end_seq is None:
                        # Partial range, default to max for end
                        end_seq = 2**63 - 1
            except ValueError:
                return encode_error("ERR invalid range")
            
            # Filter entries in range
            result = []
            for entry_id, kv_dict in entries:
                entry_ms, entry_seq = parse_stream_id(entry_id)
                
                # Check if entry is within range (inclusive)
                if compare_stream_ids(entry_ms, entry_seq, start_ms, start_seq) >= 0 and \
                   compare_stream_ids(entry_ms, entry_seq, end_ms, end_seq) <= 0:
                    # Build the kv array [field1, value1, field2, value2, ...]
                    kv_array = []
                    for field, value in kv_dict.items():
                        kv_array.append(field)
                        kv_array.append(value)
                    
                    result.append([entry_id, kv_array])
            
            # Apply COUNT limit if specified
            if count is not None and count > 0:
                result = result[:count]
            
            return encode_nested_array(result)
    elif command == "XREAD":
        # XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
        upper_parts = [part.upper() for part in command_parts]

        # Find where "STREAMS" keyword is (case-insensitive)
        try:
            streams_idx = upper_parts.index("STREAMS")
        except ValueError:
            return encode_error("ERR syntax error")
        
        # Parse options before STREAMS
        count = None
        block_ms = None
        i = 1
        while i < streams_idx:
            option = upper_parts[i]
            if option == "COUNT":
                if i + 1 < streams_idx:
                    try:
                        count = int(command_parts[i + 1])
                        i += 2
                    except ValueError:
                        return encode_error("ERR value is not an integer or out of range")
                else:
                    return encode_error("ERR syntax error")
            elif option == "BLOCK":
                if i + 1 < streams_idx:
                    try:
                        block_ms = int(command_parts[i + 1])
                        i += 2
                    except ValueError:
                        return encode_error("ERR value is not an integer or out of range")
                else:
                    return encode_error("ERR syntax error")
            else:
                i += 1
        
        # Parse streams and IDs
        stream_keys = []
        stream_ids = []
        
        args_after_streams = command_parts[streams_idx + 1:]
        num_args = len(args_after_streams)
        
        # Split into two equal parts: keys and IDs
        if num_args % 2 != 0:
            return encode_error("ERR syntax error")
        
        num_streams = num_args // 2
        stream_keys = args_after_streams[:num_streams]
        stream_ids = args_after_streams[num_streams:]
        
        # Store the last entry count for each stream before blocking
        with store_lock:
            initial_state = {}
            for stream_key in stream_keys:
                if stream_key in store:
                    stream_obj = store[stream_key]
                    if isinstance(stream_obj, dict) and "_stream_entries" in stream_obj:
                        # For $ marker: store the number of entries so we can return only new ones
                        initial_state[stream_key] = len(stream_obj["_stream_entries"])
                    else:
                        initial_state[stream_key] = 0
                else:
                    initial_state[stream_key] = 0
        
        # First attempt to read from streams (without blocking)
        with store_lock:
            result = []
            has_data = False
            
            for stream_key, stream_id_str in zip(stream_keys, stream_ids):
                if stream_id_str == "$":
                    # $ means "entries that arrive after this point"
                    # So we don't return any data now, we'll block and wait for new entries
                    continue
                
                if stream_key not in store:
                    # Stream doesn't exist yet
                    continue
                
                stream_obj = store[stream_key]
                if not isinstance(stream_obj, dict) or "_stream_entries" not in stream_obj:
                    return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
                
                entries = stream_obj["_stream_entries"]
                
                # Parse the ID and get matching entries
                matching_entries = get_stream_entries_after_id(entries, stream_id_str)
                
                # Apply COUNT limit
                if count is not None and count > 0:
                    matching_entries = matching_entries[:count]
                
                if matching_entries:
                    result.append([stream_key, matching_entries])
                    has_data = True
        
        # If we have results and no $ markers, return them immediately
        if has_data and "$" not in stream_ids:
            return encode_streams_response(result)
        
        # If BLOCK is specified, we should block (either no initial data or $ marker present)
        if block_ms is not None:
            # Block and wait for new data
            event = threading.Event()
            conn_id = id(threading.current_thread())
            context = {
                'stream_keys': stream_keys,
                'stream_ids': stream_ids,
                'count': count,
                'initial_state': initial_state,
                'event': event
            }
            
            # Register this client as blocked on these streams
            with blocked_streams_lock:
                for stream_key in stream_keys:
                    if stream_key not in blocked_streams:
                        blocked_streams[stream_key] = []
                    blocked_streams[stream_key].append((conn_id, context))
            
            # Wait for event or timeout
            event.wait(timeout=block_ms / 1000.0 if block_ms > 0 else None)
            
            # Clean up
            with blocked_streams_lock:
                for stream_key in stream_keys:
                    if stream_key in blocked_streams:
                        blocked_streams[stream_key] = [(c, ctx) for c, ctx in blocked_streams[stream_key] if c != conn_id]
                        if not blocked_streams[stream_key]:
                            del blocked_streams[stream_key]
            
            # Try to read again after unblocking
            with store_lock:
                result = []
                
                for stream_key, stream_id_str in zip(stream_keys, stream_ids):
                    if stream_key not in store:
                        continue
                    
                    stream_obj = store[stream_key]
                    if not isinstance(stream_obj, dict) or "_stream_entries" not in stream_obj:
                        return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
                    
                    entries = stream_obj["_stream_entries"]
                    
                    # For $ marker: get entries that were added after blocking started
                    if stream_id_str == "$":
                        # Return entries that were added while we were blocked
                        initial_count = initial_state.get(stream_key, 0)
                        matching_entries = []
                        for idx, (entry_id, kv_dict) in enumerate(entries):
                            if idx >= initial_count:
                                # This entry was added while we were blocked
                                kv_array = []
                                for field, value in kv_dict.items():
                                    kv_array.append(field)
                                    kv_array.append(value)
                                matching_entries.append([entry_id, kv_array])
                    else:
                        # Get matching entries
                        matching_entries = get_stream_entries_after_id(entries, stream_id_str)
                    
                    # Apply COUNT limit
                    if count is not None and count > 0:
                        matching_entries = matching_entries[:count]
                    
                    if matching_entries:
                        result.append([stream_key, matching_entries])
            
            if result:
                return encode_streams_response(result)
            else:
                # Even after blocking, if no data, return null array
                return encode_null_array()
        
        # No results and no blocking requested
        if result:
            return encode_streams_response(result)
        else:
            return encode_null_array()
    else:
        return encode_error("ERR unknown command")


def get_stream_entries_after_id(entries, stream_id_str):
    """
    Get entries from a stream that come after the specified ID.
    
    If stream_id_str is "$", returns empty list (entries that come after the last one).
    If stream_id_str is "-", returns all entries.
    Otherwise returns entries strictly after the specified ID.
    """
    if stream_id_str == "$":
        # $ means entries that arrive after this read is initiated
        # So return empty list - new entries will trigger blocking
        return []
    elif stream_id_str == "-":
        # - means all entries
        result = []
        for entry_id, kv_dict in entries:
            kv_array = []
            for field, value in kv_dict.items():
                kv_array.append(field)
                kv_array.append(value)
            result.append([entry_id, kv_array])
        return result
    else:
        # Parse the ID and return entries after it
        try:
            start_ms, start_seq = parse_stream_id(stream_id_str)
            if start_ms is None:
                return []
            
            matching_entries = []
            for entry_id, kv_dict in entries:
                entry_ms, entry_seq = parse_stream_id(entry_id)
                
                # Include entries strictly greater than the given ID
                if compare_stream_ids(entry_ms, entry_seq, start_ms, start_seq) > 0:
                    kv_array = []
                    for field, value in kv_dict.items():
                        kv_array.append(field)
                        kv_array.append(value)
                    matching_entries.append([entry_id, kv_array])
            
            return matching_entries
        except:
            return []


def is_key_expired(key):
    """Check if a key has expired. Remove it if it has."""
    if key not in expiry_times:
        return False
    
    current_time = time.time() * 1000  # Convert to milliseconds
    expiry_time = expiry_times[key]
    
    if current_time >= expiry_time:
        # Key has expired, remove it
        store.pop(key, None)
        expiry_times.pop(key, None)
        return True
    
    return False


def get_key_type(key):
    """
    Get the type of the value stored at a key.
    Returns: "string", "list", "set", "zset", "hash", "stream", "vectorset", or "none"
    For now, only handle "string" and "none".
    """
    with store_lock:
        # Check if key has expired
        if is_key_expired(key):
            return "none"
        
        if key not in store:
            return "none"
        
        value = store[key]
        
        if isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "list"
        elif isinstance(value, set):
            return "set"
        elif isinstance(value, dict):
            # Check if it's a stream (has a special marker)
            if "_stream_entries" in value:
                return "stream"
            # Otherwise treat as hash
            return "hash"
        else:
            # Default to string for unknown types
            return "string"


def normalize_index(index, list_length):
    """
    Convert an index (possibly negative) to a positive index.
    Handles out-of-range negative indexes by treating them as 0.
    """
    if index < 0:
        # Negative index: convert from end
        positive_index = list_length + index
        # If out of range, treat as 0
        if positive_index < 0:
            return 0
        return positive_index
    else:
        # Positive index: return as-is
        return index


def notify_blocked_clients(key, element):
    """
    Notify the first blocked client waiting for a key that an element is available.
    Returns True if a client was notified, False otherwise.
    """
    with blocked_clients_lock:
        if key in blocked_clients and len(blocked_clients[key]) > 0:
            # Get the first (oldest) blocked client
            conn, event = blocked_clients[key].pop(0)
            # Clean up empty lists
            if len(blocked_clients[key]) == 0:
                del blocked_clients[key]
            # Signal the event to wake up the blocked client
            event.set()
            return True
    return False


def notify_blocked_streams(key):
    """
    Notify all blocked clients waiting for a stream that new data is available.
    Returns the number of clients notified.
    """
    notified_count = 0
    with blocked_streams_lock:
        if key in blocked_streams and len(blocked_streams[key]) > 0:
            # Get all blocked clients for this key
            to_notify = blocked_streams[key][:]
            # Clear the list
            blocked_streams[key] = []
            del blocked_streams[key]
            
            # Signal all waiting clients
            for conn, context in to_notify:
                context['event'].set()
                notified_count += 1
    
    return notified_count


def generate_stream_id(current_timestamp_ms, entry_count_at_ms):
    """
    Generate a stream ID in the format: timestamp-sequence
    where timestamp is in milliseconds and sequence is the entry count at that timestamp.
    """
    return f"{current_timestamp_ms}-{entry_count_at_ms}"


def parse_stream_id(stream_id_str):
    """
    Parse a stream ID string into (milliseconds_time, sequence_number).
    Returns (milliseconds_time, sequence_number) or (None, None) if invalid format.
    
    Handles:
    - Explicit format: "1526919030473-0" (both parts are numbers)
    - Auto-sequence format: "1526919030474-*" (time part is number, sequence is *)
    - Full auto-generation: "*" (entire ID is auto-generated)
    - Special "$" marker for XREAD (indicates read from latest)
    """
    # Handle special $ marker for XREAD
    if stream_id_str == "$":
        return "\\$", "\\$"  # Return special marker to indicate $
    
    # Handle full auto-generation case
    if stream_id_str == "*":
        # Return None, None to indicate full auto-generation needed
        return None, None
    
    try:
        parts = stream_id_str.split('-')
        if len(parts) != 2:
            return None, None
        
        milliseconds_time = int(parts[0])
        
        # Check if sequence part is "*"
        if parts[1] == "*":
            # Return milliseconds_time and None to indicate auto-generation needed
            return milliseconds_time, None
        else:
            # Try to parse as integer
            sequence_number = int(parts[1])
            return milliseconds_time, sequence_number
    except (ValueError, AttributeError):
        return None, None


def parse_range_id(range_id_str):
    """
    Parse a range ID for XRANGE queries.
    
    Handles:
    - Explicit format: "1526919030473-0" (both parts are numbers)
    - Partial format: "1526919030473" (only time part, sequence defaults)
    - Special "-" (use minimum/maximum)
    
    Returns (milliseconds_time, sequence_number) or raises ValueError if invalid.
    
    For start ID: defaults sequence to 0 if omitted.
    For end ID: defaults sequence to maximum (a very large number).
    
    This function assumes 'is_start' parameter will be passed elsewhere.
    For now, we'll handle it in the caller context.
    """
    if range_id_str == "-":
        # Minimum ID indicator (for start of range)
        return 0, 0
    elif range_id_str == "+":
        # Maximum ID indicator (for end of range)
        return 2**63 - 1, 2**63 - 1  # Use very large numbers
    
    try:
        if "-" in range_id_str:
            parts = range_id_str.split("-", 1)
            milliseconds_time = int(parts[0])
            sequence_number = int(parts[1])
            return milliseconds_time, sequence_number
        else:
            # Only time part provided
            milliseconds_time = int(range_id_str)
            # Sequence will be set by caller based on context (start vs end)
            return milliseconds_time, None
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid range ID: {range_id_str}")


def compare_stream_ids(id1_ms, id1_seq, id2_ms, id2_seq):
    """
    Compare two stream IDs.
    Returns:
    -1 if id1 < id2
     0 if id1 == id2
     1 if id1 > id2
    """
    if id1_ms < id2_ms:
        return -1
    elif id1_ms > id2_ms:
        return 1
    else:
        # milliseconds_time is equal, compare sequence
        if id1_seq < id2_seq:
            return -1
        elif id1_seq > id2_seq:
            return 1
        else:
            return 0


def is_stream_id_valid(new_id_str, last_id_str=None):
    """
    Validate a stream ID.
    
    Rules:
    1. The ID must be strictly greater than 0-0 (minimum valid ID is 0-1).
    2. If there's a last ID, the new ID must be strictly greater than it.
    3. If the sequence number is None (auto-generate case), special rules apply.
    4. If both parts are None (full auto-generation with "*"), use current time.
    
    Returns:
    - (True, None, final_id) if valid
    - (False, error_message, None) if invalid
    """
    # Parse the new ID
    new_ms, new_seq = parse_stream_id(new_id_str)
    
    # Handle full auto-generation case (new_ms is None)
    if new_ms is None and new_seq is None:
        # Use current Unix time in milliseconds for the time part
        # and 0 for the sequence number (unless we need to increment)
        current_time_ms = int(time.time() * 1000)
        
        if last_id_str is not None:
            last_ms, last_seq = parse_stream_id(last_id_str)
            
            if last_ms is None or last_seq is None:
                # Only happens if last_id_str is "*" or invalid, shouldn't occur
                return False, "ERR invalid last stream ID", None
            
            if current_time_ms > last_ms:
                # Different time part, sequence starts at 0
                final_id = f"{current_time_ms}-0"
                return True, None, final_id
            elif current_time_ms == last_ms:
                # Same time part, increment sequence
                final_seq = last_seq + 1
                final_id = f"{current_time_ms}-{final_seq}"
                return True, None, final_id
            else:
                # current_time_ms < last_ms, shouldn't happen in normal circumstances
                # but if it does, use last_ms + 1
                final_id = f"{last_ms + 1}-0"
                return True, None, final_id
        else:
            # No last ID, this is the first entry
            # Special case: if time part is 0, sequence starts at 1
            if current_time_ms == 0:
                final_id = f"{current_time_ms}-1"
            else:
                final_id = f"{current_time_ms}-0"
            return True, None, final_id
    
    # Handle auto-sequence case (new_ms is not None, new_seq is None)
    if new_seq is None:
        # Auto-generate only the sequence number
        if last_id_str is not None:
            last_ms, last_seq = parse_stream_id(last_id_str)
            
            if last_ms is None:
                return False, "ERR invalid last stream ID", None
            
            if new_ms > last_ms:
                # Different time part, sequence starts at 0
                final_id = f"{new_ms}-0"
                return True, None, final_id
            elif new_ms == last_ms:
                # Same time part, increment sequence
                if last_seq is None:
                    # This shouldn't happen, but handle it
                    return False, "ERR invalid last stream ID sequence", None
                final_seq = last_seq + 1
                final_id = f"{new_ms}-{final_seq}"
                return True, None, final_id
            else:
                # new_ms < last_ms, invalid
                return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item", None
        else:
            # No last ID, this is the first entry
            # Special case: if time part is 0, sequence starts at 1
            if new_ms == 0:
                final_id = f"{new_ms}-1"
            else:
                final_id = f"{new_ms}-0"
            return True, None, final_id
    
    # Explicit ID case (new_seq is not None)
    # Check if ID is greater than 0-0
    if new_ms == 0 and new_seq == 0:
        return False, "ERR The ID specified in XADD must be greater than 0-0", None
    
    # If there's a last ID, check that new ID is strictly greater
    if last_id_str is not None:
        last_ms, last_seq = parse_stream_id(last_id_str)
        
        if last_ms is None:
            return False, "ERR invalid last stream ID", None
        
        # Handle case where last_seq is None (shouldn't happen in explicit case)
        if last_seq is None:
            return False, "ERR invalid last stream ID sequence", None
        
        # New ID must be strictly greater than last ID
        if new_ms > last_ms:
            # milliseconds_time is greater, so this is valid
            return True, None, new_id_str
        elif new_ms == last_ms:
            # milliseconds_time is equal, check sequence number
            if new_seq > last_seq:
                return True, None, new_id_str
            else:
                return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item", None
        else:
            # milliseconds_time is smaller, invalid
            return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item", None
    
    return True, None, new_id_str


def handle_client(conn, addr):
    """Handle a single client connection."""
    global active_connections
    
    #print(f"Client connected: {addr}")
    
    with connections_lock:
        active_connections += 1
    
    # Initialize transaction state for this connection
    conn_id = id(conn)
    with transaction_states_lock:
        transaction_states[conn_id] = {
            "in_transaction": False,
            "queued_commands": []
        }
    
    # Initialize watched keys for this connection
    with watched_keys_lock:
        watched_keys[conn_id] = set()
    
    try:
        with conn:
            while True:
                data = conn.recv(1024)
                
                if not data:
                    break
                
                # Parse the RESP command
                command_parts = parse_resp(data)
                
                if not command_parts:
                    continue
                
                # Get the command (case-insensitive)
                command = command_parts[0].upper()
                
                # Check if we're in a transaction
                with transaction_states_lock:
                    in_transaction = transaction_states[conn_id]["in_transaction"]
                
                # Commands that should not be queued: MULTI, EXEC, DISCARD, WATCH
                if command in ["MULTI", "EXEC", "DISCARD", "WATCH"] or not in_transaction:
                    # Process commands normally (not in transaction or special transaction commands)
                    if command == "MULTI":
                        # Start a transaction
                        with transaction_states_lock:
                            transaction_states[conn_id]["in_transaction"] = True
                            transaction_states[conn_id]["queued_commands"] = []
                        conn.sendall(encode_simple_string("OK"))
                    elif command == "EXEC":
                        # Execute all queued commands
                        with transaction_states_lock:
                            in_transaction = transaction_states[conn_id]["in_transaction"]
                            queued_commands = transaction_states[conn_id]["queued_commands"][:]
                            # Mark transaction as complete
                            transaction_states[conn_id]["in_transaction"] = False
                            transaction_states[conn_id]["queued_commands"] = []
                        
                        if not in_transaction:
                            # EXEC called without MULTI
                            conn.sendall(encode_error("ERR EXEC without MULTI"))
                        else:
                            # Execute the queued commands and collect results
                            results = []
                            for queued_cmd in queued_commands:
                                # Execute each command and collect the raw response
                                cmd_response = execute_command(queued_cmd)
                                results.append(cmd_response)
                            
                            # Build the array response
                            if not results:
                                # Empty transaction
                                response = encode_array([])
                                conn.sendall(response)
                            else:
                                # Build array of results as a string array representation
                                # We need to parse the results and re-encode them as an array
                                array_response = build_array_from_responses(results)
                                conn.sendall(array_response)
                    elif command == "DISCARD":
                        # Discard the transaction
                        with transaction_states_lock:
                            in_transaction = transaction_states[conn_id]["in_transaction"]
                            if in_transaction:
                                transaction_states[conn_id]["in_transaction"] = False
                                transaction_states[conn_id]["queued_commands"] = []
                        
                        if not in_transaction:
                            conn.sendall(encode_error("ERR DISCARD without MULTI"))
                        else:
                            conn.sendall(encode_simple_string("OK"))
                    else:
                        # Normal command execution
                        response = execute_command(command_parts)
                        conn.sendall(response)
                else:
                    # We're in a transaction and this is not a transaction control command
                    # Queue the command and return "+QUEUED\r\n"
                    with transaction_states_lock:
                        transaction_states[conn_id]["queued_commands"].append(command_parts)
                    conn.sendall(encode_simple_string("QUEUED"))
    finally:
        with connections_lock:
            active_connections -= 1
        
        # Clean up transaction state for this connection
        with transaction_states_lock:
            transaction_states.pop(conn_id, None)
        
        # Clean up watched keys for this connection
        with watched_keys_lock:
            watched_keys.pop(conn_id, None)
        
        print(f"Client disconnected: {addr}")


def build_array_from_responses(responses):
    """
    Build a RESP array from a list of raw RESP responses.
    
    This function takes a list of RESP-encoded byte responses and combines them
    into a single RESP array response.
    """
    if not responses:
        return b"*0\r\n"
    
    # Start with array header
    result = f"*{len(responses)}\r\n".encode('utf-8')
    
    # Add each response as an array element
    for resp in responses:
        result += resp
    
    return result


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Prometheus metrics."""
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/metrics':
            uptime_seconds = time.time() - start_time
            
            with connections_lock:
                current_connections = active_connections
            
            metrics_output = f"""# HELP redis_uptime_seconds Redis server uptime in seconds
# TYPE redis_uptime_seconds gauge
redis_uptime_seconds {uptime_seconds}

# HELP redis_connections_active Number of currently active client connections
# TYPE redis_connections_active gauge
redis_connections_active {current_connections}
"""
            
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.end_headers()
            self.wfile.write(metrics_output.encode('utf-8'))
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def run_metrics_server():
    """Run the Prometheus metrics HTTP server."""
    metrics_server = HTTPServer(('0.0.0.0', 8100), MetricsHandler)
    print("Prometheus metrics server listening on 0.0.0.0:8100")
    metrics_server.serve_forever()


def main():
    print("Logging starts here.")

    # Start metrics server in a separate daemon thread
    metrics_thread = threading.Thread(
        target=run_metrics_server,
        daemon=True
    )
    metrics_thread.start()

    server_socket = socket.create_server(
        ("0.0.0.0", 6379),
        reuse_port=True
    )

    print("Redis server listening on 0.0.0.0:6379")

    while True:
        conn, addr = server_socket.accept()
        
        # Create a new thread to handle this client
        client_thread = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        )
        client_thread.start()


if __name__ == "__main__":
    main()
