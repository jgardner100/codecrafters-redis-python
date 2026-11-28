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
# Format: {key: [(connection, event), ...]}
blocked_streams = {}
blocked_streams_lock = threading.Lock()

# Dictionary to track transaction state per connection
# Format: {connection_id: {"in_transaction": bool, "queued_commands": [...]}}
transaction_states = {}
transaction_states_lock = threading.Lock()

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
            result.append(line[1:])
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
            for conn, event in to_notify:
                event.set()
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
                
                # Commands that should not be queued: MULTI, EXEC, DISCARD
                if command in ["MULTI", "EXEC", "DISCARD"] or not in_transaction:
                    # Process commands normally (not in transaction or special transaction commands)
                    if command == "PING":
                        conn.sendall(b"+PONG\r\n")
                    elif command == "ECHO":
                        if len(command_parts) > 1:
                            # Send the argument as a bulk string
                            response = encode_bulk_string(command_parts[1])
                            conn.sendall(response)
                        else:
                            # No argument provided
                            conn.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")
                    elif command == "MULTI":
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
                            # For now, with empty transactions, return empty array
                            response = encode_array([])
                            conn.sendall(response)
                    elif command == "SET":
                        if len(command_parts) < 3:
                            conn.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
                        else:
                            key = command_parts[1]
                            value = command_parts[2]
                            expiry_ms = None
                            
                            # Parse optional arguments
                            i = 3
                            while i < len(command_parts):
                                option = command_parts[i].upper()
                                if option == "EX":
                                    # Expiry in seconds
                                    if i + 1 < len(command_parts):
                                        try:
                                            seconds = int(command_parts[i + 1])
                                            expiry_ms = seconds * 1000
                                            i += 2
                                        except ValueError:
                                            conn.sendall(b"-ERR invalid expire time in set\r\n")
                                            break
                                    else:
                                        conn.sendall(b"-ERR syntax error\r\n")
                                        break
                                elif option == "PX":
                                    # Expiry in milliseconds
                                    if i + 1 < len(command_parts):
                                        try:
                                            expiry_ms = int(command_parts[i + 1])
                                            i += 2
                                        except ValueError:
                                            conn.sendall(b"-ERR invalid expire time in set\r\n")
                                            break
                                    else:
                                        conn.sendall(b"-ERR syntax error\r\n")
                                        break
                                else:
                                    i += 1
                            else:
                                # Successfully parsed all options
                                with store_lock:
                                    store[key] = value
                                    if expiry_ms is not None:
                                        # Calculate absolute expiry time in milliseconds
                                        expiry_times[key] = time.time() * 1000 + expiry_ms
                                    else:
                                        # No expiry, remove any existing expiry
                                        expiry_times.pop(key, None)
                                conn.sendall(b"+OK\r\n")
                    elif command == "GET":
                        if len(command_parts) < 2:
                            conn.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                        else:
                            key = command_parts[1]
                            # Retrieve the value with thread safety
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    value = None
                                else:
                                    value = store.get(key)
                            
                            if value is None:
                                # Key doesn't exist, return null bulk string
                                conn.sendall(encode_null_bulk_string())
                            else:
                                # Return the value as a bulk string
                                response = encode_bulk_string(value)
                                conn.sendall(response)
                    elif command == "INCR":
                        if len(command_parts) < 2:
                            conn.sendall(b"-ERR wrong number of arguments for 'incr' command\r\n")
                        else:
                            key = command_parts[1]
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    value = None
                                else:
                                    value = store.get(key)
                                
                                if value is None:
                                    # Key does not exist, set it to 1
                                    store[key] = "1"
                                    new_value = 1
                                    expiry_times.pop(key, None)  # Remove any expiry
                                else:
                                    # Key exists, try to increment it
                                    try:
                                        # Convert value to integer
                                        int_value = int(value)
                                        new_value = int_value + 1
                                        # Store the new value as a string
                                        store[key] = str(new_value)
                                    except ValueError:
                                        # Value is not a valid integer
                                        conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                                        continue
                            
                            # Return the new value as an integer
                            response = encode_integer(new_value)
                            conn.sendall(response)
                    elif command == "TYPE":
                        if len(command_parts) < 2:
                            conn.sendall(b"-ERR wrong number of arguments for 'type' command\r\n")
                        else:
                            key = command_parts[1]
                            key_type = get_key_type(key)
                            # Return the type as a simple string
                            response = encode_simple_string(key_type)
                            conn.sendall(response)
                    elif command == "RPUSH":
                        if len(command_parts) < 3:
                            conn.sendall(b"-ERR wrong number of arguments for 'rpush' command\r\n")
                        else:
                            key = command_parts[1]
                            elements = command_parts[2:]
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    # Key expired, treat as non-existent
                                    store[key] = []
                                
                                # If key doesn't exist, create a new list
                                if key not in store:
                                    store[key] = []
                                
                                # Get the current value
                                current_value = store[key]
                                
                                # Check if it's a list
                                if isinstance(current_value, list):
                                    # Append elements to the list
                                    for element in elements:
                                        current_value.append(element)
                                    list_length = len(current_value)
                                else:
                                    # Key exists but is not a list, error
                                    conn.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                    continue
                            
                            # Return the length of the list as an integer
                            response = encode_integer(list_length)
                            conn.sendall(response)
                            
                            # Notify blocked clients waiting for this key
                            notify_blocked_clients(key, elements[0])
                    elif command == "LPUSH":
                        if len(command_parts) < 3:
                            conn.sendall(b"-ERR wrong number of arguments for 'lpush' command\r\n")
                        else:
                            key = command_parts[1]
                            elements = command_parts[2:]
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    # Key expired, treat as non-existent
                                    store[key] = []
                                
                                # If key doesn't exist, create a new list
                                if key not in store:
                                    store[key] = []
                                
                                # Get the current value
                                current_value = store[key]
                                
                                # Check if it's a list
                                if isinstance(current_value, list):
                                    # Prepend elements to the list
                                    # Each element is inserted at the front in order
                                    for element in elements:
                                        current_value.insert(0, element)
                                    list_length = len(current_value)
                                else:
                                    # Key exists but is not a list, error
                                    conn.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                    continue
                            
                            # Return the length of the list as an integer
                            response = encode_integer(list_length)
                            conn.sendall(response)
                            
                            # Notify blocked clients waiting for this key
                            notify_blocked_clients(key, elements[0])
                    elif command == "LRANGE":
                        if len(command_parts) < 4:
                            conn.sendall(b"-ERR wrong number of arguments for 'lrange' command\r\n")
                        else:
                            key = command_parts[1]
                            
                            try:
                                start = int(command_parts[2])
                                stop = int(command_parts[3])
                            except ValueError:
                                conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                                continue
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    value = None
                                else:
                                    value = store.get(key)
                            
                            # If key doesn't exist or is not a list, return empty array
                            if value is None or not isinstance(value, list):
                                conn.sendall(encode_array([]))
                            else:
                                list_data = value
                                list_length = len(list_data)
                                
                                # Normalize negative indexes
                                start = normalize_index(start, list_length)
                                stop = normalize_index(stop, list_length)
                                
                                # Handle edge cases
                                if start >= list_length or start > stop:
                                    # Return empty array
                                    conn.sendall(encode_array([]))
                                else:
                                    # Clamp stop index to list length
                                    if stop >= list_length:
                                        stop = list_length - 1
                                    
                                    # Extract the range
                                    result = list_data[start:stop + 1]
                                    response = encode_array(result)
                                    conn.sendall(response)
                    elif command == "LLEN":
                        if len(command_parts) < 2:
                            conn.sendall(b"-ERR wrong number of arguments for 'llen' command\r\n")
                        else:
                            key = command_parts[1]
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    value = None
                                else:
                                    value = store.get(key)
                            
                            # If key doesn't exist or is not a list, return 0
                            if value is None or not isinstance(value, list):
                                response = encode_integer(0)
                            else:
                                # Return the length of the list as an integer
                                list_length = len(value)
                                response = encode_integer(list_length)
                            
                            conn.sendall(response)
                    elif command == "LPOP":
                        if len(command_parts) < 2:
                            conn.sendall(b"-ERR wrong number of arguments for 'lpop' command\r\n")
                        else:
                            key = command_parts[1]
                            count = None
                            
                            # Parse optional count argument
                            if len(command_parts) >= 3:
                                try:
                                    count = int(command_parts[2])
                                    if count < 0:
                                        conn.sendall(b"-ERR count should be a positive integer\r\n")
                                        continue
                                except ValueError:
                                    conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                                    continue
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    value = None
                                else:
                                    value = store.get(key)
                                
                                # If key doesn't exist or is not a list
                                if value is None or not isinstance(value, list):
                                    if count is None:
                                        # No count specified: return null bulk string
                                        response = encode_null_bulk_string()
                                    else:
                                        # Count specified: return empty array
                                        response = encode_array([])
                                else:
                                    # List exists
                                    if len(value) == 0:
                                        if count is None:
                                            # No count specified: return null bulk string
                                            response = encode_null_bulk_string()
                                        else:
                                            # Count specified: return empty array
                                            response = encode_array([])
                                    else:
                                        if count is None:
                                            # No count specified: remove and return single element
                                            element = value.pop(0)
                                            response = encode_bulk_string(element)
                                        else:
                                            # Count specified: remove and return multiple elements
                                            # Remove up to count elements from the front
                                            elements_to_remove = min(count, len(value))
                                            removed_elements = []
                                            for _ in range(elements_to_remove):
                                                removed_elements.append(value.pop(0))
                                            response = encode_array(removed_elements)
                            
                            conn.sendall(response)
                    elif command == "BLPOP":
                        if len(command_parts) < 3:
                            conn.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                        else:
                            key = command_parts[1]
                            
                            # Parse timeout as a float to support decimal values like 0.5, 0.1
                            try:
                                timeout = float(command_parts[2])
                            except ValueError:
                                conn.sendall(b"-ERR timeout is not a float or out of range\r\n")
                                continue
                            
                            # Try to pop an element immediately
                            element = None
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    value = None
                                else:
                                    value = store.get(key)
                                
                                # If key exists and is a non-empty list
                                if value is not None and isinstance(value, list) and len(value) > 0:
                                    element = value.pop(0)
                            
                            if element is not None:
                                # Element was available, return it immediately
                                response = encode_array([key, element])
                                conn.sendall(response)
                            else:
                                # No element available, block the client
                                event = threading.Event()
                                
                                # Register this client as blocked for this key
                                with blocked_clients_lock:
                                    if key not in blocked_clients:
                                        blocked_clients[key] = []
                                    blocked_clients[key].append((conn, event))
                                
                                # Wait for the event to be signaled (with timeout if specified)
                                # timeout 0 means wait indefinitely
                                if timeout == 0:
                                    event.wait()
                                else:
                                    event.wait(timeout=timeout)
                                
                                # Check if we were signaled or timed out
                                if event.is_set():
                                    # We were signaled, an element is available
                                    with store_lock:
                                        # Check if key has expired
                                        if is_key_expired(key):
                                            value = None
                                        else:
                                            value = store.get(key)
                                        
                                        # Pop the element
                                        if value is not None and isinstance(value, list) and len(value) > 0:
                                            element = value.pop(0)
                                        else:
                                            element = None
                                    
                                    if element is not None:
                                        response = encode_array([key, element])
                                        conn.sendall(response)
                                    else:
                                        # This shouldn't happen, but handle it gracefully
                                        response = encode_null_array()
                                        conn.sendall(response)
                                else:
                                    # Timeout reached, return null array
                                    response = encode_null_array()
                                    conn.sendall(response)
                                
                                # Clean up the blocked client registration
                                with blocked_clients_lock:
                                    if key in blocked_clients:
                                        # Remove this client from the blocked list if it's still there
                                        blocked_clients[key] = [(c, e) for c, e in blocked_clients[key] if e is not event]
                                        if len(blocked_clients[key]) == 0:
                                            del blocked_clients[key]
                    elif command == "XADD":
                        if len(command_parts) < 4:
                            conn.sendall(b"-ERR wrong number of arguments for 'xadd' command\r\n")
                        else:
                            key = command_parts[1]
                            entry_id_input = command_parts[2]
                            
                            # Extract key-value pairs
                            kv_pairs = command_parts[3:]
                            
                            # Validate that we have an even number of arguments for key-value pairs
                            if len(kv_pairs) % 2 != 0:
                                conn.sendall(b"-ERR wrong number of arguments for 'xadd' command\r\n")
                                continue
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    # Key expired, treat as non-existent
                                    stream_data = {
                                        "_stream_entries": []
                                    }
                                    store[key] = stream_data
                                
                                # If key doesn't exist, create a new stream
                                if key not in store:
                                    stream_data = {
                                        "_stream_entries": []
                                    }
                                    store[key] = stream_data
                                
                                # Get the current value
                                current_value = store[key]
                                
                                # Check if it's a stream
                                if isinstance(current_value, dict) and "_stream_entries" in current_value:
                                    # Validate the entry ID
                                    entries = current_value["_stream_entries"]
                                    last_id = None
                                    
                                    if len(entries) > 0:
                                        # Get the ID of the last entry
                                        last_id = entries[-1]["id"]
                                    
                                    # Validate the new ID and handle auto-generation
                                    is_valid, error_msg, final_entry_id = is_stream_id_valid(entry_id_input, last_id)
                                    
                                    if not is_valid:
                                        # ID is invalid, send error
                                        error_response = encode_error(error_msg)
                                        conn.sendall(error_response)
                                        continue
                                    
                                    # Create entry object with ID and key-value pairs
                                    entry = {"id": final_entry_id}
                                    
                                    # Add key-value pairs to entry
                                    for i in range(0, len(kv_pairs), 2):
                                        entry_key = kv_pairs[i]
                                        entry_value = kv_pairs[i + 1]
                                        entry[entry_key] = entry_value
                                    
                                    # Add entry to stream
                                    current_value["_stream_entries"].append(entry)
                                    
                                    # Return the entry ID
                                    response = encode_bulk_string(final_entry_id)
                                else:
                                    # Key exists but is not a stream, error
                                    conn.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                    continue
                            
                            conn.sendall(response)
                            
                            # Notify blocked clients waiting for this stream
                            notify_blocked_streams(key)
                    elif command == "XRANGE":
                        if len(command_parts) < 4:
                            conn.sendall(b"-ERR wrong number of arguments for 'xrange' command\r\n")
                        else:
                            key = command_parts[1]
                            start_id_str = command_parts[2]
                            end_id_str = command_parts[3]
                            
                            try:
                                # Parse start ID (default sequence to 0 if omitted)
                                start_ms, start_seq = parse_range_id(start_id_str)
                                if start_seq is None:
                                    start_seq = 0
                                
                                # Parse end ID (default sequence to max if omitted)
                                end_ms, end_seq = parse_range_id(end_id_str)
                                if end_seq is None:
                                    end_seq = 2**63 - 1  # Maximum possible sequence number
                            except ValueError as e:
                                conn.sendall(encode_error(f"ERR {str(e)}"))
                                continue
                            
                            with store_lock:
                                # Check if key has expired
                                if is_key_expired(key):
                                    stream_value = None
                                else:
                                    stream_value = store.get(key)
                                
                                # If key doesn't exist or is not a stream, return empty array
                                if stream_value is None or not isinstance(stream_value, dict) or "_stream_entries" not in stream_value:
                                    conn.sendall(encode_array([]))
                                else:
                                    # Get all entries
                                    entries = stream_value["_stream_entries"]
                                    
                                    # Filter entries within the range
                                    range_entries = []
                                    for entry in entries:
                                        entry_id = entry["id"]
                                        entry_ms, entry_seq = parse_stream_id(entry_id)
                                        
                                        # Check if entry ID is within range [start, end]
                                        if compare_stream_ids(entry_ms, entry_seq, start_ms, start_seq) >= 0 and \
                                           compare_stream_ids(entry_ms, entry_seq, end_ms, end_seq) <= 0:
                                            # Entry is within range, include it
                                            range_entries.append(entry)
                                    
                                    # Format response as nested array
                                    response_data = []
                                    for entry in range_entries:
                                        entry_id = entry["id"]
                                        # Build key-value pairs list (excluding the "id" key)
                                        kv_list = []
                                        for key_name in sorted(entry.keys()):
                                            if key_name != "id":
                                                kv_list.append(key_name)
                                                kv_list.append(entry[key_name])
                                        
                                        # Each entry is [id, [k1, v1, k2, v2, ...]]
                                        response_data.append([entry_id, kv_list])
                                    
                                    # Encode as nested array
                                    response = encode_nested_array(response_data)
                                    conn.sendall(response)
                    elif command == "XREAD":
                        if len(command_parts) < 4:
                            conn.sendall(b"-ERR wrong number of arguments for 'xread' command\r\n")
                        else:
                            # Parse XREAD command
                            # Format: XREAD [BLOCK milliseconds] STREAMS key1 [key2 ...] id1 [id2 ...]
                            
                            block_timeout = None
                            has_block_keyword = False
                            streams_index = -1
                            
                            # Look for BLOCK keyword
                            for i, part in enumerate(command_parts):
                                if part.upper() == "BLOCK":
                                    has_block_keyword = True
                                    # Next argument should be the timeout in milliseconds
                                    if i + 1 < len(command_parts):
                                        try:
                                            block_timeout_ms = int(command_parts[i + 1])
                                            # Convert milliseconds to seconds for Python
                                            # If block_timeout_ms is 0, pass None to wait indefinitely
                                            if block_timeout_ms == 0:
                                                block_timeout = None
                                            else:
                                                block_timeout = block_timeout_ms / 1000.0
                                        except ValueError:
                                            conn.sendall(b"-ERR timeout is not an integer or out of range\r\n")
                                            break
                                    else:
                                        conn.sendall(b"-ERR syntax error\r\n")
                                        break
                            else:
                                # Find the STREAMS keyword
                                for i, part in enumerate(command_parts):
                                    if part.upper() == "STREAMS":
                                        streams_index = i
                                        break
                                
                                if streams_index == -1 or streams_index == len(command_parts) - 1:
                                    conn.sendall(b"-ERR syntax error\r\n")
                                    continue
                                
                                # Extract keys and IDs after STREAMS keyword
                                remaining_args = command_parts[streams_index + 1:]
                                num_args = len(remaining_args)
                                
                                # Number of keys and IDs must be equal
                                if num_args % 2 != 0:
                                    conn.sendall(b"-ERR syntax error\r\n")
                                    continue
                                
                                num_keys = num_args // 2
                                stream_keys = remaining_args[:num_keys]
                                stream_ids = remaining_args[num_keys:]
                                
                                # Resolve $ markers to actual IDs (snapshot them now)
                                resolved_ids = []
                                for i, read_id in enumerate(stream_ids):
                                    if read_id == "$":
                                        # Get the last ID in the stream (if it exists)
                                        with store_lock:
                                            # Check if key has expired
                                            if is_key_expired(stream_keys[i]):
                                                stream_value = None
                                            else:
                                                stream_value = store.get(stream_keys[i])
                                            
                                            if stream_value is None or not isinstance(stream_value, dict) or "_stream_entries" not in stream_value:
                                                # Stream doesn't exist, treat as if last ID is -1
                                                resolved_ids.append((-1, -1))
                                            else:
                                                # Stream exists, get the last entry ID
                                                entries = stream_value["_stream_entries"]
                                                if len(entries) > 0:
                                                    last_entry_id = entries[-1]["id"]
                                                    ms, seq = parse_stream_id(last_entry_id)
                                                    resolved_ids.append((ms, seq))
                                                else:
                                                    # Stream is empty, treat as if last ID is -1
                                                    resolved_ids.append((-1, -1))
                                    else:
                                        # Parse the read ID normally
                                        try:
                                            ms, seq = parse_stream_id(read_id)
                                            resolved_ids.append((ms, seq))
                                        except (ValueError, AttributeError):
                                            conn.sendall(encode_error("ERR Invalid stream ID specified as stream argument"))
                                            break
                                else:
                                    # Successfully resolved all IDs
                                    # First, try to read without blocking
                                    result_streams = []
                                    
                                    for i, stream_key in enumerate(stream_keys):
                                        read_ms, read_seq = resolved_ids[i]
                                        
                                        with store_lock:
                                            # Check if key has expired
                                            if is_key_expired(stream_key):
                                                stream_value = None
                                            else:
                                                stream_value = store.get(stream_key)
                                            
                                            # If key doesn't exist or is not a stream, skip it
                                            if stream_value is None or not isinstance(stream_value, dict) or "_stream_entries" not in stream_value:
                                                continue
                                            
                                            # Get all entries
                                            entries = stream_value["_stream_entries"]
                                            
                                            # Filter entries greater than the specified ID (exclusive)
                                            filtered_entries = []
                                            for entry in entries:
                                                entry_id = entry["id"]
                                                entry_ms, entry_seq = parse_stream_id(entry_id)
                                                
                                                # Include entries with ID > read_id
                                                if compare_stream_ids(entry_ms, entry_seq, read_ms, read_seq) > 0:
                                                    # Entry ID is greater than read_id, include it
                                                    filtered_entries.append(entry)
                                            
                                            # Format entries for response
                                            if filtered_entries:
                                                response_entries = []
                                                for entry in filtered_entries:
                                                    entry_id = entry["id"]
                                                    # Build key-value pairs list (excluding the "id" key)
                                                    kv_list = []
                                                    for key_name in sorted(entry.keys()):
                                                        if key_name != "id":
                                                            kv_list.append(key_name)
                                                            kv_list.append(entry[key_name])
                                                    
                                                    # Each entry is [entry_id, [k1, v1, k2, v2, ...]]
                                                    response_entries.append([entry_id, kv_list])
                                                
                                                # Add to result streams
                                                result_streams.append([stream_key, response_entries])
                                    
                                    # Check if we have data or should block
                                    if result_streams:
                                        # Data is available, return it immediately
                                        response = encode_streams_response(result_streams)
                                        conn.sendall(response)
                                    elif has_block_keyword:
                                        # No data available, block the client if BLOCK was specified
                                        event = threading.Event()
                                        
                                        # Register this client as blocked for all these keys
                                        with blocked_streams_lock:
                                            for stream_key in stream_keys:
                                                if stream_key not in blocked_streams:
                                                    blocked_streams[stream_key] = []
                                                blocked_streams[stream_key].append((conn, event))
                                        
                                        # Wait for the event with timeout
                                        # If block_timeout is None, event.wait() will wait indefinitely
                                        event.wait(timeout=block_timeout)
                                        
                                        # Check if we were signaled or timed out
                                        if event.is_set():
                                            # We were signaled, new data is available, read again using resolved_ids
                                            result_streams = []
                                            
                                            for i, stream_key in enumerate(stream_keys):
                                                read_ms, read_seq = resolved_ids[i]
                                                
                                                with store_lock:
                                                    # Check if key has expired
                                                    if is_key_expired(stream_key):
                                                        stream_value = None
                                                    else:
                                                        stream_value = store.get(stream_key)
                                                    
                                                    # If key doesn't exist or is not a stream, skip it
                                                    if stream_value is None or not isinstance(stream_value, dict) or "_stream_entries" not in stream_value:
                                                        continue
                                                    
                                                    # Get all entries
                                                    entries = stream_value["_stream_entries"]
                                                    
                                                    # Filter entries greater than the specified ID (exclusive)
                                                    filtered_entries = []
                                                    for entry in entries:
                                                        entry_id = entry["id"]
                                                        entry_ms, entry_seq = parse_stream_id(entry_id)
                                                        
                                                        # Include entries with ID > read_id
                                                        if compare_stream_ids(entry_ms, entry_seq, read_ms, read_seq) > 0:
                                                            # Entry ID is greater than read_id, include it
                                                            filtered_entries.append(entry)
                                                    
                                                    # Format entries for response
                                                    if filtered_entries:
                                                        response_entries = []
                                                        for entry in filtered_entries:
                                                            entry_id = entry["id"]
                                                            # Build key-value pairs list (excluding the "id" key)
                                                            kv_list = []
                                                            for key_name in sorted(entry.keys()):
                                                                if key_name != "id":
                                                                    kv_list.append(key_name)
                                                                    kv_list.append(entry[key_name])
                                                            
                                                            # Each entry is [entry_id, [k1, v1, k2, v2, ...]]
                                                            response_entries.append([entry_id, kv_list])
                                                        
                                                        # Add to result streams
                                                        result_streams.append([stream_key, response_entries])
                                            
                                            if result_streams:
                                                response = encode_streams_response(result_streams)
                                                conn.sendall(response)
                                            else:
                                                # No data found, return null array
                                                response = encode_null_array()
                                                conn.sendall(response)
                                        else:
                                            # Timeout expired, return null array
                                            response = encode_null_array()
                                            conn.sendall(response)
                                        
                                        # Clean up the blocked client registration
                                        with blocked_streams_lock:
                                            for stream_key in stream_keys:
                                                if stream_key in blocked_streams:
                                                    # Remove this client from the blocked list
                                                    blocked_streams[stream_key] = [(c, e) for c, e in blocked_streams[stream_key] if e is not event]
                                                    if len(blocked_streams[stream_key]) == 0:
                                                        del blocked_streams[stream_key]
                                    else:
                                        # No data and not blocking, return null array
                                        response = encode_null_array()
                                        conn.sendall(response)
                            continue
                        continue
                    else:
                        # Unknown command
                        conn.sendall(b"-ERR unknown command\r\n")
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
        
        print(f"Client disconnected: {addr}")


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
