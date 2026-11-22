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
    s_bytes = s.encode('utf-8') if isinstance(s, str) else s
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


def encode_array(elements):
    """Encode a list of strings as a RESP array."""
    if not elements:
        return b"*0\r\n"
    
    response = f"*{len(elements)}\r\n".encode('utf-8')
    for element in elements:
        response += encode_bulk_string(element)
    
    return response


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


def handle_client(conn, addr):
    """Handle a single client connection."""
    global active_connections
    
    print(f"Client connected: {addr}")
    
    with connections_lock:
        active_connections += 1
    
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
                else:
                    # Unknown command
                    conn.sendall(b"-ERR unknown command\r\n")
    finally:
        with connections_lock:
            active_connections -= 1
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
