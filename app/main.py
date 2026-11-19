import socket
import threading


# Global dictionary to store key-value pairs
store = {}
store_lock = threading.Lock()


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


def handle_client(conn, addr):
    """Handle a single client connection."""
    print(f"Client connected: {addr}")
    
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
                    # Store the key-value pair with thread safety
                    with store_lock:
                        store[key] = value
                    conn.sendall(b"+OK\r\n")
            elif command == "GET":
                if len(command_parts) < 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                else:
                    key = command_parts[1]
                    # Retrieve the value with thread safety
                    with store_lock:
                        value = store.get(key)
                    
                    if value is None:
                        # Key doesn't exist, return null bulk string
                        conn.sendall(encode_null_bulk_string())
                    else:
                        # Return the value as a bulk string
                        response = encode_bulk_string(value)
                        conn.sendall(response)
            else:
                # Unknown command
                conn.sendall(b"-ERR unknown command\r\n")
    
    print(f"Client disconnected: {addr}")


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(
        ("localhost", 6379),
        reuse_port=True
    )

    print("Redis server listening on localhost:6379")

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
