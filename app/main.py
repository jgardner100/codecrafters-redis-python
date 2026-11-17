import socket
import threading


def handle_client(conn, addr):
    """Handle a single client connection."""
    print(f"Client connected: {addr}")
    
    with conn:
        while True:
            data = conn.recv(1024)
            
            if not data:
                break
            
            conn.sendall(b"+PONG\r\n")
    
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
