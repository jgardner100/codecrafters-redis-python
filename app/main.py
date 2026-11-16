import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(
        ("localhost", 6379),
        reuse_port=True
    )

    while True:
        conn, addr = server_socket.accept()

        with conn:
            while True:
                data = conn.recv(1024)

                if not data:
                    break

                conn.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()

