import socket  # noqa: F401


def handle_client(client):
    # Handle client communication here.
    client.recv(1024).decode("utf-8")
    client.sendall(b"+PONG\r\n")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client, _ = server_socket.accept()  # wait for client

    while True:
        handle_client(client)  # handle client connection


if __name__ == "__main__":
    main()
