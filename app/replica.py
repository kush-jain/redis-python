import socket

from app.serialiser import RedisEncoder


class Replica:

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = int(master_port)
        self.encoder = RedisEncoder()

    def send_to_master(self, message):
        """
        Send data to a TCP socket.

        Args:
            message (str/bytes): Data to be sent
        """
        try:
            # Create a TCP socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # Connect to the server
                sock.connect((self.master_host, self.master_port))

                # Convert message to bytes if it's not already
                if isinstance(message, str):
                    message = message.encode('utf-8')

                # Send the data
                sock.sendall(message)

        except ConnectionRefusedError:
            print(f"Connection to {self.master_host}:{self.master_port} was refused")
        except socket.error as e:
            print(f"Socket error occurred: {e}")

    def handshake(self):
        """
        Handshake process with master

        There are three parts to this handshake:

            The replica sends a PING to the master
            The replica sends REPLCONF twice to the master
            The replica sends PSYNC to the master
        """

        self.send_to_master(self.ping())

    def ping(self):
        return self.encoder.encode_array(["PING"])
