import socket

from app.serialiser import RedisEncoder
from app.exceptions import RedisException


class Replica:

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = int(master_port)
        self.encoder = RedisEncoder()
        self.socket = None

    def connect_to_master(self):
        """
        Establish a persistent connection to the master.

        Raises:
            RedisException: If connection fails
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.master_host, self.master_port))
        except Exception as e:
            raise RedisException(f"Connection failed: {e}") from e

    def close_connection(self):
        """
        Safely close the socket connection.
        """
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                print("Error closing socket: %s", {e})
            finally:
                self.socket = None

    def send_to_master(self, message):
        """
        Send data to a TCP socket.

        Args:
            message (str/bytes): Data to be sent
        """

        try:
            self.socket.sendall(message.encode('utf-8'))
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

        self.connect_to_master()
        self.ping()

    def ping(self):
        return self.send_to_master(self.encoder.encode_array(["PING"]))
