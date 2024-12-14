import socket

from app.serialiser import RedisEncoder, RedisDecoder
from app.exceptions import RedisException


class Replica:

    def __init__(self, master_host, master_port, self_port):
        self.master_host = master_host
        self.master_port = int(master_port)
        self.port = int(self_port)
        self.encoder = RedisEncoder()
        self.decoder = RedisDecoder()
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

        self.socket.sendall(message.encode('utf-8'))
        response = self.socket.recv(1024)
        if not response:
            raise RedisException("No response received from master")

        return response.decode('utf-8')

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
        self.replconf()
        self.psync()

    def ping(self):
        response = self.send_to_master(self.encoder.encode_array(["PING"]))
        if self.decoder.decode(response) != "PONG":
            raise RedisException("Failed to receive PONG")


    def replconf(self):
        """
        Send REPLCONF commands to configure replication.

        1. Notify master of listening port
        2. Declare replication capabilities
        """
        # Send listening port configuration
        resp = self.send_to_master(
            self.encoder.encode_array(["REPLCONF", "listening-port", str(self.port)])
        )
        if self.decoder.decode(resp)!= "OK":
            raise RedisException("Failed to set listening port")

        # Send capabilities
        resp = self.send_to_master(
            self.encoder.encode_array(["REPLCONF", "capa", "psync2"])
        )
        if self.decoder.decode(resp)!= "OK":
            raise RedisException("Failed to set replication capabilities")

    def psync(self):
        """
        Initiate replication to master

        The PSYNC command is used to synchronize the state of the replica with the master.
        The replica will send this command to the master with two arguments:

        The first argument is the replication ID of the master
            Since this is the first time the replica is connecting to the master, the replication ID will be ? (a question mark)

        The second argument is the offset of the master
            Since this is the first time the replica is connecting to the master, the offset will be -1
        """
        response = self.send_to_master(self.encoder.encode_array(["PSYNC", "?", "-1"]))
        if not self.decoder.decode(response).startswith("FULLRESYNC"):
            raise RedisException("Failed to initiate replication")
