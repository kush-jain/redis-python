from datetime import datetime, timedelta
import fnmatch
import os

from app.serialiser import RedisEncoder
from app.exceptions import RedisException
from app.database import Database
from app.utils import gen_random_string


PING = "ping"
ECHO = "echo"
SET = "set"
GET = "get"
CONFIG = "config"
KEYS = "keys"
INFO = "info"
REPLCONF = "replconf"


class RedisCommandHandler:

    def __init__(self):
        self.encoder = RedisEncoder()
        self.db = Database()

    def ping(self, command_arr):
        return self.encoder.encode_simple_string("PONG")

    def echo(self, args):
        return self.encoder.encode_bulk_string(args[0])

    def set(self, args):
        key = args[0]
        value = args[1]

        optional_args = args[2:]
        expires_at = None

        for idx, arg in enumerate(optional_args):
            if arg.lower() == "px":
                expires_at = datetime.now() + timedelta(milliseconds=int(optional_args[idx+1]))
            elif arg.lower() == "ex":
                expires_at = datetime.now() + timedelta(seconds=int(optional_args[idx+1]))

        self.db.set(key, value, expires_at)

        return self.encoder.encode_simple_string("OK")

    def get(self, key):

        key = key[0]
        value = self.db.get(key)

        return self.encoder.encode_bulk_string(value)

    def keys(self, pattern):
        """
        Match the pattern against DB keys, and return that
        """

        result = []
        for key in self.db:
            if fnmatch.fnmatch(key, pattern[0]):
                result.append(key)

        return self.encoder.encode_array(result)

    def config_get(self, args):
        key = args[0]
        value = os.getenv(key)
        return self.encoder.encode_array([key, value])

    def config(self, args):

        subcommand = args[0].lower()

        config_map = {
            "get": self.config_get,
        }

        if subcommand not in config_map:
            raise RedisException(f"Invalid config subcommand: {subcommand}")

        return config_map[subcommand](args[1:])

    def info_replication(self):
        is_replica = os.getenv("replicaof")
        role = "slave" if is_replica else "master"

        # Initialize the response dictionary with the role
        response_parts = {"role": role}

        # Add additional master-specific information if the node is a master
        if role == "master":
            response_parts["master_repl_offset"] = 0
            response_parts["master_replid"] = gen_random_string(40)

        # Convert dictionary to formatted response lines and encode
        response_lines = [f"{key}:{value}" for key, value in response_parts.items()]
        return self.encoder.encode_bulk_string("\r\n".join(response_lines))

    def info(self, args=None):

        args = args or []

        if not args:
            raise RedisException("Currently, INFO command expects subcommand")

        subcommand = args[0].lower()

        info_map = {
            "replication": self.info_replication,
        }

        if subcommand not in info_map:
            raise RedisException(f"Invalid info subcommand: {subcommand}")

        return info_map[subcommand]()

    def replconf(self, args):
        """
        Sends response back to replconf command.
        Currently, just send back OK - we need to add actual capabilities later on
        """

        return self.encoder.encode_simple_string("OK")

    def handle(self, command_arr):

        command = command_arr
        if isinstance(command_arr, list):
            command = command_arr[0]
            command_arr = command_arr[1:]

        command = command.lower()

        kls_map = {
            PING: self.ping,
            ECHO: self.echo,
            SET: self.set,
            GET: self.get,
            CONFIG: self.config,
            KEYS: self.keys,
            INFO: self.info,
            REPLCONF: self.replconf,
        }
        kls = kls_map.get(command)
        if not kls:
            raise RedisException("Invalid command")

        return kls(command_arr)
