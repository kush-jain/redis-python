from datetime import datetime, timedelta
import fnmatch
import os

from app.serialiser import RedisEncoder
from app.exceptions import RedisException


PING = "ping"
ECHO = "echo"
SET = "set"
GET = "get"
CONFIG = "config"
KEYS = "keys"


class RedisCommandHandler:

    def __init__(self, db):
        # Database is dependency injection
        self.encoder = RedisEncoder()
        self.db = db

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
        }
        kls = kls_map.get(command)
        if not kls:
            raise RedisException("Invalid command")

        return kls(command_arr)
