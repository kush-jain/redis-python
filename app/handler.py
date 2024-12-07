from datetime import datetime, timedelta
import os

from app.serialiser import RedisEncoder
from app.exceptions import RedisException


PING = "ping"
ECHO = "echo"
SET = "set"
GET = "get"
CONFIG = "config"


DB = {}


class RedisCommandHandler:

    def __init__(self):
        self.encoder = RedisEncoder()

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

        DB[key] = {"value": value, "expires_at": expires_at}

        return self.encoder.encode_simple_string("OK")

    def get(self, key):
        key = key[0]
        value = DB.get(key)

        if value is not None:
            if value["expires_at"] is not None and value["expires_at"] < datetime.now():
                del DB[key]
                value = {}

        if value is None:
            value = {}

        return self.encoder.encode_bulk_string(value.get("value"))

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
        }
        kls = kls_map.get(command)
        if not kls:
            raise RedisException("Invalid command")

        return kls(command_arr)
