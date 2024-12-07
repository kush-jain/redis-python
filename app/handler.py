from datetime import datetime, timedelta

from app.serialiser import RedisEncoder
from app.exceptions import RedisException


PING = "ping"
ECHO = "echo"
SET = "set"
GET = "get"


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
        timeout = None
        expires_at = None

        for idx, arg in enumerate(optional_args):
            if arg.lower() == "px":
                timeout = int(optional_args[idx+1])

        if timeout is not None:
            expires_at = datetime.now() + timedelta(milliseconds=timeout)

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
        }
        kls = kls_map.get(command)
        if not kls:
            raise RedisException("Invalid command")

        return kls(command_arr)
