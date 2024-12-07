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
        DB[key] = value
        return self.encoder.encode_simple_string("OK")

    def get(self, key):
        key = key[0]
        value = DB.get(key)
        return self.encoder.encode_bulk_string(value)

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
