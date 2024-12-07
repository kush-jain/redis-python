from app.serialiser import EncodeTypes
from app.exceptions import RedisException


PING = "ping"
ECHO = "echo"
SET = "set"
GET = "get"


DB = {}


class RedisCommandHandler:

    def __init__(self):
        self.response = {
            "data": "",
            "type": None
        }

    def ping(self):
        self.response = {
            "data": "PONG",
            "type": EncodeTypes.SIMPLE_STRING
        }

    def echo(self, args):
        self.response["data"] = args[0]

    def set(self, args):
        key = args[0]
        value = args[1]
        DB[key] = value
        self.response["data"] = "OK"
        self.response["type"] = EncodeTypes.SIMPLE_STRING

    def get(self, key):
        key = key[0]
        if key in DB:
            self.response["data"] = DB[key]
        else:
            self.response["data"] = None
            self.response["type"] = EncodeTypes.BULK_STRING

    def handle(self, command_arr):

        command = command_arr
        if isinstance(command_arr, list):
            command = command_arr[0]
            command_arr = command_arr[1:]

        command = command.lower()

        if command == PING:
            self.ping()
        elif command == ECHO:
            self.echo(command_arr)
        elif command == SET:
            self.set(command_arr)
        elif command == GET:
            self.get(command_arr)
        else:
            raise RedisException("Unknown command")

        return self.response
