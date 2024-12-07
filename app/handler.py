from app.serialiser import EncodeTypes
from app.exceptions import RedisException


PING = "ping"
ECHO = "echo"


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
        else:
            raise RedisException("Unknown command")

        return self.response
