from app.handler import RedisCommandHandler
from app.serialiser import EncodeTypes


class TestHandler:

    def test_ping(self):
        handler = RedisCommandHandler()
        assert handler.handle("ping") == {
            "data": "PONG",
            "type": EncodeTypes.SIMPLE_STRING
        }

    def test_echo(self):
        handler = RedisCommandHandler()
        assert handler.handle(["ECHO", "hello"])["data"] == "hello"
