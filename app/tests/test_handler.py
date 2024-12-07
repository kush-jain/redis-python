from app.handler import RedisCommandHandler, DB
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

    def test_set(self):
        handler = RedisCommandHandler()
        handler.handle(["SET", "key", "value"])
        assert handler.response["data"] == "OK"
        assert DB["key"] == "value"

    def test_get(self):
        handler = RedisCommandHandler()
        handler.handle(["SET", "key", "value"])
        assert handler.handle(["GET", "key"])["data"] == "value"

    def test_null_get(self):
        handler = RedisCommandHandler()
        assert handler.handle(["GET", "non_existent_key"])["data"] is None
