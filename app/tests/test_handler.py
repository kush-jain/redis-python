import time
from app.handler import RedisCommandHandler, DB


class TestHandler:

    def test_ping(self):
        handler = RedisCommandHandler()
        assert handler.handle("ping") == "+PONG\r\n"

    def test_echo(self):
        handler = RedisCommandHandler()
        assert handler.handle(["ECHO", "hello"]) == "$5\r\nhello\r\n"

    def test_set(self):
        handler = RedisCommandHandler()
        assert handler.handle(["SET", "key", "value"]) == "+OK\r\n"
        assert DB["key"]["value"] == "value"

    def test_get(self):
        handler = RedisCommandHandler()
        handler.handle(["SET", "key", "value"])
        assert handler.handle(["GET", "key"]) == "$5\r\nvalue\r\n"

    def test_null_get(self):
        handler = RedisCommandHandler()
        assert handler.handle(["GET", "non_existent_key"]) == "$-1\r\n"

    def test_set_with_expiry(self):
        handler = RedisCommandHandler()
        assert handler.handle(["SET", "expiry_key", "value", "PX", "10"]) == "+OK\r\n"
        assert DB["expiry_key"]["value"] == "value"
        assert DB["expiry_key"]["expires_at"] is not None

        assert handler.handle(["GET", "expiry_key"]) == "$5\r\nvalue\r\n"
        time.sleep(0.2)
        assert handler.handle(["GET", "expiry_key"]) == "$-1\r\n"
