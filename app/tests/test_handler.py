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
        assert DB["key"] == "value"

    def test_get(self):
        handler = RedisCommandHandler()
        assert handler.handle(["GET", "key"]) == "$5\r\nvalue\r\n"

    def test_null_get(self):
        handler = RedisCommandHandler()
        assert handler.handle(["GET", "non_existent_key"]) == "$-1\r\n"
