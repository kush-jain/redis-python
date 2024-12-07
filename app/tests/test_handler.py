import os
import time

import pytest

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
        assert DB.get("key")["value"] == "value"

    def test_get(self):
        handler = RedisCommandHandler()
        handler.handle(["SET", "key", "value"])
        assert handler.handle(["GET", "key"]) == "$5\r\nvalue\r\n"

    def test_null_get(self):
        handler = RedisCommandHandler()
        assert handler.handle(["GET", "non_existent_key"]) == "$-1\r\n"

    def test_set_with_expiry_px(self):
        handler = RedisCommandHandler()
        assert handler.handle(["SET", "expiry_key", "value", "PX", "10"]) == "+OK\r\n"
        assert DB.get("expiry_key")["value"] == "value"
        assert DB.get("expiry_key")["expires_at"] is not None

        assert handler.handle(["GET", "expiry_key"]) == "$5\r\nvalue\r\n"
        time.sleep(0.2)
        assert handler.handle(["GET", "expiry_key"]) == "$-1\r\n"

    @pytest.mark.slow
    def test_set_with_expiry_ex(self):
        handler = RedisCommandHandler()
        assert handler.handle(["SET", "expiry_key", "value", "EX", "1"]) == "+OK\r\n"

        assert handler.handle(["GET", "expiry_key"]) == "$5\r\nvalue\r\n"
        time.sleep(2)
        assert handler.handle(["GET", "expiry_key"]) == "$-1\r\n"

    def test_config_get(self):
        handler = RedisCommandHandler()
        os.environ["TEST_VAR"] = "test_value"
        assert handler.handle(["CONFIG", "GET", "TEST_VAR"]) == "*2\r\n$8\r\nTEST_VAR\r\n$10\r\ntest_value\r\n"

    def test_keys(self):
        handler = RedisCommandHandler()

        # Reset DB
        DB.clear()

        handler.handle(["SET", "key1", "value1"])
        handler.handle(["SET", "key2", "value2"])
        handler.handle(["SET", "key3", "value3"])

        assert handler.handle(["KEYS", "*"]) == "*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"
        assert handler.handle(["KEYS", "*1"]) == "*1\r\n$4\r\nkey1\r\n"
