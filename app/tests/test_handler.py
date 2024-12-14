import os
import time

import pytest

from app.handler import RedisCommandHandler


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
        assert handler.db.get("key") == "value"

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
        handler.db.clear()

        handler.handle(["SET", "key1", "value1"])
        handler.handle(["SET", "key2", "value2"])
        handler.handle(["SET", "key3", "value3"])

        assert handler.handle(["KEYS", "*"]) == "*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"
        assert handler.handle(["KEYS", "*1"]) == "*1\r\n$4\r\nkey1\r\n"

    def test_info_replication(self):
        handler = RedisCommandHandler()
        master_replication = handler.handle(["INFO", "replication"])

        resp = master_replication.split("\r\n")
        resp_map = {}

        for item in resp[1:]:

            if not item:
                continue

            key, value = item.split(":")
            resp_map[key] = value

        assert resp_map["role"] == "master"
        assert resp_map["master_repl_offset"] == "0"
        assert len(resp_map["master_replid"]) == 40

    def test_info_replication_slave(self):
        handler = RedisCommandHandler()
        os.environ["replicaof"] = "localhost 6379"
        assert handler.handle(["INFO", "replication"]) == "$10\r\nrole:slave\r\n"

    def test_replconf(self):
        handler = RedisCommandHandler()
        assert handler.handle(["REPLCONF", "capa", "psycn2"]) == "+OK\r\n"

    def test_psync(self):
        handler = RedisCommandHandler()
        resp = handler.handle(["psync", "master_replid", "offset"])
        assert resp.startswith("+FULLRESYNC")
