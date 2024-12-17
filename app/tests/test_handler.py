import os
import time

import pytest

from app.handler import RedisCommandHandler
from app.serialiser import RedisEncoder

encoder = RedisEncoder()

@pytest.mark.asyncio
class TestHandler:

    async def test_ping(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_simple_string("ping")) == "+PONG\r\n"

    async def test_echo(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_array(["ECHO", "hello"])) == "$5\r\nhello\r\n"

    async def test_set(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_array(["SET", "key", "value"])) == "+OK\r\n"
        assert handler.db.get("key") == "value"

    async def test_get(self):
        handler = RedisCommandHandler()
        await handler.handle(encoder.encode_array(["SET", "key", "value"]))
        assert await handler.handle(encoder.encode_array(["GET", "key"])) == "$5\r\nvalue\r\n"

    async def test_null_get(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_array(["GET", "non_existent_key"])) == "$-1\r\n"

    async def test_set_with_expiry_px(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_array(["SET", "expiry_key", "value", "PX", "10"])) == "+OK\r\n"

        assert await handler.handle(encoder.encode_array(["GET", "expiry_key"])) == "$5\r\nvalue\r\n"
        time.sleep(0.2)
        assert await handler.handle(encoder.encode_array(["GET", "expiry_key"])) == "$-1\r\n"

    @pytest.mark.slow
    async def test_set_with_expiry_ex(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_array(["SET", "expiry_key", "value", "EX", "1"])) == "+OK\r\n"

        assert await handler.handle(encoder.encode_array(["GET", "expiry_key"])) == "$5\r\nvalue\r\n"
        time.sleep(2)
        assert await handler.handle(encoder.encode_array(["GET", "expiry_key"])) == "$-1\r\n"

    async def test_config_get(self):
        handler = RedisCommandHandler()
        os.environ["TEST_VAR"] = "test_value"
        assert await handler.handle(encoder.encode_array(["CONFIG", "GET", "TEST_VAR"])) == "*2\r\n$8\r\nTEST_VAR\r\n$10\r\ntest_value\r\n"

    async def test_keys(self):
        handler = RedisCommandHandler()

        # Reset DB
        handler.db.clear()

        await handler.handle(encoder.encode_array(["SET", "key1", "value1"]))
        await handler.handle(encoder.encode_array(["SET", "key2", "value2"]))
        await handler.handle(encoder.encode_array(["SET", "key3", "value3"]))

        assert await handler.handle(encoder.encode_array(["KEYS", "*"])) == "*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"
        assert await handler.handle(encoder.encode_array(["KEYS", "*1"])) == "*1\r\n$4\r\nkey1\r\n"

    async def test_info_replication(self):
        handler = RedisCommandHandler()
        master_replication = await handler.handle(encoder.encode_array(["INFO", "replication"]))

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

    async def test_info_replication_slave(self):
        handler = RedisCommandHandler()
        os.environ["replicaof"] = "localhost 6379"
        assert await handler.handle(encoder.encode_array(["INFO", "replication"])) == "$10\r\nrole:slave\r\n"

    async def test_replconf(self):
        handler = RedisCommandHandler()
        assert await handler.handle(encoder.encode_array(["REPLCONF", "capa", "psycn2"])) == "+OK\r\n"

    async def test_psync(self):
        handler = RedisCommandHandler()
        resp = await handler.handle(encoder.encode_array(["psync", "master_replid", "offset"]))
        assert resp.startswith(b"+FULLRESYNC")
