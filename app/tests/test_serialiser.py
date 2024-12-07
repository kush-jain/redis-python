from app.serialiser import RedisDecoder, RedisEncoder, EncodeTypes


class TestRedisDecoder:
    def test_simple_string(self):
        assert RedisDecoder().decode("+OK\r\n") == "OK"

    def test_bulk_string(self):
        assert RedisDecoder().decode("$5\r\nhello\r\n") == "hello"

    def test_array(self):
        assert RedisDecoder().decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == ["foo", "bar"]


class TestRedisEncoder:

    def test_simple_string(self):
        assert RedisEncoder().encode("OK", EncodeTypes.SIMPLE_STRING) == "+OK\r\n"

    def test_bulk_string(self):
        assert RedisEncoder().encode("hello") == "$5\r\nhello\r\n"

    def test_array(self):
        assert RedisEncoder().encode(["foo", "bar"]) == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

    def test_null_bulk_string(self):
        assert RedisEncoder().encode(None, EncodeTypes.BULK_STRING) == "$-1\r\n"
