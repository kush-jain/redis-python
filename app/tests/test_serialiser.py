from app.serialiser import RedisDecoder, RedisEncoder


class TestRedisDecoder:
    def test_simple_string(self):
        assert RedisDecoder().decode("+OK\r\n") == "OK"

    def test_bulk_string(self):
        assert RedisDecoder().decode("$5\r\nhello\r\n") == "hello"

    def test_array(self):
        assert RedisDecoder().decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == ["foo", "bar"]

    def test_multi_command_decoder_single_command(self):
        assert RedisDecoder().multi_command_decoder("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == [["foo", "bar"]]

    def test_multi_command_decoder_multi_command(self):
        assert RedisDecoder().multi_command_decoder(
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        ) == [["foo", "bar"], ["foo", "bar"]]


class TestRedisEncoder:

    def test_simple_string(self):
        assert RedisEncoder().encode_simple_string("OK") == "+OK\r\n"

    def test_bulk_string(self):
        assert RedisEncoder().encode_bulk_string("hello") == "$5\r\nhello\r\n"

    def test_null_bulk_string(self):
        assert RedisEncoder().encode_bulk_string(None) == "$-1\r\n"

    def test_array(self):
        assert RedisEncoder().encode_array(["foo", "bar"]) == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
