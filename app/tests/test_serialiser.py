from app.serialiser import RedisDecoder, RedisEncoder


class TestRedisDecoder:
    def test_simple_string(self):
        decoder = RedisDecoder()
        assert decoder.decode("+OK\r\n") == "OK"
        assert decoder.bytes_processed == 5

    def test_bulk_string(self):
        decoder = RedisDecoder()
        assert decoder.decode("$5\r\nhello\r\n") == "hello"
        assert decoder.bytes_processed == 11

    def test_array(self):
        decoder = RedisDecoder()
        assert decoder.decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == ["foo", "bar"]
        assert decoder.bytes_processed == 22

    def test_multi_command_decoder_single_command(self):
        assert RedisDecoder().multi_command_decoder(
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        ) == [(["foo", "bar"], 22)]

    def test_multi_command_decoder_multi_command(self):
        assert RedisDecoder().multi_command_decoder(
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        ) == [(["foo", "bar"], 22), (["foo", "bar"], 22)]

    def test_complex_multi_command(self):
        commands = [
            "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n",
            "*3\r\n$3\r\nSET\r\n$5\r\nmango\r\n$9\r\nblueberry\r\n",
            "*3\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$9\r\nraspberry\r\n"
        ]
        response = RedisDecoder().multi_command_decoder("".join(commands))
        assert response == [
            (["REPLCONF", "GETACK", "*"], 37),
            (["SET", "mango", "blueberry"], 39),
            (["SET", "strawberry", "raspberry"], 45)
        ]


class TestRedisEncoder:

    def test_simple_string(self):
        assert RedisEncoder().encode_simple_string("OK") == "+OK\r\n"

    def test_bulk_string(self):
        assert RedisEncoder().encode_bulk_string("hello") == "$5\r\nhello\r\n"

    def test_null_bulk_string(self):
        assert RedisEncoder().encode_bulk_string(None) == "$-1\r\n"

    def test_array(self):
        assert RedisEncoder().encode_array(["foo", "bar"]) == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
