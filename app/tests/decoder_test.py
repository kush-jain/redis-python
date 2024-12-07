from app.decoder import RedisDecoder


def test_simple_string():
    assert RedisDecoder("+OK\r\n").parsed_data == "OK"


def test_bulk_string():
    assert RedisDecoder("$5\r\nhello\r\n").parsed_data == "hello"


def test_array():
    assert RedisDecoder("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").parsed_data == ["foo", "bar"]
