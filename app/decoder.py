TERMINATOR = "\r\n"

SIMPLE_STRING = "+"
BULK_STRING = "$"
ARRAY = "*"


class RedisDecoder:
    """
    Decode Redis Request
    """

    def __init__(self, data: str):

        self.parsed_data = self.decode(data.split(TERMINATOR))[0]

    def decode_simple_string(self, arr: list[str]):
        return arr[0][1:], arr[1:]

    def decode_bulk_string(self, arr: list[str]):
        return arr[1], arr[2:]

    def decode_array(self, arr: list[str]):
        val = []
        count = int(arr[0][1:])

        rem_arr = arr[1:]

        while True:
            if len(val) == count:
                break

            item, rem_arr = self.decode(rem_arr)
            val.append(item)

        return val, rem_arr


    def decode(self, arr: list[str]):
        """
        Decodes a Redis request string into a Python object.

        Args:
            arr (str): Array of strings

        Returns:
            Any: The decoded Python object.
            Array: The remaining data after decoding.
        """


        resp_type = arr[0]

        if resp_type[0] == SIMPLE_STRING:
            return self.decode_simple_string(arr)

        if resp_type[0] == BULK_STRING:
            return self.decode_bulk_string(arr)

        if resp_type[0] == ARRAY:
            return self.decode_array(arr)

        return None, []
