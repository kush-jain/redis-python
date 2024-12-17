TERMINATOR = "\r\n"

SIMPLE_STRING = "+"
BULK_STRING = "$"
ARRAY = "*"



class RedisDecoder:
    """
    Decode Redis Request
    """

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

            item, rem_arr = self._decode(rem_arr)
            val.append(item)

        return val, rem_arr

    def multi_command_decoder(self, data):
        """
        Decode potentially multiple commands in a single Redis request string.

        Args:
            data (str): Redis request string containing one or more commands

        Returns:
            list: List of decoded commands
        """
        arr = data.split(TERMINATOR)
        if arr[-1] == "":
            arr = arr[:-1]  # Remove the last empty string

        # List to store multiple decoded commands
        commands = []

        # Continue decoding until all data is processed
        while arr:

            # Decode the first command
            command, arr = self._decode(arr)

            # Add the decoded command to the list
            if command is not None:
                commands.append(command)

            # If no more data, break the loop
            if not arr:
                break

        return commands

    def decode(self, data):
        """
        Decodes a Redis request string into a Python object.

        Args:
            str: Redis request string

        Returns:
            Any: The decoded Python object.
        """

        arr = data.split(TERMINATOR)
        return self._decode(arr)[0]

    def _decode(self, arr: list[str]):
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


class RedisEncoder:

    def encode_simple_string(self, data):
        return f"{SIMPLE_STRING}{data}{TERMINATOR}"

    def encode_bulk_string(self, data):
        if data is None:
            return f"{BULK_STRING}-1{TERMINATOR}"           # Null bulk string
        return f"{BULK_STRING}{len(data)}{TERMINATOR}{data}{TERMINATOR}"

    def encode_array(self, data):
        return f"{ARRAY}{len(data)}{TERMINATOR}{''.join(self.encode_bulk_string(item) for item in data)}"

    def encode_file(self, data: bytes):
        """
        Similar to bulk string except we do not add end terminator.
        Also, we expect data to be in bytes and give back bytes
        """

        return f"{BULK_STRING}{len(data)}{TERMINATOR}".encode("utf-8") + data
