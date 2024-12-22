import string
import random
import time

from app.exceptions import RedisException


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(Singleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


def gen_random_string(length: int) -> str:
    """
    Generate random alphanumeric string of given length
    """

    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


class StreamUtils:

    @classmethod
    def compare_stream_ids(cls, current_stream_id, incoming_stream_id):
        """
        Compare incoming stream_id with current stream_id.
        Stream id is combination of two strings:
            <a>-<b>
            where <a> is timestamp in seconds and <b> is str representation of integer

        incoming_stream_id should always be greater than current_stream_id.
        First timestamp is compared, and if equal then integer part is compared
        """

        # WE need to generate complete, so no use checking,
        # since we will always generate correct one
        if incoming_stream_id == "*":
            return True

        # Split both stream IDs into their components
        current_timestamp, current_int = current_stream_id.split('-')
        incoming_timestamp, incoming_int = incoming_stream_id.split('-')

        # Convert components to appropriate types for comparison
        current_timestamp = int(current_timestamp)
        incoming_timestamp = int(incoming_timestamp)

        # Compare timestamps first
        if incoming_timestamp > current_timestamp:
            return True

        # We need to generate the integer part
        # We can only generate it if timestamp is equal to or greater than current
        # We already have checked for greater timestamp above
        if incoming_int == "*":
            return incoming_timestamp == current_timestamp

        current_int = int(current_int)
        incoming_int = int(incoming_int)

        if incoming_timestamp == current_timestamp:
            # If timestamps are equal, compare the integer part
            return incoming_int > current_int

        return False

    @classmethod
    def generate_stream_id(cls, incoming_stream_id, current_stream_id):
        """
        Stream ID could be:
        - *: we need to generate it
        - <timestamp>-*: We need to generate the sequence number part
        - Explicit: no need to do anything just return as it is
        """

        if incoming_stream_id == "*":
            current_timestamp = time.time_ns() // 1_000_000
            return f"{current_timestamp}-0"

        timestamp, sequence = incoming_stream_id.split("-")

        # This is explicit case
        if sequence != "*":
            return incoming_stream_id

        # Otherwise we need to generate the sequence number part
        # If current stream_id also has same timestamp, then we just increment by 1
        current_timestamp, current_sequence = current_stream_id.split("-")

        if current_timestamp == timestamp:
            seq = int(current_sequence) + 1
            return f"{timestamp}-{seq}"

        # If it is new timestamp, then we need to start from the beginning
        return f"{timestamp}-0"
