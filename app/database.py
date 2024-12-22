from collections import OrderedDict
from datetime import datetime
from enum import Enum

from app.exceptions import RedisException
from app.utils import Singleton

STREAM = "stream"

class DBErrorCode(Enum):
    STREAM_ID_SMALLER_THAN_TOP = (STREAM, "small-top", "Stream ID is smaller than top element")
    STREAM_ID_SMALLER_THAN_0 = (STREAM, "small-first", "Stream ID is smaller than 0-1")

    def __init__(self, module, code, message):
        self.code = code
        self.module = module
        self.message = message


class RedisDBException(RedisException):
    """
    Base class for DB Exceptions
    """

    def __init__(self, error_code: DBErrorCode):
        self.error_code = error_code
        super().__init__(f"[{error_code.code}] {error_code.message}")

    @property
    def code(self):
        return self.error_code.code

    @property
    def module(self):
        return self.error_code.module

    @property
    def message(self):
        return self.error_code.message


class Database(metaclass=Singleton):

    def __init__(self, data=None):
        self.data = data or OrderedDict()

    def clear(self):
        self.data = {}

    def set(self, key, value, expires=None):
        self.data[key] = {
            "value": value,
            "expires_at": expires,
        }

    def compare_stream_ids(self, current_stream_id, incoming_stream_id):
        """
        Compare incoming stream_id with current stream_id.
        Stream id is combination of two strings:
            <a>-<b>
            where <a> is timestamp in seconds and <b> is str representation of integer

        incoming_stream_id should always be greater than current_stream_id.
        First timestamp is compared, and if equal then integer part is compared
        """

        # Split both stream IDs into their components
        current_timestamp, current_int = current_stream_id.split('-')
        incoming_timestamp, incoming_int = incoming_stream_id.split('-')

        # Convert components to appropriate types for comparison
        current_timestamp = int(current_timestamp)
        incoming_timestamp = int(incoming_timestamp)
        current_int = int(current_int)
        incoming_int = int(incoming_int)

        # Compare timestamps first
        if incoming_timestamp > current_timestamp:
            return True

        if incoming_timestamp == current_timestamp:
            # If timestamps are equal, compare the integer part
            return incoming_int > current_int

        return False

    def stream_add(self, stream_key, stream_id, *args):
        """
        Add the stream_id to the stream_key

        Final stream format:
        <stream_key>: {
            "type": "stream",
            "<stream_id_1">: {
                "key1": "value1",
                "key2": "value2"
            },
            ...
        }
        """

        if not self.compare_stream_ids("0-0", stream_id):
            raise RedisDBException(DBErrorCode.STREAM_ID_SMALLER_THAN_0)

        # First check if stream_key exists, if not, create one
        if stream_key not in self.data:
            self.data[stream_key] = OrderedDict(type=STREAM)
        else:
            last_stream_id = next(reversed(self.data[stream_key]))
            if not self.compare_stream_ids(last_stream_id, stream_id):
                raise RedisDBException(DBErrorCode.STREAM_ID_SMALLER_THAN_TOP)

        # Args are key1, value1, key2, value2 format
        result = {args[i]: args[i + 1] for i in range(0, len(args), 2)}
        self.data[stream_key][stream_id] = result

    def get(self, key):
        value = self.data.get(key)

        if isinstance(value, dict) and value.get('type') == STREAM:
            return value

        if value is not None:
            if value["expires_at"] is not None and value["expires_at"] < datetime.now():
                self.del_key(key)
                value = {}

        if value is None:
            value = {}

        return value.get("value")

    def del_key(self, key):
        if key in self.data:
            del self.data[key]

    def __iter__(self):
        return iter(self.data)
