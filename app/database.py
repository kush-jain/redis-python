from collections import OrderedDict
from datetime import datetime

from app.exceptions import RedisException
from app.utils import Singleton

STREAM = "stream"


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

        # First check if stream_key exists, if not, create one
        if stream_key not in self.data:
            self.data[stream_key] = OrderedDict(type=STREAM)
        else:
            last_stream_id = next(reversed(self.data[stream_key]))
            if last_stream_id >= stream_id:
                raise RedisException("Stream ID must be strictly greater than last entered ID")

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
