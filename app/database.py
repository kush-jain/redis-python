from collections import OrderedDict
from datetime import datetime


class Database:

    def __init__(self):
        self.data = OrderedDict()

    def clear(self):
        self.data = {}

    def set(self, key, value, expires=None):
        self.data[key] = {
            "value": value,
            "expires_at": expires,
        }

    def get(self, key):
        value = self.data.get(key)

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


DB = Database()
