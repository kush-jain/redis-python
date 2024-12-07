from collections import OrderedDict

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
        return self.data.get(key)

    def del_key(self, key):
        if key in self.data:
            del self.data[key]

    def __iter__(self):
        return iter(self.data)


DB = Database()
