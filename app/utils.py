import string
import random

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
