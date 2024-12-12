# Basic Redis Implementation

In this, implement basic Redis implementation
Idea is not to create REDIS, but learn the key concepts in REDIS.

## Inspiration

Inspired by [codecrafters](https://app.codecrafters.io/courses/redis/).

## Dependencies

Tested with Python 3.9+
Requires `pytest` if you need to run local test cases

## References and Help

### Socket

Worked directly from source

[Python Socket Guide](https://docs.python.org/3/howto/sockets.html)
[Async Socket](https://docs.python.org/3/library/asyncio-eventloop.html#working-with-socket-objects-directly)

### Async Programming

[Event Loop by Philip Roberts](https://www.youtube.com/watch?v=8aGhZQkoFbQ) is an excellent introduction to this concept.
RealPython Resources: <https://realpython.com/python-concurrency/> and <https://realpython.com/async-io-python/>

### RDB File Format and Parsing

<https://rdb.fnordig.de/file_format.html> - This was almost always open while I was working on the parsing logic for RDB
[RDB Parser Tool](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/rdbtools/parser.py) This repo served as reference whenever you hit a wall
