# Basic Redis Implementation

In this, implement basic Redis implementation
Idea is not to create REDIS, but learn the key concepts in REDIS.

## Inspiration

Inspired by [codecrafters](https://app.codecrafters.io/courses/redis/).

## Dependencies

Tested with Python 3.9+
Requires `pytest` if you need to run local test cases

## Functionalities

### Commands

1. PING and ECHO commands
2. SET Command with limited expiry support. Only ACTIVE expiry possible
3. GET Command
4. KEYS Command
5. Limited CONFIG GET Command
6. For GET and KEYS, can read from RDB file.

### Command Line Options

1. Custom DIR and DBFILENAME parameters to specify RDB file location
2. PORT

### RDB Parser

Only support RDB v3

1. Read Metadata
2. Can parse any number of keys
3. Only work with string values for now. Does not work with compressed strings. Does work with integers encoded as strings
4. Does not support writing to RDB

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
[RDB Parser Tool](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/rdbtools/parser.py) This repo served as reference whenever I hitted a wall
