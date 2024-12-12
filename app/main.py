import argparse
import asyncio
import os

from app.serialiser import RedisDecoder
from app.handler import RedisCommandHandler
from app.rdb_parser import RDBParser
from app.database import Database


async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)

        if not data:
            break

        data = RedisDecoder().decode(data.decode("utf-8"))
        response = RedisCommandHandler().handle(data)

        writer.write(response.encode("utf-8"))
        await writer.drain()


async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")
    args = parser.parse_args()

    if args.dir:
        os.environ["dir"] = args.dir

    if args.dbfilename:
        os.environ["dbfilename"] = args.dbfilename

    db_data = {}

    if args.dbfilename:
        rdb = RDBParser(args.dir, args.dbfilename)
        db_data = rdb.databases[0] if rdb.databases else {}

    Database(db_data)

    asyncio.run(main())
