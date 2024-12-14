import argparse
import asyncio
import os

from app.serialiser import RedisDecoder
from app.handler import RedisCommandHandler
from app.rdb_parser import RDBParser
from app.database import Database
from app.replica import Replica


async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)

        if not data:
            break

        data = RedisDecoder().decode(data.decode("utf-8"))
        response = RedisCommandHandler().handle(data)

        writer.write(response.encode("utf-8"))
        await writer.drain()


async def main(port):
    server = await asyncio.start_server(handle_client, "localhost", port)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")
    parser.add_argument("--port", default=6379)
    parser.add_argument(
        "--replicaof",
        help="If this is specified, it is assumed that this is slave replica. '<Master HOST> <Master PORT>' is needed")
    args = parser.parse_args()

    if args.dir:
        os.environ["dir"] = args.dir

    if args.dbfilename:
        os.environ["dbfilename"] = args.dbfilename

    if args.replicaof:
        os.environ["replicaof"] = args.replicaof
        master_host, master_port = args.replicaof.split()
        Replica(master_host, master_port).handshake()

    db_data = {}

    if args.dbfilename:
        rdb = RDBParser(args.dir, args.dbfilename)
        db_data = rdb.databases[0] if rdb.databases else {}

    Database(db_data)

    asyncio.run(main(args.port))
