import argparse
import asyncio
import os

from app.connection_registry import ConnectionRegistry
from app.handler import RedisCommandHandler
from app.rdb.parser import RDBParser
from app.database import Database
from app.replica import Replica


async def handle_client(reader, writer):

    connection_registry = ConnectionRegistry()
    handler = RedisCommandHandler(connection_registry)

    try:
        while True:
            data = await reader.read(1024)

            if not data:
                break

            data = data.decode("utf-8")
            response = await handler.handle(data, writer)

            if response:
                if isinstance(response, str):
                    response = response.encode("utf-8")
                writer.write(response)

            await writer.drain()
    except Exception as e:
        print("Error", e)
    finally:
        # Ensure cleanup of replica if connection closes
        await connection_registry.remove_replica(writer)
        writer.close()
        await writer.wait_closed()


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

        replica = Replica(master_host, master_port, args.port)
        replica.handshake()
        replica.close_connection()

    db_data = {}

    if args.dbfilename:
        rdb = RDBParser(args.dir, args.dbfilename)
        db_data = rdb.databases[0] if rdb.databases else {}

    Database(db_data)

    asyncio.run(main(args.port))
