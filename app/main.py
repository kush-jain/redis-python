import asyncio

from app.serialiser import RedisDecoder, RedisEncoder


async def handle_client(reader, writer):
    # Handle client communication here.
    while True:
        data = await reader.read(1024)

        if not data:
            break

        data = RedisDecoder().decode(data.decode("utf-8"))
        response = RedisEncoder().encode("PONG")

        writer.write(response.encode("utf-8"))
        await writer.drain()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_client, "localhost", 6379)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
