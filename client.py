import asyncio
import json
import os

import websockets


RELAY_URL = os.environ["RELAY_URL"]
TOKEN = os.environ["RELAY_TOKEN"]

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 3128


async def proxy_connection(
    reader,
    writer,
):

    print(
        "Local client connected",
        flush=True,
    )

    try:

        async with websockets.connect(
            RELAY_URL,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as ws:

            await ws.send(
                json.dumps({
                    "type": "auth",
                    "role": "client",
                    "token": TOKEN,
                })
            )

            async def local_to_websocket():

                try:

                    while True:

                        data = await reader.read(
                            65536
                        )

                        if not data:
                            break

                        await ws.send(data)

                except Exception:
                    pass


            async def websocket_to_local():

                try:

                    async for message in ws:

                        if isinstance(
                            message,
                            bytes,
                        ):

                            writer.write(message)

                            await writer.drain()

                except Exception:
                    pass


            await asyncio.gather(
                local_to_websocket(),
                websocket_to_local(),
            )

    except Exception as e:

        print(
            "Proxy connection error:",
            repr(e),
            flush=True,
        )

    finally:

        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass

        print(
            "Local client disconnected",
            flush=True,
        )


async def main():

    server = await asyncio.start_server(
        proxy_connection,
        LOCAL_HOST,
        LOCAL_PORT,
    )

    addresses = ", ".join(
        str(sock.getsockname())
        for sock in server.sockets
    )

    print(
        "Local proxy listening on:",
        addresses,
        flush=True,
    )

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())