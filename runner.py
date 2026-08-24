import asyncio
import json
import os

import websockets


RELAY_URL = os.environ["RELAY_URL"]
TOKEN = os.environ["RELAY_TOKEN"]

SQUID_HOST = "127.0.0.1"
SQUID_PORT = 3128


async def bridge_connection(client_id):

    bridge_url = (
        RELAY_URL
        .replace(
            "/ws/control",
            f"/ws/bridge/{client_id}",
        )
    )

    print(
        f"Opening bridge: {client_id}",
        flush=True,
    )

    try:

        async with websockets.connect(
            bridge_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as ws:

            await ws.send(
                json.dumps({
                    "type": "auth",
                    "role": "bridge",
                    "token": TOKEN,
                })
            )

            reader, writer = await asyncio.open_connection(
                SQUID_HOST,
                SQUID_PORT,
            )

            async def websocket_to_squid():

                try:

                    async for message in ws:

                        if isinstance(message, bytes):

                            writer.write(message)

                            await writer.drain()

                except Exception as e:

                    print(
                        "WS -> Squid error:",
                        repr(e),
                        flush=True,
                    )


            async def squid_to_websocket():

                try:

                    while True:

                        data = await reader.read(
                            65536
                        )

                        if not data:
                            break

                        await ws.send(data)

                except Exception as e:

                    print(
                        "Squid -> WS error:",
                        repr(e),
                        flush=True,
                    )


            await asyncio.gather(
                websocket_to_squid(),
                squid_to_websocket(),
            )

            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

    except Exception as e:

        print(
            f"Bridge {client_id} error:",
            repr(e),
            flush=True,
        )


async def runner():

    while True:

        try:

            print(
                f"Connecting to {RELAY_URL}",
                flush=True,
            )

            async with websockets.connect(
                RELAY_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as ws:

                print(
                    "Connected to Render",
                    flush=True,
                )

                await ws.send(
                    json.dumps({
                        "type": "auth",
                        "role": "runner",
                        "token": TOKEN,
                    })
                )

                print(
                    "Runner authenticated",
                    flush=True,
                )

                async for message in ws:

                    if not isinstance(
                        message,
                        str,
                    ):
                        continue

                    data = json.loads(message)

                    if data.get("type") == "connect":

                        client_id = data["id"]

                        asyncio.create_task(
                            bridge_connection(
                                client_id
                            )
                        )

        except Exception as e:

            print(
                "Runner connection error:",
                repr(e),
                flush=True,
            )

        print(
            "Reconnecting in 5 seconds...",
            flush=True,
        )

        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(runner())