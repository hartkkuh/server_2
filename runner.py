import os
import asyncio
import base64
import json

import websockets


RENDER_WS = os.environ["RENDER_WS"]
TOKEN = os.environ["PROXY_TOKEN"]


connections = {}


async def send_data(ws, connection_id, data):

    encoded = base64.b64encode(data).decode("ascii")

    await ws.send(json.dumps({
        "type": "data",
        "connection_id": connection_id,
        "data": encoded,
    }))


async def close_connection(ws, connection_id):

    try:

        await ws.send(json.dumps({
            "type": "close",
            "connection_id": connection_id,
        }))

    except Exception:
        pass


async def pipe_reader(ws, connection_id, reader):

    try:

        while True:

            data = await reader.read(16384)

            if not data:
                break

            await send_data(
                ws,
                connection_id,
                data
            )

    except Exception as e:

        print(
            f"[{connection_id}] reader error:",
            e
        )

    finally:

        await close_connection(
            ws,
            connection_id
        )


async def handle_open(ws, message):

    connection_id = message["connection_id"]

    host = message["host"]

    port = int(message["port"])

    print(
        f"[OPEN] {connection_id} -> "
        f"{host}:{port}"
    )

    try:

        reader, writer = await asyncio.open_connection(
            host,
            port
        )

        connections[connection_id] = (
            reader,
            writer
        )

        asyncio.create_task(
            pipe_reader(
                ws,
                connection_id,
                reader
            )
        )

    except Exception as e:

        print(
            f"[OPEN ERROR] {connection_id}:",
            e
        )

        await ws.send(json.dumps({
            "type": "error",
            "connection_id": connection_id,
            "message": str(e),
        }))


async def handle_data(message):

    connection_id = message["connection_id"]

    encoded = message.get("data", "")

    if connection_id not in connections:
        return

    reader, writer = connections[connection_id]

    try:

        data = base64.b64decode(encoded)

        writer.write(data)

        await writer.drain()

    except Exception as e:

        print(
            f"[WRITE ERROR] {connection_id}:",
            e
        )

        try:
            writer.close()
        except Exception:
            pass


async def handle_close(message):

    connection_id = message["connection_id"]

    connection = connections.pop(
        connection_id,
        None
    )

    if not connection:
        return

    reader, writer = connection

    try:

        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass

    except Exception:
        pass

    print(
        f"[CLOSE] {connection_id}"
    )


async def run():

    url = (
        RENDER_WS
        + "?token="
        + TOKEN
    )

    while True:

        try:

            print(
                "Connecting to Render..."
            )

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_size=None
            ) as ws:

                print(
                    "Connected to Render"
                )

                async for raw_message in ws:

                    message = json.loads(
                        raw_message
                    )

                    msg_type = message.get(
                        "type"
                    )

                    if msg_type == "open":

                        asyncio.create_task(
                            handle_open(
                                ws,
                                message
                            )
                        )

                    elif msg_type == "data":

                        await handle_data(
                            message
                        )

                    elif msg_type == "close":

                        await handle_close(
                            message
                        )

        except Exception as e:

            print(
                "Render connection error:",
                e
            )

        # Clean up connections

        for connection_id, (
            reader,
            writer
        ) in list(connections.items()):

            try:
                writer.close()
            except Exception:
                pass

        connections.clear()

        print(
            "Reconnecting in 5 seconds..."
        )

        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run())