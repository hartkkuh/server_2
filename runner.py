import os
import asyncio
import base64
import json

import websockets


# =========================================
# Configuration
# =========================================

RENDER_WS = os.environ[
    "RENDER_WS"
]

TOKEN = os.environ[
    "PROXY_TOKEN"
]


# =========================================
# Active TCP connections
# =========================================

connections = {}

connections_lock = asyncio.Lock()


# =========================================
# Send TCP data -> Render
# =========================================

async def send_data(
    ws,
    connection_id,
    data
):

    if not data:
        return

    encoded = base64.b64encode(
        data
    ).decode("ascii")

    await ws.send(
        json.dumps({

            "type": "data",

            "connection_id":
                connection_id,

            "data":
                encoded,
        })
    )


# =========================================
# Notify Render about close
# =========================================

async def close_connection(
    ws,
    connection_id
):

    try:

        await ws.send(
            json.dumps({

                "type": "close",

                "connection_id":
                    connection_id,
            })
        )

    except Exception:

        pass


# =========================================
# Read remote TCP socket
# =========================================

async def pipe_reader(
    ws,
    connection_id,
    reader
):

    try:

        while True:

            data = await reader.read(
                16384
            )

            if not data:
                break

            await send_data(
                ws,
                connection_id,
                data
            )

    except asyncio.CancelledError:

        raise

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"reader error:",
            e
        )

    finally:

        # Remove connection only if it
        # still points to the same socket.
        connection = connections.pop(
            connection_id,
            None
        )

        if connection:

            _, writer = connection

            try:

                writer.close()

                try:
                    await writer.wait_closed()
                except Exception:
                    pass

            except Exception:
                pass

        await close_connection(
            ws,
            connection_id
        )


# =========================================
# Handle OPEN
# =========================================

async def handle_open(
    ws,
    message
):

    connection_id = message[
        "connection_id"
    ]

    host = message[
        "host"
    ]

    port = int(
        message["port"]
    )

    print(
        f"[OPEN] "
        f"{connection_id} -> "
        f"{host}:{port}"
    )

    try:

        # =================================
        # Connect to destination
        # =================================

        reader, writer = await asyncio.open_connection(
            host,
            port
        )

        # =================================
        # Store connection
        # =================================

        old_connection = connections.get(
            connection_id
        )

        if old_connection:

            try:

                old_reader, old_writer = (
                    old_connection
                )

                old_writer.close()

                try:
                    await old_writer.wait_closed()
                except Exception:
                    pass

            except Exception:
                pass

        connections[
            connection_id
        ] = (
            reader,
            writer
        )

        # =================================
        # Start TCP reader
        # =================================

        asyncio.create_task(
            pipe_reader(
                ws,
                connection_id,
                reader
            )
        )

    except Exception as e:

        print(
            f"[OPEN ERROR] "
            f"{connection_id}:",
            e
        )

        try:

            await ws.send(
                json.dumps({

                    "type": "error",

                    "connection_id":
                        connection_id,

                    "message":
                        str(e),
                })
            )

        except Exception:

            pass


# =========================================
# Handle DATA
# =========================================

async def handle_data(
    message
):

    connection_id = message[
        "connection_id"
    ]

    encoded = message.get(
        "data",
        ""
    )

    connection = connections.get(
        connection_id
    )

    if not connection:
        return

    reader, writer = connection

    try:

        data = base64.b64decode(
            encoded
        )

        if not data:
            return

        writer.write(
            data
        )

        await writer.drain()

    except Exception as e:

        print(
            f"[WRITE ERROR] "
            f"{connection_id}:",
            e
        )

        connections.pop(
            connection_id,
            None
        )

        try:

            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

        except Exception:

            pass


# =========================================
# Handle CLOSE
# =========================================

async def handle_close(
    message
):

    connection_id = message[
        "connection_id"
    ]

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
        f"[CLOSE] "
        f"{connection_id}"
    )


# =========================================
# Cleanup all connections
# =========================================

async def cleanup_connections():

    for connection_id, (
        reader,
        writer
    ) in list(
        connections.items()
    ):

        try:

            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

        except Exception:

            pass

    connections.clear()


# =========================================
# Main Runner
# =========================================

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

                # =========================
                # Receive commands
                # =========================

                async for raw_message in ws:

                    message = json.loads(
                        raw_message
                    )

                    msg_type = message.get(
                        "type"
                    )

                    # ---------------------
                    # OPEN
                    # ---------------------

                    if msg_type == "open":

                        asyncio.create_task(
                            handle_open(
                                ws,
                                message
                            )
                        )

                    # ---------------------
                    # DATA
                    # ---------------------

                    elif msg_type == "data":

                        await handle_data(
                            message
                        )

                    # ---------------------
                    # CLOSE
                    # ---------------------

                    elif msg_type == "close":

                        await handle_close(
                            message
                        )

        except Exception as e:

            print(
                "Render connection error:",
                e
            )

        # =================================
        # WebSocket disconnected
        # =================================

        await cleanup_connections()

        print(
            "Reconnecting in 5 seconds..."
        )

        await asyncio.sleep(
            5
        )


# =========================================
# Main
# =========================================

if __name__ == "__main__":

    asyncio.run(
        run()
    )