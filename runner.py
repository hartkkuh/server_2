import os
import asyncio
import base64
import json
import websockets


# =========================================================
# Configuration
# =========================================================

RENDER_WS = os.environ[
    "RENDER_WS"
]

TOKEN = os.environ[
    "PROXY_TOKEN"
]

BUFFER_SIZE = 65536


# =========================================================
# Connections
# =========================================================

connections = {}

connections_lock = asyncio.Lock()

opening_connections = {}


# =========================================================
# Send DATA
# =========================================================

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
            "data": encoded,
        })
    )


# =========================================================
# Send CLOSE
# =========================================================

async def send_close(
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


# =========================================================
# TCP reader
# =========================================================

async def pipe_reader(
    ws,
    connection_id,
    reader,
    writer
):

    try:

        while True:

            data = await reader.read(
                BUFFER_SIZE
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
            f"reader error: {e}"
        )

    finally:

        async with connections_lock:

            current = connections.get(
                connection_id
            )

            if (
                current is not None
                and current[1] is writer
            ):

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

        await send_close(
            ws,
            connection_id
        )


# =========================================================
# OPEN
# =========================================================

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
        message[
            "port"
        ]
    )

    print(
        f"[OPEN] "
        f"{connection_id} -> "
        f"{host}:{port}"
    )

    try:

        # =================================================
        # TCP connect
        # =================================================

        reader, writer = (
            await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port
                ),
                timeout=15
            )
        )

        # =================================================
        # Replace old connection if any
        # =================================================

        async with connections_lock:

            old = connections.get(
                connection_id
            )

            if old is not None:

                try:

                    old[1].close()

                    try:
                        await old[1].wait_closed()
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

        print(
            f"[CONNECTED] "
            f"{connection_id} -> "
            f"{host}:{port}"
        )

        # =================================================
        # Reader task
        # =================================================

        asyncio.create_task(
            pipe_reader(
                ws,
                connection_id,
                reader,
                writer
            )
        )

    except asyncio.CancelledError:

        raise

    except Exception as e:

        print(
            f"[OPEN ERROR] "
            f"{connection_id}: "
            f"{e}"
        )

        try:

            await ws.send(
                json.dumps({
                    "type":
                        "error",
                    "connection_id":
                        connection_id,
                    "message":
                        str(e),
                })
            )

        except Exception:
            pass


# =========================================================
# DATA
# =========================================================

async def handle_data(
    ws,
    message
):

    connection_id = message[
        "connection_id"
    ]

    # Wait for OPEN if necessary.

    task = opening_connections.get(
        connection_id
    )

    if task is not None:

        try:
            await task
        except Exception:
            pass

    async with connections_lock:

        connection = connections.get(
            connection_id
        )

    if connection is None:

        await send_close(
            ws,
            connection_id
        )

        return

    reader, writer = connection

    encoded = message.get(
        "data",
        ""
    )

    if not encoded:
        return

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
            f"{connection_id}: "
            f"{e}"
        )

        async with connections_lock:

            current = connections.get(
                connection_id
            )

            if (
                current is not None
                and current[1] is writer
            ):

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

        await send_close(
            ws,
            connection_id
        )


# =========================================================
# CLOSE
# =========================================================

async def handle_close(
    ws,
    message
):

    connection_id = message[
        "connection_id"
    ]

    task = opening_connections.get(
        connection_id
    )

    if task is not None:

        try:
            await task
        except Exception:
            pass

    async with connections_lock:

        connection = connections.pop(
            connection_id,
            None
        )

    if connection is None:
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


# =========================================================
# Cleanup
# =========================================================

async def cleanup_connections():

    async with connections_lock:

        items = list(
            connections.items()
        )

        connections.clear()

    for (
        connection_id,
        (
            reader,
            writer
        )
    ) in items:

        try:

            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

        except Exception:
            pass

    for task in list(
        opening_connections.values()
    ):

        if not task.done():

            task.cancel()

    opening_connections.clear()


# =========================================================
# Main
# =========================================================

async def run():

    url = (
        RENDER_WS
        + "?token="
        + TOKEN
    )

    reconnect_delay = 2

    while True:

        try:

            print(
                "Connecting to Render..."
            )

            async with websockets.connect(
                url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=5,
                max_size=None,
                open_timeout=20
            ) as ws:

                print(
                    "Connected to Render"
                )

                reconnect_delay = 2

                # =================================================
                # Receive loop
                # =================================================

                async for raw_message in ws:

                    try:

                        message = json.loads(
                            raw_message
                        )

                    except Exception as e:

                        print(
                            "Invalid JSON:",
                            e
                        )

                        continue

                    msg_type = message.get(
                        "type"
                    )

                    connection_id = message.get(
                        "connection_id"
                    )

                    if not connection_id:

                        continue

                    # =================================================
                    # OPEN
                    # =================================================

                    if msg_type == "open":

                        old_task = (
                            opening_connections.get(
                                connection_id
                            )
                        )

                        if (
                            old_task is not None
                            and not old_task.done()
                        ):

                            old_task.cancel()

                        task = asyncio.create_task(
                            handle_open(
                                ws,
                                message
                            )
                        )

                        opening_connections[
                            connection_id
                        ] = task

                        def cleanup_open(
                            completed_task,
                            cid=connection_id
                        ):

                            current = (
                                opening_connections.get(
                                    cid
                                )
                            )

                            if (
                                current
                                is completed_task
                            ):

                                opening_connections.pop(
                                    cid,
                                    None
                                )

                        task.add_done_callback(
                            cleanup_open
                        )

                    # =================================================
                    # DATA
                    # =================================================

                    elif msg_type == "data":

                        # IMPORTANT:
                        # Do not block the WebSocket receive loop.
                        asyncio.create_task(
                            handle_data(
                                ws,
                                message
                            )
                        )

                    # =================================================
                    # CLOSE
                    # =================================================

                    elif msg_type == "close":

                        asyncio.create_task(
                            handle_close(
                                ws,
                                message
                            )
                        )

                    else:

                        print(
                            "Unknown message type:",
                            msg_type
                        )

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK,
        ) as e:

            print(
                "Render WebSocket closed:",
                e
            )

        except asyncio.TimeoutError:

            print(
                "Render WebSocket opening timed out"
            )

        except Exception as e:

            print(
                "Render connection error:",
                e
            )

        # =========================================================
        # Cleanup
        # =========================================================

        await cleanup_connections()

        print(
            f"Reconnecting in "
            f"{reconnect_delay} seconds..."
        )

        await asyncio.sleep(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            15
        )


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    asyncio.run(
        run()
    )