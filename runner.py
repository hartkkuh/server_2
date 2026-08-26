import os
import asyncio
import base64
import json

import websockets


# =========================================================
# Configuration
# =========================================================

RENDER_WS = os.environ["RENDER_WS"]

TOKEN = os.environ["PROXY_TOKEN"]

BUFFER_SIZE = 16384

OPEN_TIMEOUT = 60

PING_INTERVAL = 20

PING_TIMEOUT = 30

RECONNECT_DELAY = 5


# =========================================================
# Active connections
# =========================================================

connections = {}

connections_lock = asyncio.Lock()


# =========================================================
# Send JSON
# =========================================================

async def send_json(
    ws,
    message
):

    await ws.send(
        json.dumps(
            message
        )
    )


# =========================================================
# Send TCP data
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

    await send_json(
        ws,
        {
            "type": "data",
            "connection_id":
                connection_id,
            "data": encoded,
        }
    )


# =========================================================
# Notify Render about close
# =========================================================

async def notify_close(
    ws,
    connection_id
):

    try:

        await send_json(
            ws,
            {
                "type": "close",
                "connection_id":
                    connection_id,
            }
        )

    except Exception:
        pass


# =========================================================
# Read remote TCP socket
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

            await writer.wait_closed()

        except Exception:
            pass

        await notify_close(
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
        message["port"]
    )

    print(
        f"[OPEN] "
        f"{connection_id} -> "
        f"{host}:{port}"
    )

    reader = None
    writer = None

    try:

        reader, writer = (
            await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port
                ),
                timeout=30
            )
        )

        async with connections_lock:

            old = connections.get(
                connection_id
            )

            if old:

                try:

                    old[1].close()

                    await old[1].wait_closed()

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

        asyncio.create_task(
            pipe_reader(
                ws,
                connection_id,
                reader,
                writer
            )
        )

    except Exception as e:

        print(
            f"[OPEN ERROR] "
            f"{connection_id}: {e}"
        )

        if writer:

            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        try:

            await send_json(
                ws,
                {
                    "type": "error",
                    "connection_id":
                        connection_id,
                    "message":
                        str(e),
                }
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

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"Base64 error: {e}"
        )

        return

    async with connections_lock:

        connection = connections.get(
            connection_id
        )

    if not connection:

        # =================================================
        # DATA יכול להגיע מעט לפני OPEN.
        #
        # במקום לזרוק אותו, נחכה שה-OPEN ייצור
        # את החיבור.
        # =================================================

        for _ in range(100):

            await asyncio.sleep(
                0.05
            )

            async with connections_lock:

                connection = connections.get(
                    connection_id
                )

            if connection:

                break

        if not connection:

            print(
                f"[{connection_id}] "
                f"DATA arrived before connection"
            )

            return

    reader, writer = connection

    try:

        writer.write(
            data
        )

        await writer.drain()

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"write error: {e}"
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

            await writer.wait_closed()

        except Exception:
            pass


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

    async with connections_lock:

        connection = connections.pop(
            connection_id,
            None
        )

    if not connection:
        return

    reader, writer = connection

    try:

        writer.close()

        await writer.wait_closed()

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

        current = list(
            connections.items()
        )

        connections.clear()

    for connection_id, (
        reader,
        writer
    ) in current:

        try:

            writer.close()

            await writer.wait_closed()

        except Exception:
            pass


# =========================================================
# Main
# =========================================================

async def run():

    url = (
        RENDER_WS
        + "?token="
        + TOKEN
    )

    reconnect_delay = RECONNECT_DELAY

    while True:

        try:

            print(
                "Connecting to Render..."
            )

            async with websockets.connect(
                url,
                open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=10,
                max_size=None
            ) as ws:

                print(
                    "Connected to Render"
                )

                reconnect_delay = RECONNECT_DELAY

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

                    if msg_type == "pong":
                        continue

                    connection_id = message.get(
                        "connection_id"
                    )

                    if not connection_id:
                        continue

                    # =====================================
                    # OPEN
                    # =====================================

                    if msg_type == "open":

                        asyncio.create_task(
                            handle_open(
                                ws,
                                message
                            )
                        )

                    # =====================================
                    # DATA
                    # =====================================

                    elif msg_type == "data":

                        await handle_data(
                            ws,
                            message
                        )

                    # =====================================
                    # CLOSE
                    # =====================================

                    elif msg_type == "close":

                        await handle_close(
                            ws,
                            message
                        )

                    else:

                        print(
                            "Unknown message type:",
                            msg_type
                        )

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK
        ) as e:

            print(
                "Render WebSocket closed:",
                e
            )

        except (
            asyncio.TimeoutError,
            TimeoutError
        ) as e:

            print(
                "Render connection timeout:",
                e
            )

        except Exception as e:

            print(
                "Render connection error:",
                e
            )

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
            30
        )


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    asyncio.run(
        run()
    )