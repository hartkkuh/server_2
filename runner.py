import os
import asyncio
import base64
import json
import socket
from typing import Dict


# ============================================================
# Configuration
# ============================================================

RENDER_WS = os.environ[
    "RENDER_WS"
]

TOKEN = os.environ[
    "PROXY_TOKEN"
]

BUFFER_SIZE = 65536

CONNECT_TIMEOUT = 20

RECONNECT_DELAYS = (
    2,
    4,
    8,
    15,
    30
)


# ============================================================
# Active connections
# ============================================================

connections: Dict[
    str,
    tuple
] = {}

connections_lock = asyncio.Lock()

opening_connections = {}


# ============================================================
# Send message
# ============================================================

async def send_message(
    ws,
    message
):

    await ws.send(
        json.dumps(
            message,
            separators=(
                ",",
                ":"
            )
        )
    )


# ============================================================
# Send TCP data to Render
# ============================================================

async def send_data(
    ws,
    connection_id,
    data
):

    if not data:
        return

    encoded = base64.b64encode(
        data
    ).decode(
        "ascii"
    )

    await send_message(
        ws,
        {
            "type": "data",
            "connection_id":
                connection_id,
            "data":
                encoded,
        }
    )


# ============================================================
# Notify Render CLOSE
# ============================================================

async def notify_close(
    ws,
    connection_id
):

    try:

        await send_message(
            ws,
            {
                "type": "close",
                "connection_id":
                    connection_id,
            }
        )

    except Exception:
        pass


# ============================================================
# Reader
# ============================================================

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
            f"reader error:",
            e
        )

    finally:

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

        await notify_close(
            ws,
            connection_id
        )


# ============================================================
# OPEN
# ============================================================

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

        # ====================================================
        # Connect with timeout
        # ====================================================

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                host,
                port
            ),
            timeout=CONNECT_TIMEOUT
        )

        # ====================================================
        # Store
        # ====================================================

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

        # ====================================================
        # Tell Render OPEN succeeded
        # ====================================================

        await send_message(
            ws,
            {
                "type":
                    "open_ok",
                "connection_id":
                    connection_id,
            }
        )

        # ====================================================
        # Start reader
        # ====================================================

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
            f"{connection_id}:",
            e
        )

        await send_message(
            ws,
            {
                "type":
                    "error",
                "connection_id":
                    connection_id,
                "message":
                    str(e),
            }
        )


# ============================================================
# DATA
# ============================================================

async def handle_data(
    connection_id,
    encoded
):

    # ========================================================
    # Wait for OPEN
    # ========================================================

    task = opening_connections.get(
        connection_id
    )

    if task is not None:

        try:

            await task

        except Exception:
            pass

    connection = connections.get(
        connection_id
    )

    if not connection:
        return

    reader, writer = connection

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
            f"{connection_id}:",
            e
        )

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


# ============================================================
# CLOSE
# ============================================================

async def handle_close(
    connection_id
):

    task = opening_connections.get(
        connection_id
    )

    if task is not None:

        try:
            await task
        except Exception:
            pass

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


# ============================================================
# Cleanup
# ============================================================

async def cleanup_connections():

    for (
        connection_id,
        connection
    ) in list(
        connections.items()
    ):

        try:

            reader, writer = connection

            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

        except Exception:
            pass

    connections.clear()

    for task in list(
        opening_connections.values()
    ):

        if not task.done():

            task.cancel()

    opening_connections.clear()


# ============================================================
# Run one WebSocket session
# ============================================================

async def run_session():

    import websockets

    url = (
        RENDER_WS
        + "?token="
        + TOKEN
    )

    print(
        "Connecting to Render..."
    )

    async with websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=None,
        max_queue=1024
    ) as ws:

        print(
            "Connected to Render"
        )

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

                old_task = opening_connections.get(
                    connection_id
                )

                if old_task:

                    if not old_task.done():

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

                    if current is completed_task:

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

                await handle_data(
                    connection_id,
                    message.get(
                        "data",
                        ""
                    )
                )

            # =================================================
            # CLOSE
            # =================================================

            elif msg_type == "close":

                await handle_close(
                    connection_id
                )

            else:

                print(
                    "Unknown message type:",
                    msg_type
                )


# ============================================================
# Main reconnect loop
# ============================================================

async def run():

    delay_index = 0

    while True:

        try:

            await run_session()

            delay_index = 0

        except asyncio.CancelledError:

            raise

        except Exception as e:

            print(
                "Render connection error:",
                e
            )

        await cleanup_connections()

        delay = RECONNECT_DELAYS[
            min(
                delay_index,
                len(RECONNECT_DELAYS) - 1
            )
        ]

        print(
            f"Reconnecting in {delay} seconds..."
        )

        await asyncio.sleep(
            delay
        )

        if delay_index < (
            len(RECONNECT_DELAYS) - 1
        ):

            delay_index += 1


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            run()
        )

    except KeyboardInterrupt:

        print(
            "\nRunner stopped."
        )