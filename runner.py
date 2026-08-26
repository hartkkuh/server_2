import os
import asyncio
import base64
import json
import websockets


# ============================================================
# Configuration
# ============================================================

RENDER_WS = os.environ[
    "RENDER_WS"
]

TOKEN = os.environ[
    "PROXY_TOKEN"
]

BUFFER_SIZE = 32768


# ============================================================
# Connections
# ============================================================

connections = {}

opening_connections = {}

connections_lock = asyncio.Lock()


# ============================================================
# Send data to Render
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


    await ws.send(
        json.dumps({
            "type":
                "data",

            "connection_id":
                connection_id,

            "data":
                encoded,
        })
    )


# ============================================================
# Send close
# ============================================================

async def send_close(
    ws,
    connection_id
):

    try:

        await ws.send(
            json.dumps({
                "type":
                    "close",

                "connection_id":
                    connection_id,
            })
        )

    except Exception:
        pass


# ============================================================
# TCP reader
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
            f"reader error: {e}"
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

            await writer.wait_closed()

        except Exception:
            pass


        await send_close(
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
        # TCP connection
        # ====================================================

        reader, writer = await asyncio.wait_for(

            asyncio.open_connection(
                host,
                port
            ),

            timeout=20
        )


        # ====================================================
        # Replace old connection
        # ====================================================

        old = connections.get(
            connection_id
        )


        if old is not None:

            try:

                old_writer = old[1]

                old_writer.close()

                await old_writer.wait_closed()

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


# ============================================================
# Wait for OPEN
# ============================================================

async def wait_for_open(
    connection_id
):

    task = opening_connections.get(
        connection_id
    )


    if task is None:
        return


    try:

        await task

    except Exception:
        pass


# ============================================================
# DATA
# ============================================================

async def handle_data(
    ws,
    message
):

    connection_id = message[
        "connection_id"
    ]


    # ========================================================
    # Important:
    # wait for TCP connection
    # ========================================================

    await wait_for_open(
        connection_id
    )


    connection = connections.get(
        connection_id
    )


    if connection is None:
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


# ============================================================
# CLOSE
# ============================================================

async def handle_close(
    ws,
    message
):

    connection_id = message[
        "connection_id"
    ]


    await wait_for_open(
        connection_id
    )


    connection = connections.pop(
        connection_id,
        None
    )


    if connection is None:
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

        reader, writer = connection


        try:

            writer.close()

            await writer.wait_closed()

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
# Runner
# ============================================================

async def run():

    url = (
        RENDER_WS
        + "?token="
        + TOKEN
    )


    retry_delay = 2


    while True:

        try:

            print(
                "Connecting to Render..."
            )


            async with websockets.connect(

                url,

                ping_interval=20,

                ping_timeout=20,

                close_timeout=5,

                max_size=None,

                open_timeout=30,

                compression=None

            ) as ws:


                print(
                    "Connected to Render"
                )


                # Reset reconnect delay
                retry_delay = 2


                # =================================================
                # Main WebSocket receive loop
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


                    # =============================================
                    # OPEN
                    # =============================================

                    if msg_type == "open":

                        old_task = (
                            opening_connections.get(
                                connection_id
                            )
                        )


                        if (
                            old_task
                            and
                            not old_task.done()
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


                    # =============================================
                    # DATA
                    # =============================================

                    elif msg_type == "data":

                        await handle_data(
                            ws,
                            message
                        )


                    # =============================================
                    # CLOSE
                    # =============================================

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


        except Exception as e:

            print(
                "Render connection error:",
                e
            )


        # ========================================================
        # Cleanup
        # ========================================================

        await cleanup_connections()


        print(
            f"Reconnecting in "
            f"{retry_delay} seconds..."
        )


        await asyncio.sleep(
            retry_delay
        )


        retry_delay = min(
            retry_delay * 2,
            30
        )


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        run()
    )