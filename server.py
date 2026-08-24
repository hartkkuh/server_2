import asyncio
import json
import os
import uuid

from fastapi import FastAPI, WebSocket

app = FastAPI()

TOKEN = os.environ["RELAY_TOKEN"]

runner_control = None

pending_clients = {}
active_bridges = {}

lock = asyncio.Lock()


@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "proxy relay",
    }


async def auth(ws, expected_role):

    try:

        msg = await asyncio.wait_for(
            ws.receive_text(),
            timeout=15,
        )

        data = json.loads(msg)

        if data.get("type") != "auth":
            return False

        if data.get("token") != TOKEN:
            return False

        if data.get("role") != expected_role:
            return False

        return True

    except Exception:
        return False


async def relay(a, b):

    async def a_to_b():

        try:

            while True:

                msg = await a.receive()

                if msg.get("bytes") is not None:
                    await b.send_bytes(msg["bytes"])

                elif msg.get("text") is not None:
                    await b.send_text(msg["text"])

                elif msg.get("type") == "websocket.disconnect":
                    break

        except Exception:
            pass


    async def b_to_a():

        try:

            while True:

                msg = await b.receive()

                if msg.get("bytes") is not None:
                    await a.send_bytes(msg["bytes"])

                elif msg.get("text") is not None:
                    await a.send_text(msg["text"])

                elif msg.get("type") == "websocket.disconnect":
                    break

        except Exception:
            pass


    await asyncio.gather(
        a_to_b(),
        b_to_a(),
    )


@app.websocket("/ws/control")
async def control(ws: WebSocket):

    global runner_control

    await ws.accept()

    if not await auth(ws, "runner"):
        await ws.close(code=1008)
        return

    async with lock:
        runner_control = ws

    print("Runner connected", flush=True)

    try:

        while True:

            msg = await ws.receive()

            if msg.get("type") == "websocket.disconnect":
                break

    except Exception as e:

        print(
            "Control error:",
            repr(e),
            flush=True,
        )

    finally:

        async with lock:

            if runner_control is ws:
                runner_control = None

        print(
            "Runner disconnected",
            flush=True,
        )


@app.websocket("/ws/client")
async def client(ws: WebSocket):

    await ws.accept()

    if not await auth(ws, "client"):
        await ws.close(code=1008)
        return

    async with lock:
        runner = runner_control

    if runner is None:

        await ws.send_text(
            json.dumps({
                "type": "error",
                "message": "No runner connected",
            })
        )

        await ws.close(code=1013)
        return

    client_id = uuid.uuid4().hex

    async with lock:
        pending_clients[client_id] = ws

    print(
        "Client:",
        client_id,
        flush=True,
    )

    try:

        await runner.send_text(
            json.dumps({
                "type": "connect",
                "id": client_id,
            })
        )

        # Wait for runner bridge.
        for _ in range(300):

            async with lock:

                bridge = active_bridges.get(
                    client_id
                )

            if bridge is not None:
                break

            await asyncio.sleep(0.1)

        else:

            await ws.send_text(
                json.dumps({
                    "type": "error",
                    "message": "Runner bridge timeout",
                })
            )

            return

        await ws.send_text(
            json.dumps({
                "type": "connected",
            })
        )

        async with lock:
            pending_clients.pop(
                client_id,
                None,
            )

    except Exception as e:

        print(
            "Client error:",
            repr(e),
            flush=True,
        )

    finally:

        async with lock:

            pending_clients.pop(
                client_id,
                None,
            )

            active_bridges.pop(
                client_id,
                None,
            )

        try:
            await ws.close()
        except Exception:
            pass


@app.websocket("/ws/bridge/{client_id}")
async def bridge(
    ws: WebSocket,
    client_id: str,
):

    await ws.accept()

    if not await auth(ws, "bridge"):
        await ws.close(code=1008)
        return

    async with lock:

        client = pending_clients.get(
            client_id
        )

        if client is None:
            await ws.close(code=1008)
            return

        active_bridges[client_id] = ws

    print(
        "Bridge connected:",
        client_id,
        flush=True,
    )

    try:

        await client.send_text(
            json.dumps({
                "type": "connected",
            })
        )

        await relay(
            client,
            ws,
        )

    except Exception as e:

        print(
            "Bridge error:",
            repr(e),
            flush=True,
        )

    finally:

        async with lock:

            pending_clients.pop(
                client_id,
                None,
            )

            active_bridges.pop(
                client_id,
                None,
            )

        try:
            await client.close()
        except Exception:
            pass

        try:
            await ws.close()
        except Exception:
            pass