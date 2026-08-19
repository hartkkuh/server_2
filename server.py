import asyncio
import json
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

TOKEN = os.environ["RELAY_TOKEN"]

runner_pool = asyncio.Queue()


@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "proxy relay",
    }


async def authenticate(ws: WebSocket):
    try:
        message = await ws.receive_text()
        data = json.loads(message)

        if data.get("type") != "auth":
            return None

        if data.get("token") != TOKEN:
            return None

        return data.get("role")

    except Exception:
        return None


async def relay_websockets(
    client: WebSocket,
    runner: WebSocket,
):
    async def client_to_runner():
        try:
            while True:
                message = await client.receive()

                if message.get("bytes") is not None:
                    await runner.send_bytes(message["bytes"])

                elif message.get("text") is not None:
                    await runner.send_text(message["text"])

        except Exception:
            pass

    async def runner_to_client():
        try:
            while True:
                message = await runner.receive()

                if message.get("bytes") is not None:
                    await client.send_bytes(message["bytes"])

                elif message.get("text") is not None:
                    await client.send_text(message["text"])

        except Exception:
            pass

    await asyncio.gather(
        client_to_runner(),
        runner_to_client(),
    )


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):

    await ws.accept()

    role = await authenticate(ws)

    if role not in ("runner", "client"):
        await ws.close(code=1008)
        return

    # ---------------------------------
    # GitHub Actions Runner
    # ---------------------------------

    if role == "runner":

        try:
            while True:

                await runner_pool.put(ws)

                # Wait until this runner is selected.
                await ws.receive_text()

        except Exception:
            return

    # ---------------------------------
    # Windows client
    # ---------------------------------

    if role == "client":

        try:
            runner = await asyncio.wait_for(
                runner_pool.get(),
                timeout=30,
            )
        except asyncio.TimeoutError:

            await ws.send_text(
                json.dumps({
                    "type": "error",
                    "message": "No runner available"
                })
            )

            await ws.close()
            return

        try:

            await runner.send_text(
                json.dumps({
                    "type": "connect"
                })
            )

            await ws.send_text(
                json.dumps({
                    "type": "connect"
                })
            )

            await relay_websockets(ws, runner)

        except Exception:
            pass

        finally:

            try:
                await runner.close()
            except Exception:
                pass

            try:
                await ws.close()
            except Exception:
                pass