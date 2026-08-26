import os
import asyncio
import base64
import uuid
from typing import Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel


app = FastAPI()


TOKEN = os.environ.get("PROXY_TOKEN", "CHANGE_ME")


class Connection:
    def __init__(self):
        self.to_runner = asyncio.Queue()
        self.to_client = asyncio.Queue()
        self.closed = False


connections: Dict[str, Connection] = {}

runner_ws = None
runner_lock = asyncio.Lock()


def check_token(token: str):
    if token != TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")


class OpenRequest(BaseModel):
    token: str
    connection_id: str
    host: str
    port: int


class DataRequest(BaseModel):
    token: str
    connection_id: str
    data: str


class CloseRequest(BaseModel):
    token: str
    connection_id: str


@app.get("/")
async def root():
    return {
        "status": "online",
        "runner_connected": runner_ws is not None,
        "connections": len(connections),
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    global runner_ws

    token = websocket.query_params.get("token")

    if token != TOKEN:
        await websocket.close(code=1008)
        return

    await websocket.accept()

    async with runner_lock:

        if runner_ws is not None:
            try:
                await runner_ws.close()
            except Exception:
                pass

        runner_ws = websocket

    print("Runner connected")

    async def sender():

        while True:

            # Wait for something that needs to be sent to runner
            tasks = []

            for connection_id, conn in list(connections.items()):

                tasks.append(
                    asyncio.create_task(
                        conn.to_runner.get()
                    )
                )

            if not tasks:
                await asyncio.sleep(0.05)
                continue

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

            for task in done:

                try:
                    message = task.result()
                except Exception:
                    continue

                try:
                    await websocket.send_json(message)
                except Exception:
                    return

    sender_task = asyncio.create_task(sender())

    try:

        while True:

            message = await websocket.receive_json()

            msg_type = message.get("type")
            connection_id = message.get("connection_id")

            if not connection_id:
                continue

            conn = connections.get(connection_id)

            if not conn:
                continue

            if msg_type == "data":

                data = message.get("data", "")

                await conn.to_client.put({
                    "type": "data",
                    "data": data,
                })

            elif msg_type == "close":

                await conn.to_client.put({
                    "type": "close"
                })

                conn.closed = True

            elif msg_type == "error":

                await conn.to_client.put({
                    "type": "error",
                    "message": message.get("message", "Runner error")
                })

                conn.closed = True

    except WebSocketDisconnect:
        print("Runner disconnected")

    except Exception as e:
        print("WebSocket error:", e)

    finally:

        sender_task.cancel()

        async with runner_lock:

            if runner_ws is websocket:
                runner_ws = None

        print("Runner connection closed")


@app.post("/open")
async def open_connection(req: OpenRequest):

    check_token(req.token)

    if runner_ws is None:
        raise HTTPException(
            status_code=503,
            detail="Runner is not connected"
        )

    connection_id = req.connection_id

    if connection_id in connections:
        raise HTTPException(
            status_code=409,
            detail="Connection already exists"
        )

    conn = Connection()

    connections[connection_id] = conn

    await conn.to_runner.put({
        "type": "open",
        "connection_id": connection_id,
        "host": req.host,
        "port": req.port,
    })

    return {
        "ok": True,
        "connection_id": connection_id,
    }


@app.post("/data")
async def send_data(req: DataRequest):

    check_token(req.token)

    conn = connections.get(req.connection_id)

    if not conn:
        raise HTTPException(
            status_code=404,
            detail="Connection not found"
        )

    await conn.to_runner.put({
        "type": "data",
        "connection_id": req.connection_id,
        "data": req.data,
    })

    return {"ok": True}


@app.get("/recv/{connection_id}")
async def receive_data(
    connection_id: str,
    token: str
):

    check_token(token)

    conn = connections.get(connection_id)

    if not conn:
        raise HTTPException(
            status_code=404,
            detail="Connection not found"
        )

    try:

        # Long polling.
        # Wait up to 25 seconds for data.

        message = await asyncio.wait_for(
            conn.to_client.get(),
            timeout=25
        )

        return message

    except asyncio.TimeoutError:

        return {
            "type": "timeout"
        }


@app.post("/close")
async def close_connection(req: CloseRequest):

    check_token(req.token)

    conn = connections.pop(req.connection_id, None)

    if conn:

        conn.closed = True

        if runner_ws is not None:

            try:

                await conn.to_runner.put({
                    "type": "close",
                    "connection_id": req.connection_id,
                })

            except Exception:
                pass

    return {
        "ok": True
    }