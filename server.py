import os
import asyncio
from typing import Dict, Optional

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException
)

from pydantic import BaseModel


# =========================================================
# FastAPI
# =========================================================

app = FastAPI()


# =========================================================
# Configuration
# =========================================================

TOKEN = os.environ.get(
    "PROXY_TOKEN",
    "CHANGE_ME"
)

RECV_TIMEOUT = 20


# =========================================================
# Connection
# =========================================================

class Connection:

    def __init__(self):

        self.to_runner = asyncio.Queue()

        self.to_client = asyncio.Queue()

        self.closed = False

        self.created = asyncio.get_running_loop().time()


# =========================================================
# Global state
# =========================================================

connections: Dict[
    str,
    Connection
] = {}

runner_ws: Optional[
    WebSocket
] = None

runner_lock = asyncio.Lock()

runner_wakeup = asyncio.Event()


# =========================================================
# Authentication
# =========================================================

def check_token(
    token: str
):

    if token != TOKEN:

        raise HTTPException(
            status_code=401,
            detail="Invalid token"
        )


# =========================================================
# Models
# =========================================================
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


# =========================================================
# Root
# =========================================================

@app.get("/")
async def root():

    return {
        "status": "online",
        "runner_connected": runner_ws is not None,
        "connections": len(connections),
    }


# =========================================================
# WebSocket
# =========================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    global runner_ws

    token = websocket.query_params.get("token")

    if token != TOKEN:

        await websocket.close(
            code=1008
        )

        return

    await websocket.accept()

    # =====================================================
    # Register Runner
    # =====================================================

    async with runner_lock:

        old_ws = runner_ws

        runner_ws = websocket

        if old_ws is not None:

            try:

                await old_ws.close(
                    code=1000
                )

            except Exception:
                pass

    print(
        "Runner connected"
    )

    # =====================================================
    # Sender
    # =====================================================

    async def sender():

        while True:

            if runner_ws is not websocket:
                return

            await runner_wakeup.wait()
            runner_wakeup.clear()

            while True:

                sent = False

                for connection_id, conn in list(connections.items()):

                    if conn.closed:
                        continue

                    for _ in range(64):

                        try:
                            message = conn.to_runner.get_nowait()
                        except asyncio.QueueEmpty:
                            break

                        try:
                            await websocket.send_json(message)
                            sent = True
                        except Exception:
                            return

                if not sent:
                    break

    sender_task = asyncio.create_task(
        sender()
    )

    # =====================================================
    # Receive from Runner
    # =====================================================

    try:

        while True:

            message = await websocket.receive_json()

            msg_type = message.get(
                "type"
            )

            connection_id = message.get(
                "connection_id"
            )

            if not connection_id:
                continue

            conn = connections.get(
                connection_id
            )

            if conn is None:
                continue

            # =================================================
            # DATA
            # =================================================

            if msg_type == "data":

                data = message.get(
                    "data",
                    ""
                )

                if data:

                    await conn.to_client.put({
                        "type": "data",
                        "data": data,
                    })

            # =================================================
            # CLOSE
            # =================================================

            elif msg_type == "close":

                conn.closed = True

                await conn.to_client.put({
                    "type": "close"
                })

            # =================================================
            # ERROR
            # =================================================

            elif msg_type == "error":

                conn.closed = True

                await conn.to_client.put({
                    "type": "error",
                    "message": message.get(
                        "message",
                        "Runner error"
                    )
                })

    except WebSocketDisconnect:

        print(
            "Runner disconnected"
        )

    except Exception as e:

        print(
            "WebSocket error:",
            e
        )

    finally:

        sender_task.cancel()

        try:
            await sender_task
        except BaseException:
            pass

        async with runner_lock:

            if runner_ws is websocket:

                runner_ws = None

        print(
            "Runner connection closed"
        )


# =========================================================
# OPEN
# =========================================================

@app.post("/open")
async def open_connection(
    req: OpenRequest
):

    check_token(
        req.token
    )

    if runner_ws is None:

        raise HTTPException(
            status_code=503,
            detail="Runner is not connected"
        )

    connection_id = req.connection_id

    # If an old connection with the same ID exists,
    # remove it first.

    old = connections.pop(
        connection_id,
        None
    )

    if old is not None:

        old.closed = True

    conn = Connection()

    connections[
        connection_id
    ] = conn

    await conn.to_runner.put({
        "type": "open",
        "connection_id":
            connection_id,
        "host":
            req.host,
        "port":
            req.port,
    })
    runner_wakeup.set()

    print(
        f"[OPEN] "
        f"{connection_id} -> "
        f"{req.host}:{req.port}"
    )

    return {
        "ok": True,
        "connection_id":
            connection_id,
    }


# =========================================================
# DATA
# =========================================================

@app.post("/data")
async def send_data(
    req: DataRequest
):

    check_token(
        req.token
    )

    conn = connections.get(
        req.connection_id
    )

    if conn is None:

        raise HTTPException(
            status_code=404,
            detail="Connection not found"
        )

    if conn.closed:

        raise HTTPException(
            status_code=410,
            detail="Connection closed"
        )

    await conn.to_runner.put({
        "type": "data",
        "connection_id":
            req.connection_id,
        "data":
            req.data,
    })
    runner_wakeup.set()

    return {
        "ok": True
    }


# =========================================================
# RECEIVE
# =========================================================

@app.get(
    "/recv/{connection_id}"
)
async def receive_data(
    connection_id: str,
    token: str
):

    check_token(
        token
    )

    conn = connections.get(
        connection_id
    )

    if conn is None:

        raise HTTPException(
            status_code=404,
            detail="Connection not found"
        )

    if conn.closed:

        connections.pop(
            connection_id,
            None
        )

        return {
            "type": "close"
        }

    try:

        message = await asyncio.wait_for(
            conn.to_client.get(),
            timeout=RECV_TIMEOUT
        )

        msg_type = message.get(
            "type"
        )

        if msg_type in (
            "close",
            "error"
        ):

            conn.closed = True

            connections.pop(
                connection_id,
                None
            )

        return message

    except asyncio.TimeoutError:

        return {
            "type": "timeout"
        }


# =========================================================
# CLOSE
# =========================================================

@app.post("/close")
async def close_connection(
    req: CloseRequest
):

    check_token(
        req.token
    )

    conn = connections.pop(
        req.connection_id,
        None
    )

    if conn is not None:

        conn.closed = True

        # Notify Runner even if it is temporarily
        # unavailable.

        if runner_ws is not None:

            try:

                await conn.to_runner.put({
                    "type": "close",
                    "connection_id":
                        req.connection_id,
                })
                runner_wakeup.set()

            except Exception:
                pass

        print(
            f"[CLOSE] "
            f"{req.connection_id}"
        )

    return {
        "ok": True
    }