import os
import asyncio
from typing import Dict

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException
)

from pydantic import BaseModel


# =========================================
# FastAPI
# =========================================

app = FastAPI()


# =========================================
# Configuration
# =========================================

TOKEN = os.environ.get(
    "PROXY_TOKEN",
    "CHANGE_ME"
)

# קצר כדי לא להשאיר HTTP request פתוח
# יותר מדי זמן על Render.
RECV_TIMEOUT = 5


# =========================================
# Connection
# =========================================

class Connection:

    def __init__(self):

        self.to_runner = asyncio.Queue(
            maxsize=100
        )

        self.to_client = asyncio.Queue(
            maxsize=100
        )

        self.closed = False


# =========================================
# Global state
# =========================================

connections: Dict[
    str,
    Connection
] = {}

runner_ws = None

runner_lock = asyncio.Lock()


# =========================================
# Authentication
# =========================================

def check_token(
    token: str
):

    if token != TOKEN:

        raise HTTPException(
            status_code=401,
            detail="Invalid token"
        )


# =========================================
# Models
# =========================================

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


# =========================================
# Root
# =========================================

@app.get("/")
async def root():

    return {
        "status": "online",

        "runner_connected":
            runner_ws is not None,

        "connections":
            len(connections),
    }


# =========================================
# WebSocket
# =========================================

@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket
):

    global runner_ws

    token = websocket.query_params.get(
        "token"
    )

    if token != TOKEN:

        await websocket.close(
            code=1008
        )

        return

    await websocket.accept()

    # =====================================
    # Register Runner
    # =====================================

    async with runner_lock:

        old_ws = runner_ws

        runner_ws = websocket

        if old_ws is not None:

            try:

                await old_ws.close()

            except Exception:
                pass

    print(
        "Runner connected"
    )

    # =====================================
    # Sender
    # =====================================

    async def sender():

        while True:

            if not connections:

                await asyncio.sleep(
                    0.05
                )

                continue

            tasks = []

            task_map = {}

            # ---------------------------------
            # One queue reader per connection
            # ---------------------------------

            for (
                connection_id,
                conn
            ) in list(
                connections.items()
            ):

                if conn.closed:
                    continue

                task = asyncio.create_task(
                    conn.to_runner.get()
                )

                tasks.append(task)

                task_map[task] = (
                    connection_id,
                    conn
                )

            if not tasks:

                await asyncio.sleep(
                    0.05
                )

                continue

            try:

                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )

            except asyncio.CancelledError:

                for task in tasks:

                    task.cancel()

                raise

            # ---------------------------------
            # Cancel remaining queue waits
            # ---------------------------------

            for task in pending:

                task.cancel()

            # ---------------------------------
            # Send messages
            # ---------------------------------

            for task in done:

                try:

                    message = task.result()

                except Exception:

                    continue

                try:

                    await websocket.send_json(
                        message
                    )

                except Exception as e:

                    print(
                        "WebSocket send error:",
                        e
                    )

                    return

    sender_task = asyncio.create_task(
        sender()
    )

    # =====================================
    # Receive from Runner
    # =====================================

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

            if not conn:

                continue

            # =================================
            # DATA
            # =================================

            if msg_type == "data":

                data = message.get(
                    "data",
                    ""
                )

                if data:

                    try:

                        await conn.to_client.put({
                            "type": "data",
                            "data": data,
                        })

                    except Exception as e:

                        print(
                            f"[{connection_id}] "
                            f"queue error:",
                            e
                        )

            # =================================
            # CLOSE
            # =================================

            elif msg_type == "close":

                conn.closed = True

                try:

                    await conn.to_client.put({
                        "type": "close"
                    })

                except Exception:
                    pass

            # =================================
            # ERROR
            # =================================

            elif msg_type == "error":

                conn.closed = True

                try:

                    await conn.to_client.put({
                        "type": "error",

                        "message":
                            message.get(
                                "message",
                                "Runner error"
                            )
                    })

                except Exception:
                    pass

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


# =========================================
# Open
# =========================================

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

    # -------------------------------------
    # Duplicate
    # -------------------------------------

    if connection_id in connections:

        raise HTTPException(
            status_code=409,
            detail="Connection already exists"
        )

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

    print(
        f"[OPEN] "
        f"{connection_id} -> "
        f"{req.host}:{req.port}"
    )

    return {
        "ok": True,

        "connection_id":
            connection_id
    }


# =========================================
# DATA
# =========================================

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

    if not conn:

        raise HTTPException(
            status_code=404,
            detail="Connection not found"
        )

    if conn.closed:

        raise HTTPException(
            status_code=410,
            detail="Connection closed"
        )

    try:

        await conn.to_runner.put({

            "type": "data",

            "connection_id":
                req.connection_id,

            "data":
                req.data,
        })

    except Exception as e:

        raise HTTPException(
            status_code=503,
            detail=str(e)
        )

    return {
        "ok": True
    }


# =========================================
# RECEIVE
# =========================================

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

    if not conn:

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

        # =================================
        # CLOSE
        # =================================

        if message.get(
            "type"
        ) == "close":

            conn.closed = True

            connections.pop(
                connection_id,
                None
            )

        # =================================
        # ERROR
        # =================================

        elif message.get(
            "type"
        ) == "error":

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


# =========================================
# CLOSE
# =========================================

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

    if conn:

        conn.closed = True

        try:

            await conn.to_runner.put({

                "type": "close",

                "connection_id":
                    req.connection_id,
            })

        except Exception:
            pass

        print(
            f"[CLOSE] "
            f"{req.connection_id}"
        )

    return {
        "ok": True
    }