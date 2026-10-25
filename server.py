import os
import asyncio
import base64
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

MAX_CONNECTIONS = 32

QUEUE_SIZE = 32

MAX_BATCH_BYTES = 256 * 1024


# =========================================================
# Connection
# =========================================================

class Connection:

    def __init__(self):

        self.to_runner = asyncio.Queue(
            maxsize=QUEUE_SIZE
        )

        self.to_client = asyncio.Queue(
            maxsize=QUEUE_SIZE
        )

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


def enqueue_client(
    conn: Connection,
    message: dict
):

    try:

        conn.to_client.put_nowait(
            message
        )

    except asyncio.QueueFull:

        conn.closed = True


async def fail_all_connections(
    reason: str
):

    dropped = 0

    for connection_id, conn in list(
        connections.items()
    ):

        conn.closed = True

        enqueue_client(
            conn,
            {
                "type": "error",
                "message": reason,
            }
        )

        current = connections.get(
            connection_id
        )

        if current is conn:

            connections.pop(
                connection_id,
                None
            )

            dropped += 1

    if dropped:

        print(
            f"Dropped {dropped} tunnels: "
            f"{reason}"
        )


def decode_chunk(
    message: dict
):

    encoded = message.get(
        "data"
    ) or ""

    if not encoded:

        return b""

    try:

        return base64.b64decode(
            encoded
        )

    except Exception:

        return b""


def pack_messages(
    conn: Connection,
    connection_id: str,
    message: dict
):

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

    if msg_type != "data":

        return message

    chunks = []

    raw_len = 0

    pending_end = None

    first = decode_chunk(
        message
    )

    if first:

        chunks.append(
            first
        )

        raw_len += len(
            first
        )

    while raw_len < MAX_BATCH_BYTES:

        try:

            nxt = conn.to_client.get_nowait()

        except asyncio.QueueEmpty:

            break

        ntype = nxt.get(
            "type"
        )

        if ntype == "data":

            blob = decode_chunk(
                nxt
            )

            if not blob:

                continue

            chunks.append(
                blob
            )

            raw_len += len(
                blob
            )

            continue

        if ntype in (
            "close",
            "error"
        ):

            pending_end = nxt

            break

        break

    if (
        pending_end is not None
        or conn.closed
    ):

        conn.closed = True

    if (
        conn.closed
        and conn.to_client.empty()
    ):

        connections.pop(
            connection_id,
            None
        )

    payload = base64.b64encode(
        b"".join(chunks)
    ).decode(
        "ascii"
    )

    body = {
        "type": "data",
        "data": payload,
    }

    if pending_end is not None:

        body["closed"] = True

        if pending_end.get(
            "type"
        ) == "error":

            body["message"] = pending_end.get(
                "message",
                ""
            )

    elif (
        conn.closed
        and conn.to_client.empty()
    ):

        body["closed"] = True

    return body


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

    # המנהרות הישנות שייכות לסוקט הקודם.
    # ה-runner החדש לא מכיר אותן, אז סוגרים
    # אותן מיד במקום להשחיל אליהן מידע לריק.
    await fail_all_connections(
        "Runner replaced"
    )

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
                            try:
                                await websocket.close()
                            except Exception:
                                pass
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

                    enqueue_client(
                        conn,
                        {
                            "type": "data",
                            "data": data,
                        }
                    )

                    if conn.closed:

                        try:

                            conn.to_runner.put_nowait({
                                "type": "close",
                                "connection_id":
                                    connection_id,
                            })

                            runner_wakeup.set()

                        except asyncio.QueueFull:
                            pass

            # =================================================
            # CLOSE
            # =================================================

            elif msg_type == "close":

                conn.closed = True

                enqueue_client(
                    conn,
                    {
                        "type": "close"
                    }
                )

            # =================================================
            # ERROR
            # =================================================

            elif msg_type == "error":

                conn.closed = True

                enqueue_client(
                    conn,
                    {
                        "type": "error",
                        "message": message.get(
                            "message",
                            "Runner error"
                        )
                    }
                )

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

            owner = runner_ws is websocket

            if owner:

                runner_ws = None

        if owner:

            await fail_all_connections(
                "Runner disconnected"
            )

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

    if len(connections) >= MAX_CONNECTIONS:

        raise HTTPException(
            status_code=503,
            detail="Too many connections"
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

    try:

        await asyncio.wait_for(
            conn.to_runner.put({
                "type": "open",
                "connection_id":
                    connection_id,
                "host":
                    req.host,
                "port":
                    req.port,
            }),
            timeout=8
        )

    except asyncio.TimeoutError:

        conn.closed = True

        connections.pop(
            connection_id,
            None
        )

        raise HTTPException(
            status_code=429,
            detail="Connection congested"
        )

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

    try:

        await asyncio.wait_for(
            conn.to_runner.put({
                "type": "data",
                "connection_id":
                    req.connection_id,
                "data":
                    req.data,
            }),
            timeout=8
        )

    except asyncio.TimeoutError:

        raise HTTPException(
            status_code=429,
            detail="Connection congested"
        )

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

    if (
        conn.closed
        and conn.to_client.empty()
    ):

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

    except asyncio.TimeoutError:

        if (
            conn.closed
            and conn.to_client.empty()
        ):

            connections.pop(
                connection_id,
                None
            )

            return {
                "type": "close"
            }

        return {
            "type": "timeout"
        }

    return pack_messages(
        conn,
        connection_id,
        message
    )


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

        # מעיר את /recv שמחכה על תור ריק,
        # אחרת הבקשה נשארת תפוסה עד ה-timeout.
        enqueue_client(
            conn,
            {
                "type": "close"
            }
        )

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