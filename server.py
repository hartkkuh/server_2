import os
import asyncio
import json
import base64
from typing import Dict, Optional

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException,
    Request,
)

from fastapi.responses import StreamingResponse
from pydantic import BaseModel


# ============================================================
# Configuration
# ============================================================

TOKEN = os.environ.get(
    "PROXY_TOKEN",
    "CHANGE_ME"
)

# כמה זמן חיבור HTTPS יכול להיות idle
KEEPALIVE_SECONDS = 15

# גודל queue לכל connection
QUEUE_MAX_SIZE = 256


# ============================================================
# FastAPI
# ============================================================

app = FastAPI()


# ============================================================
# Connection
# ============================================================

class Connection:

    def __init__(self):

        # Windows -> Runner
        self.to_runner = asyncio.Queue(
            maxsize=QUEUE_MAX_SIZE
        )

        # Runner -> Windows
        self.to_client = asyncio.Queue(
            maxsize=QUEUE_MAX_SIZE
        )

        self.closed = False

        self.open_finished = asyncio.Event()

        self.open_success = False

        self.lock = asyncio.Lock()


# ============================================================
# Global state
# ============================================================

connections: Dict[str, Connection] = {}

runner_ws: Optional[WebSocket] = None

runner_lock = asyncio.Lock()


# ============================================================
# Authentication
# ============================================================

def check_token(token: str):

    if token != TOKEN:

        raise HTTPException(
            status_code=401,
            detail="Invalid token"
        )


# ============================================================
# Request models
# ============================================================

class OpenRequest(BaseModel):

    token: str
    connection_id: str
    host: str
    port: int


class CloseRequest(BaseModel):

    token: str
    connection_id: str


# ============================================================
# Root
# ============================================================

@app.get("/")
async def root():

    return {
        "status": "online",
        "runner_connected": runner_ws is not None,
        "connections": len(connections),
    }


# ============================================================
# Send one command to Runner
# ============================================================

async def queue_runner_message(
    connection_id: str,
    message: dict
):

    conn = connections.get(
        connection_id
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

    await conn.to_runner.put(
        message
    )


# ============================================================
# Runner WebSocket
# ============================================================

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

    # ========================================================
    # Register runner
    # ========================================================

    async with runner_lock:

        old_runner = runner_ws

        runner_ws = websocket

        if old_runner is not None:

            try:
                await old_runner.close()
            except Exception:
                pass

    print(
        "Runner connected"
    )

    # ========================================================
    # Sender task
    #
    # Takes messages from all connection queues
    # and sends them to GitHub Runner.
    # ========================================================

    async def sender():

        while True:

            if runner_ws is not websocket:

                return

            current = list(
                connections.items()
            )

            tasks = []

            task_map = {}

            for connection_id, conn in current:

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
                    0.02
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

            for task in pending:

                task.cancel()

            for task in done:

                try:

                    message = task.result()

                except Exception:

                    continue

                try:

                    await websocket.send_text(
                        json.dumps(
                            message,
                            separators=(
                                ",",
                                ":"
                            )
                        )
                    )

                except Exception:

                    return

    sender_task = asyncio.create_task(
        sender()
    )

    # ========================================================
    # Runner receiver
    # ========================================================

    try:

        while True:

            raw = await websocket.receive_text()

            try:

                message = json.loads(
                    raw
                )

            except Exception:

                continue

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

            # =================================================
            # Runner DATA -> Windows
            # =================================================

            if msg_type == "data":

                encoded = message.get(
                    "data",
                    ""
                )

                if encoded:

                    await conn.to_client.put(
                        {
                            "type": "data",
                            "data": encoded,
                        }
                    )

            # =================================================
            # Runner OPEN result
            # =================================================

            elif msg_type == "open_ok":

                conn.open_success = True

                conn.open_finished.set()

            # =================================================
            # Runner ERROR
            # =================================================

            elif msg_type == "error":

                conn.open_success = False

                conn.closed = True

                conn.open_finished.set()

                await conn.to_client.put(
                    {
                        "type": "error",
                        "message": message.get(
                            "message",
                            "Runner error"
                        )
                    }
                )

            # =================================================
            # Runner CLOSE
            # =================================================

            elif msg_type == "close":

                conn.closed = True

                conn.open_finished.set()

                await conn.to_client.put(
                    {
                        "type": "close"
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

            if runner_ws is websocket:

                runner_ws = None

        # ====================================================
        # Do NOT immediately delete all connections.
        #
        # Runner reconnects and new connections can continue.
        # Existing TCP connections will eventually timeout/close.
        # ====================================================

        print(
            "Runner connection closed"
        )


# ============================================================
# OPEN
# ============================================================

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

    if req.connection_id in connections:

        raise HTTPException(
            status_code=409,
            detail="Connection already exists"
        )

    conn = Connection()

    connections[
        req.connection_id
    ] = conn

    await conn.to_runner.put(
        {
            "type": "open",
            "connection_id":
                req.connection_id,
            "host":
                req.host,
            "port":
                req.port,
        }
    )

    print(
        f"[OPEN] "
        f"{req.connection_id} -> "
        f"{req.host}:{req.port}"
    )

    # ========================================================
    # Wait until Runner actually connected to destination.
    #
    # This fixes the important OPEN/DATA race.
    # ========================================================

    try:

        await asyncio.wait_for(
            conn.open_finished.wait(),
            timeout=30
        )

    except asyncio.TimeoutError:

        connections.pop(
            req.connection_id,
            None
        )

        raise HTTPException(
            status_code=504,
            detail="Runner open timeout"
        )

    if not conn.open_success:

        connections.pop(
            req.connection_id,
            None
        )

        raise HTTPException(
            status_code=502,
            detail="Runner failed to open connection"
        )

    return {
        "ok": True
    }


# ============================================================
# STREAM: Windows -> Runner
#
# Client sends NDJSON continuously.
#
# Each line:
#
# {"data":"BASE64..."}
#
# ============================================================

@app.post(
    "/tunnel/{connection_id}/up"
)
async def tunnel_up(
    connection_id: str,
    request: Request,
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

    async def process_stream():

        buffer = b""

        try:

            async for chunk in request.stream():

                if not chunk:
                    continue

                buffer += chunk

                while b"\n" in buffer:

                    line, buffer = buffer.split(
                        b"\n",
                        1
                    )

                    if not line:
                        continue

                    try:

                        message = json.loads(
                            line.decode(
                                "utf-8"
                            )
                        )

                    except Exception:

                        continue

                    encoded = message.get(
                        "data",
                        ""
                    )

                    if not encoded:
                        continue

                    if conn.closed:
                        return

                    await conn.to_runner.put(
                        {
                            "type": "data",
                            "connection_id":
                                connection_id,
                            "data": encoded,
                        }
                    )

        except asyncio.CancelledError:

            raise

        except Exception as e:

            print(
                f"[UP ERROR] "
                f"{connection_id}:",
                e
            )

        finally:

            # Browser closed upload stream
            if not conn.closed:

                await conn.to_runner.put(
                    {
                        "type": "close",
                        "connection_id":
                            connection_id,
                    }
                )

    await process_stream()

    return {
        "ok": True
    }


# ============================================================
# STREAM: Runner -> Windows
#
# This connection stays open.
# ============================================================

@app.get(
    "/tunnel/{connection_id}/down"
)
async def tunnel_down(
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

    async def stream():

        try:

            while True:

                try:

                    message = await asyncio.wait_for(
                        conn.to_client.get(),
                        timeout=KEEPALIVE_SECONDS
                    )

                    payload = (
                        json.dumps(
                            message,
                            separators=(
                                ",",
                                ":"
                            )
                        )
                        + "\n"
                    )

                    yield payload.encode(
                        "utf-8"
                    )

                    if message.get(
                        "type"
                    ) in (
                        "close",
                        "error"
                    ):

                        break

                except asyncio.TimeoutError:

                    # Keep-alive
                    yield b'{"type":"ping"}\n'

                if conn.closed:

                    break

        except asyncio.CancelledError:

            raise

        except Exception as e:

            print(
                f"[DOWN ERROR] "
                f"{connection_id}:",
                e
            )

        finally:

            connections.pop(
                connection_id,
                None
            )

    return StreamingResponse(
        stream(),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control":
                "no-cache",
            "X-Accel-Buffering":
                "no",
            "Connection":
                "keep-alive",
        }
    )


# ============================================================
# CLOSE
# ============================================================

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

        conn.open_finished.set()

        if runner_ws is not None:

            try:

                await conn.to_runner.put(
                    {
                        "type": "close",
                        "connection_id":
                            req.connection_id,
                    }
                )

            except Exception:
                pass

        print(
            f"[CLOSE] "
            f"{req.connection_id}"
        )

    return {
        "ok": True
    }