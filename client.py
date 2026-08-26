import socket
import threading
import uuid
import base64
import json
import requests
import urllib3
import queue
import time


# ============================================================
# Configuration
# ============================================================

RENDER_URL = (
    "https://server-2-vddr.onrender.com"
)

TOKEN = "131232"

LOCAL_HOST = "127.0.0.1"

LOCAL_PORT = 3128


# ============================================================
# Performance
# ============================================================

BUFFER_SIZE = 65536

CONNECT_TIMEOUT = 20

HTTP_TIMEOUT = 120

VERIFY_SSL = False


# ============================================================
# Queue size
# ============================================================

UPLOAD_QUEUE_SIZE = 256


if not VERIFY_SSL:

    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# ============================================================
# HTTP Session
# ============================================================

def create_session():

    session = requests.Session()

    session.verify = VERIFY_SSL

    session.headers.update({
        "User-Agent":
            "HTTPS-Tunnel-Proxy/3.0",
        "Connection":
            "keep-alive",
    })

    return session


# ============================================================
# Render URL
# ============================================================

def render_url(path):

    return (
        RENDER_URL.rstrip("/")
        + path
    )


# ============================================================
# OPEN
# ============================================================

def render_open(
    connection_id,
    host,
    port
):

    response = requests.post(

        render_url("/open"),

        json={
            "token":
                TOKEN,
            "connection_id":
                connection_id,
            "host":
                host,
            "port":
                port,
        },

        timeout=(
            CONNECT_TIMEOUT,
            HTTP_TIMEOUT
        ),

        verify=VERIFY_SSL,

        headers={
            "User-Agent":
                "HTTPS-Tunnel-Proxy/3.0",
            "Connection":
                "keep-alive",
        }
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# CLOSE
# ============================================================

def render_close(
    connection_id
):

    try:

        requests.post(

            render_url("/close"),

            json={
                "token":
                    TOKEN,
                "connection_id":
                    connection_id,
            },

            timeout=(
                5,
                15
            ),

            verify=VERIFY_SSL,

            headers={
                "User-Agent":
                    "HTTPS-Tunnel-Proxy/3.0",
                "Connection":
                    "close",
            }
        )

    except Exception:
        pass


# ============================================================
# Encode upload message
# ============================================================

def encode_data(data):

    return (
        json.dumps(
            {
                "data":
                    base64.b64encode(
                        data
                    ).decode(
                        "ascii"
                    )
            },
            separators=(
                ",",
                ":"
            )
        )
        + "\n"
    ).encode(
        "utf-8"
    )


# ============================================================
# UPLOAD STREAM
#
# Windows -> Render
# ============================================================

def upload_worker(
    connection_id,
    upload_queue,
    stop_event
):

    session = create_session()

    def generator():

        while not stop_event.is_set():

            try:

                item = upload_queue.get(
                    timeout=5
                )

            except queue.Empty:

                # Keep connection alive
                yield b'{"ping":1}\n'

                continue

            if item is None:

                break

            try:

                yield encode_data(
                    item
                )

            except Exception as e:

                print(
                    f"[{connection_id}] "
                    f"Upload encode error:",
                    e
                )

                break

    try:

        response = session.post(

            render_url(
                f"/tunnel/{connection_id}/up"
            ),

            params={
                "token":
                    TOKEN
            },

            data=generator(),

            stream=True,

            timeout=(
                CONNECT_TIMEOUT,
                HTTP_TIMEOUT
            ),

            verify=VERIFY_SSL,

            headers={
                "Content-Type":
                    "application/x-ndjson",
                "Cache-Control":
                    "no-cache",
                "Connection":
                    "keep-alive",
                "Accept":
                    "application/json",
            }
        )

        response.raise_for_status()

        # Consume response so requests
        # properly maintains connection.

        for _ in response.iter_content(
            chunk_size=1024
        ):

            if stop_event.is_set():
                break

    except Exception as e:

        if not stop_event.is_set():

            print(
                f"[{connection_id}] "
                f"Upload stream error:",
                e
            )

    finally:

        try:
            response.close()
        except Exception:
            pass

        session.close()


# ============================================================
# DOWNSTREAM STREAM
#
# Render -> Windows
# ============================================================

def download_worker(
    client_socket,
    connection_id,
    stop_event
):

    session = create_session()

    response = None

    try:

        response = session.get(

            render_url(
                f"/tunnel/{connection_id}/down"
            ),

            params={
                "token":
                    TOKEN
            },

            stream=True,

            timeout=(
                CONNECT_TIMEOUT,
                HTTP_TIMEOUT
            ),

            verify=VERIFY_SSL,

            headers={
                "Accept":
                    "application/x-ndjson",
                "Cache-Control":
                    "no-cache",
                "Connection":
                    "keep-alive",
            }
        )

        response.raise_for_status()

        buffer = b""

        for chunk in response.iter_content(
            chunk_size=8192
        ):

            if stop_event.is_set():
                break

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

                msg_type = message.get(
                    "type"
                )

                # =========================================
                # Keepalive
                # =========================================

                if msg_type == "ping":

                    continue

                # =========================================
                # DATA
                # =========================================

                if msg_type == "data":

                    encoded = message.get(
                        "data",
                        ""
                    )

                    if not encoded:
                        continue

                    try:

                        data = base64.b64decode(
                            encoded
                        )

                    except Exception:

                        continue

                    if data:

                        client_socket.sendall(
                            data
                        )

                # =========================================
                # CLOSE
                # =========================================

                elif msg_type == "close":

                    print(
                        f"[{connection_id}] "
                        f"Remote closed connection"
                    )

                    stop_event.set()

                    return

                # =========================================
                # ERROR
                # =========================================

                elif msg_type == "error":

                    print(
                        f"[{connection_id}] "
                        f"Remote error:",
                        message.get(
                            "message"
                        )
                    )

                    stop_event.set()

                    return

    except Exception as e:

        if not stop_event.is_set():

            print(
                f"[{connection_id}] "
                f"Download stream error:",
                e
            )

    finally:

        stop_event.set()

        try:

            if response:

                response.close()

        except Exception:
            pass

        session.close()

        try:

            client_socket.shutdown(
                socket.SHUT_RDWR
            )

        except Exception:
            pass


# ============================================================
# Parse HTTP proxy request
# ============================================================

def parse_request(
    client_socket
):

    client_socket.settimeout(
        CONNECT_TIMEOUT
    )

    data = b""

    while b"\r\n\r\n" not in data:

        chunk = client_socket.recv(
            8192
        )

        if not chunk:

            return None

        data += chunk

        if len(data) > 65536:

            raise ValueError(
                "HTTP header too large"
            )

    header_end = data.find(
        b"\r\n\r\n"
    )

    header = data[
        :header_end + 4
    ]

    remaining = data[
        header_end + 4:
    ]

    lines = header.decode(
        "iso-8859-1"
    ).split(
        "\r\n"
    )

    if not lines:

        return None

    parts = lines[0].split()

    if len(parts) < 2:

        return None

    method = parts[0].upper()

    target = parts[1]

    # ========================================================
    # CONNECT
    # ========================================================

    if method == "CONNECT":

        if ":" in target:

            host, port = target.rsplit(
                ":",
                1
            )

            port = int(
                port
            )

        else:

            host = target

            port = 443

        return (
            method,
            host,
            port,
            True,
            remaining
        )

    # ========================================================
    # HTTP
    # ========================================================

    host = None

    port = 80

    for line in lines[1:]:

        if line.lower().startswith(
            "host:"
        ):

            host = line[
                5:
            ].strip()

            break

    if not host:

        return None

    if ":" in host:

        host, port_string = host.rsplit(
            ":",
            1
        )

        port = int(
            port_string
        )

    return (
        method,
        host,
        port,
        False,
        remaining
    )


# ============================================================
# Handle one browser connection
# ============================================================

def handle_client(
    client_socket
):

    connection_id = str(
        uuid.uuid4()
    )

    stop_event = threading.Event()

    upload_queue = queue.Queue(
        maxsize=UPLOAD_QUEUE_SIZE
    )

    upload_thread = None

    download_thread = None

    try:

        parsed = parse_request(
            client_socket
        )

        if not parsed:

            return

        (
            method,
            host,
            port,
            is_connect,
            remaining
        ) = parsed

        print(
            f"[{connection_id}] "
            f"{method} "
            f"{host}:{port}"
        )

        # ====================================================
        # Open destination on Runner
        # ====================================================

        render_open(
            connection_id,
            host,
            port
        )

        # ====================================================
        # HTTPS CONNECT
        # ====================================================

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: HTTPS-Tunnel-Proxy/3.0\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n"
            )

        # ====================================================
        # HTTP request
        # ====================================================

        else:

            # For ordinary HTTP proxy requests,
            # forward the original HTTP request.
            #
            # This preserves paths such as:
            #
            # http://example.com/test?a=123

            # Reconstruct original request
            # from available data.

            request = (
                f"{method} "
                f"http://{host}/ "
                f"HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            ).encode(
                "iso-8859-1"
            )

            upload_queue.put(
                request
            )

        # ====================================================
        # Start downstream
        # ====================================================

        download_thread = threading.Thread(
            target=download_worker,
            args=(
                client_socket,
                connection_id,
                stop_event
            ),
            daemon=True
        )

        download_thread.start()

        # ====================================================
        # Start upstream
        # ====================================================

        upload_thread = threading.Thread(
            target=upload_worker,
            args=(
                connection_id,
                upload_queue,
                stop_event
            ),
            daemon=True
        )

        upload_thread.start()

        # ====================================================
        # Send data already received
        # ====================================================

        if remaining:

            upload_queue.put(
                remaining
            )

        # ====================================================
        # Browser -> Render
        # ====================================================

        client_socket.settimeout(
            None
        )

        while not stop_event.is_set():

            try:

                data = client_socket.recv(
                    BUFFER_SIZE
                )

            except (
                ConnectionResetError,
                BrokenPipeError,
                OSError
            ):

                break

            if not data:

                break

            # =================================================
            # Queue instead of making an HTTP request.
            #
            # This is one of the major performance improvements.
            # =================================================

            while not stop_event.is_set():

                try:

                    upload_queue.put(
                        data,
                        timeout=1
                    )

                    break

                except queue.Full:

                    continue

    except Exception as e:

        if not stop_event.is_set():

            print(
                f"[{connection_id}] "
                f"error:",
                e
            )

    finally:

        stop_event.set()

        # Tell upload generator to stop

        try:

            upload_queue.put_nowait(
                None
            )

        except Exception:
            pass

        # Close remote

        render_close(
            connection_id
        )

        # Close local

        try:

            client_socket.shutdown(
                socket.SHUT_RDWR
            )

        except Exception:
            pass

        try:

            client_socket.close()

        except Exception:
            pass


# ============================================================
# Start proxy
# ============================================================

def start_proxy():

    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server.bind(
        (
            LOCAL_HOST,
            LOCAL_PORT
        )
    )

    server.listen(
        200
    )

    print()
    print(
        "================================="
    )
    print(
        " HTTPS Tunnel Proxy v3"
    )
    print(
        "================================="
    )
    print(
        f"Listening on "
        f"{LOCAL_HOST}:{LOCAL_PORT}"
    )
    print(
        f"Render: "
        f"{RENDER_URL}"
    )
    print(
        f"SSL verification: "
        f"{VERIFY_SSL}"
    )
    print(
        "Streaming mode: ENABLED"
    )
    print(
        "Buffer: "
        f"{BUFFER_SIZE // 1024} KB"
    )
    print()

    try:

        while True:

            client_socket, address = (
                server.accept()
            )

            print(
                "Local client:",
                address
            )

            thread = threading.Thread(
                target=handle_client,
                args=(
                    client_socket,
                ),
                daemon=True
            )

            thread.start()

    except KeyboardInterrupt:

        print(
            "\nStopping proxy..."
        )

    finally:

        try:
            server.close()
        except Exception:
            pass


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    start_proxy()