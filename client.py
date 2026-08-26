import socket
import threading
import uuid
import base64
import time

import requests
import urllib3


# =========================================================
# Configuration
# =========================================================

RENDER_URL = "https://server-2-vddr.onrender.com"

TOKEN = "131232"

LOCAL_HOST = "127.0.0.1"

LOCAL_PORT = 3128

BUFFER_SIZE = 16384

VERIFY_SSL = False

REQUEST_RETRIES = 5

CONNECT_TIMEOUT = 20

READ_TIMEOUT = 40


if not VERIFY_SSL:

    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# =========================================================
# Render HTTP request
# =========================================================

def render_request(
    method,
    path,
    **kwargs
):

    url = RENDER_URL + path

    timeout = kwargs.pop(
        "timeout",
        (
            CONNECT_TIMEOUT,
            READ_TIMEOUT
        )
    )

    last_error = None

    for attempt in range(
        REQUEST_RETRIES
    ):

        try:

            response = requests.request(
                method,
                url,
                timeout=timeout,
                verify=VERIFY_SSL,
                headers={
                    "Connection": "close",
                    "User-Agent":
                        "HTTPS-Tunnel-Proxy/2.1",
                },
                **kwargs
            )

            response.raise_for_status()

            return response

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:

            last_error = e

            if attempt + 1 < REQUEST_RETRIES:

                time.sleep(
                    min(
                        0.5 * (attempt + 1),
                        3
                    )
                )

                continue

            raise last_error


# =========================================================
# POST
# =========================================================

def render_post(
    path,
    data
):

    response = render_request(
        "POST",
        path,
        json=data
    )

    return response.json()


# =========================================================
# GET
# =========================================================

def render_get(
    path,
    params=None
):

    response = render_request(
        "GET",
        path,
        params=params
    )

    return response.json()


# =========================================================
# SEND DATA
# =========================================================

def send_data(
    connection_id,
    data
):

    if not data:
        return

    encoded = base64.b64encode(
        data
    ).decode("ascii")

    render_post(
        "/data",
        {
            "token": TOKEN,
            "connection_id":
                connection_id,
            "data": encoded,
        }
    )


# =========================================================
# CLOSE
# =========================================================

def close_remote(
    connection_id
):

    try:

        render_post(
            "/close",
            {
                "token": TOKEN,
                "connection_id":
                    connection_id,
            }
        )

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"close error: {e}"
        )


# =========================================================
# RECEIVE FROM RENDER
# =========================================================

def receive_loop(
    client_socket,
    connection_id,
    stop_event
):

    errors = 0

    try:

        while not stop_event.is_set():

            try:

                result = render_get(
                    f"/recv/{connection_id}",
                    {
                        "token": TOKEN
                    }
                )

                errors = 0

            except requests.HTTPError as e:

                status = (
                    e.response.status_code
                    if e.response is not None
                    else None
                )

                if status in (
                    404,
                    410
                ):

                    print(
                        f"[{connection_id}] "
                        f"Remote connection disappeared "
                        f"({status})"
                    )

                    break

                if status in (
                    502,
                    503,
                    504
                ):

                    errors += 1

                    print(
                        f"[{connection_id}] "
                        f"Render HTTP {status} "
                        f"(retry {errors})"
                    )

                    if errors >= 20:
                        break

                    time.sleep(
                        min(
                            errors * 0.5,
                            5
                        )
                    )

                    continue

                raise

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as e:

                errors += 1

                print(
                    f"[{connection_id}] "
                    f"Render receive error: "
                    f"{e} "
                    f"(retry {errors})"
                )

                if errors >= 20:

                    print(
                        f"[{connection_id}] "
                        f"Too many errors"
                    )

                    break

                time.sleep(
                    min(
                        errors * 0.5,
                        5
                    )
                )

                continue

            # =================================================
            # Process message
            # =================================================

            if not isinstance(
                result,
                dict
            ):
                continue

            msg_type = result.get(
                "type"
            )

            # =================================================
            # Timeout
            # =================================================

            if msg_type == "timeout":

                continue

            # =================================================
            # DATA
            # =================================================

            if msg_type == "data":

                encoded = result.get(
                    "data",
                    ""
                )

                if not encoded:
                    continue

                try:

                    data = base64.b64decode(
                        encoded
                    )

                    if data:

                        client_socket.sendall(
                            data
                        )

                except (
                    BrokenPipeError,
                    ConnectionResetError,
                    OSError,
                ) as e:

                    print(
                        f"[{connection_id}] "
                        f"Local socket closed: {e}"
                    )

                    break

            # =================================================
            # CLOSE
            # =================================================

            elif msg_type == "close":

                print(
                    f"[{connection_id}] "
                    f"Remote closed connection"
                )

                break

            # =================================================
            # ERROR
            # =================================================

            elif msg_type == "error":

                print(
                    f"[{connection_id}] "
                    f"Remote error: "
                    f"{result.get('message')}"
                )

                break

    finally:

        stop_event.set()

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


# =========================================================
# Parse CONNECT
# =========================================================

def parse_target(
    client_socket
):

    client_socket.settimeout(
        20
    )

    data = b""

    while b"\r\n\r\n" not in data:

        chunk = client_socket.recv(
            4096
        )

        if not chunk:
            return None, None, False, b"", None

        data += chunk

        if len(data) > 65536:

            raise Exception(
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
    ).split("\r\n")

    if not lines:
        return None, None, False, b"", None

    parts = lines[0].split()

    if len(parts) < 2:

        return None, None, False, b"", None

    method = parts[0].upper()

    target = parts[1]

    # =====================================================
    # HTTPS CONNECT
    # =====================================================

    if method == "CONNECT":

        if ":" in target:

            host, port = target.rsplit(
                ":",
                1
            )

            port = int(port)

        else:

            host = target

            port = 443

        return (
            host,
            port,
            True,
            remaining,
            method
        )

    # =====================================================
    # HTTP
    # =====================================================

    host = None

    port = 80

    for line in lines[1:]:

        if line.lower().startswith(
            "host:"
        ):

            host = line[5:].strip()

            break

    if not host:

        return (
            None,
            None,
            False,
            remaining,
            method
        )

    if ":" in host:

        host, port_string = host.rsplit(
            ":",
            1
        )

        port = int(
            port_string
        )

    return (
        host,
        port,
        False,
        remaining,
        method
    )


# =========================================================
# Client handler
# =========================================================

def handle_client(
    client_socket
):

    connection_id = str(
        uuid.uuid4()
    )

    stop_event = threading.Event()

    try:

        (
            host,
            port,
            is_connect,
            remaining,
            method
        ) = parse_target(
            client_socket
        )

        if not host:

            return

        print(
            f"[{connection_id}] "
            f"{'CONNECT' if is_connect else method} "
            f"{host}:{port}"
        )

        # =================================================
        # OPEN
        # =================================================

        render_post(
            "/open",
            {
                "token": TOKEN,
                "connection_id":
                    connection_id,
                "host": host,
                "port": port,
            }
        )

        # =================================================
        # CONNECT response
        # =================================================

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: Python-HTTPS-Tunnel\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n"
            )

        # =================================================
        # HTTP
        # =================================================

        else:

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

            send_data(
                connection_id,
                request
            )

        # =================================================
        # Start receiver
        # =================================================

        receiver = threading.Thread(
            target=receive_loop,
            args=(
                client_socket,
                connection_id,
                stop_event
            ),
            daemon=True
        )

        receiver.start()

        # =================================================
        # Send buffered data
        # =================================================

        if remaining:

            send_data(
                connection_id,
                remaining
            )

        # =================================================
        # Browser -> Render
        # =================================================

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
                OSError,
            ):

                break

            if not data:
                break

            try:

                send_data(
                    connection_id,
                    data
                )

            except Exception as e:

                print(
                    f"[{connection_id}] "
                    f"send error: {e}"
                )

                break

    except Exception as e:

        if not stop_event.is_set():

            print(
                f"[{connection_id}] "
                f"error: {e}"
            )

    finally:

        stop_event.set()

        close_remote(
            connection_id
        )

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


# =========================================================
# Start proxy
# =========================================================

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
        100
    )

    print()
    print(
        "================================="
    )
    print(
        " HTTPS Tunnel Proxy v2.1"
    )
    print(
        "================================="
    )
    print(
        f"Listening on "
        f"{LOCAL_HOST}:{LOCAL_PORT}"
    )
    print(
        f"Render: {RENDER_URL}"
    )
    print(
        f"SSL verification: "
        f"{VERIFY_SSL}"
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
                args=(client_socket,),
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


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    start_proxy()