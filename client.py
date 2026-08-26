import socket
import threading
import uuid
import base64
import requests
import urllib3
import time


# =========================================
# Configuration
# =========================================

RENDER_URL = "https://server-2-vddr.onrender.com"

TOKEN = "131232"

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 3128

BUFFER_SIZE = 16384

# NetFree
VERIFY_SSL = False

# =========================================
# Timeouts
# =========================================

CONNECT_TIMEOUT = 15

# לא לחכות 30 שניות ל-/recv.
RECV_TIMEOUT_SECONDS = 5

# Timeout לבקשות רגילות
HTTP_READ_TIMEOUT = 30

# =========================================
# Retry
# =========================================

MAX_RECV_ERRORS = 30


if not VERIFY_SSL:

    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# =========================================
# HTTP request to Render
# =========================================

def render_request(
    method,
    path,
    *,
    json_data=None,
    params=None,
    timeout=None,
    retries=0
):

    url = RENDER_URL + path

    if timeout is None:

        timeout = (
            CONNECT_TIMEOUT,
            HTTP_READ_TIMEOUT
        )

    last_error = None

    for attempt in range(retries + 1):

        try:

            response = requests.request(
                method,
                url,

                json=json_data,

                params=params,

                timeout=timeout,

                verify=VERIFY_SSL,

                headers={
                    "User-Agent":
                        "HTTPS-Tunnel-Client/2.0",

                    "Accept":
                        "application/json",

                    # לא לסגור בכוח.
                    # מאפשר ל-requests לנהל HTTP.
                    "Connection":
                        "keep-alive",
                }
            )

            response.raise_for_status()

            return response

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:

            last_error = e

            if attempt < retries:

                delay = min(
                    0.5 * (attempt + 1),
                    3
                )

                time.sleep(delay)

                continue

            raise last_error


# =========================================
# POST helper
# =========================================

def render_post(
    path,
    json_data,
    *,
    retries=0
):

    response = render_request(
        "POST",
        path,
        json_data=json_data,
        timeout=(
            CONNECT_TIMEOUT,
            HTTP_READ_TIMEOUT
        ),
        retries=retries
    )

    return response.json()


# =========================================
# GET helper
# =========================================

def render_get(
    path,
    params=None
):

    response = render_request(
        "GET",
        path,
        params=params,
        timeout=(
            CONNECT_TIMEOUT,
            RECV_TIMEOUT_SECONDS + 10
        ),
        retries=0
    )

    return response.json()


# =========================================
# Send data
# =========================================

def send_data(
    connection_id,
    data
):

    if not data:
        return False

    encoded = base64.b64encode(
        data
    ).decode("ascii")

    try:

        render_post(
            "/data",
            {
                "token": TOKEN,

                "connection_id":
                    connection_id,

                "data":
                    encoded,
            },

            # IMPORTANT:
            # אין retry אוטומטי ל-data.
            # אחרת עלול להיות duplicate TCP data.
            retries=0
        )

        return True

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"Send data error: {e}"
        )

        return False


# =========================================
# Close remote connection
# =========================================

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
            },

            retries=1
        )

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"Close remote error: {e}"
        )


# =========================================
# Receive data from Render
# =========================================

def receive_loop(
    client_socket,
    connection_id,
    stop_event
):

    consecutive_errors = 0

    try:

        while not stop_event.is_set():

            try:

                result = render_get(
                    f"/recv/{connection_id}",

                    {
                        "token": TOKEN
                    }
                )

                consecutive_errors = 0

            # =================================
            # HTTP error
            # =================================

            except requests.HTTPError as e:

                status = (
                    e.response.status_code
                    if e.response is not None
                    else None
                )

                # Connection really gone

                if status in (
                    404,
                    410
                ):

                    print(
                        f"[{connection_id}] "
                        f"Remote connection gone "
                        f"({status})"
                    )

                    break

                # Temporary Render error

                if status in (
                    502,
                    503,
                    504
                ):

                    consecutive_errors += 1

                    print(
                        f"[{connection_id}] "
                        f"Render HTTP {status} "
                        f"retry={consecutive_errors}"
                    )

                    if consecutive_errors >= MAX_RECV_ERRORS:

                        print(
                            f"[{connection_id}] "
                            f"Too many Render errors"
                        )

                        break

                    time.sleep(
                        min(
                            0.5 *
                            consecutive_errors,
                            5
                        )
                    )

                    continue

                print(
                    f"[{connection_id}] "
                    f"HTTP error: {e}"
                )

                break

            # =================================
            # Network error
            # =================================

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as e:

                consecutive_errors += 1

                print(
                    f"[{connection_id}] "
                    f"Render receive error: "
                    f"{e} "
                    f"retry={consecutive_errors}"
                )

                # לא סוגרים מיד.
                # נותנים ל-Render להתאושש.

                if consecutive_errors >= MAX_RECV_ERRORS:

                    print(
                        f"[{connection_id}] "
                        f"Too many receive errors"
                    )

                    break

                time.sleep(
                    min(
                        0.5 *
                        consecutive_errors,
                        5
                    )
                )

                continue

            except Exception as e:

                consecutive_errors += 1

                print(
                    f"[{connection_id}] "
                    f"Receive error: {e}"
                )

                if consecutive_errors >= 10:
                    break

                time.sleep(1)

                continue

            # =================================
            # Validate result
            # =================================

            if not isinstance(
                result,
                dict
            ):

                continue

            msg_type = result.get(
                "type"
            )

            # =================================
            # Timeout
            # =================================

            if msg_type == "timeout":

                continue

            # =================================
            # DATA
            # =================================

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

            # =================================
            # CLOSE
            # =================================

            elif msg_type == "close":

                print(
                    f"[{connection_id}] "
                    f"Remote closed connection"
                )

                break

            # =================================
            # ERROR
            # =================================

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


# =========================================
# Parse browser request
# =========================================

def parse_target(
    client_socket
):

    client_socket.settimeout(15)

    data = b""

    while b"\r\n\r\n" not in data:

        chunk = client_socket.recv(
            4096
        )

        if not chunk:

            return (
                None,
                None,
                False,
                b"",
                None,
                None
            )

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

        return (
            None,
            None,
            False,
            b"",
            None,
            None
        )

    request_line = lines[0]

    parts = request_line.split()

    if len(parts) < 2:

        return (
            None,
            None,
            False,
            b"",
            None,
            None
        )

    method = parts[0].upper()

    target = parts[1]

    # =====================================
    # HTTPS CONNECT
    # =====================================

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
            method,
            None
        )

    # =====================================
    # Normal HTTP
    # =====================================

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
            method,
            None
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
        method,
        target
    )


# =========================================
# Handle local client
# =========================================

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
            method,
            target
        ) = parse_target(
            client_socket
        )

        if not host:

            client_socket.close()

            return

        print(
            f"[{connection_id}] "
            f"{method} "
            f"{host}:{port}"
        )

        # =================================
        # Open Render connection
        # =================================

        render_post(
            "/open",

            {
                "token": TOKEN,

                "connection_id":
                    connection_id,

                "host": host,

                "port": port,
            },

            # Open is safe to retry
            # because server rejects duplicates.

            retries=3
        )

        # =================================
        # HTTPS
        # =================================

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: Python-HTTPS-Tunnel\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n"
            )

        # =================================
        # HTTP
        # =================================

        else:

            # במקרה HTTP רגיל, צריך לשמור
            # את ה-path המקורי.

            if not target:

                target = "/"

            request = (
                f"{method} "
                f"{target} "
                f"HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            ).encode(
                "iso-8859-1"
            )

            if not send_data(
                connection_id,
                request
            ):

                return

        # =================================
        # Receiver thread
        # =================================

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

        # =================================
        # Send buffered data
        # =================================

        if remaining:

            if not send_data(
                connection_id,
                remaining
            ):

                return

        # =================================
        # Browser -> Render
        # =================================

        client_socket.settimeout(None)

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

            if not send_data(
                connection_id,
                data
            ):

                break

    except Exception as e:

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


# =========================================
# Start proxy
# =========================================

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

    server.listen(200)

    print()
    print(
        "================================="
    )
    print(
        " HTTPS Tunnel Proxy v2"
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


# =========================================
# Main
# =========================================

if __name__ == "__main__":

    start_proxy()