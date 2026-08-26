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

# בגלל NetFree
VERIFY_SSL = False

# כמה פעמים לנסות בקשות Render
REQUEST_RETRIES = 3

# זמן timeout לחיבור HTTP
CONNECT_TIMEOUT = 15

# זמן timeout לקבלת תשובה
READ_TIMEOUT = 30

# כאשר /recv מחזיר timeout, השרת מחכה פחות זמן
# כדי להימנע מניתוקים של reverse proxy.
RECV_TIMEOUT_SECONDS = 10


if not VERIFY_SSL:
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# =========================================
# HTTP helpers
# =========================================

def render_request(method, path, **kwargs):
    """
    כל בקשה ל-Render מקבלת Session חדש.
    זה מונע בעיות thread-safety של requests.Session.
    """

    url = RENDER_URL + path

    timeout = kwargs.pop(
        "timeout",
        (CONNECT_TIMEOUT, READ_TIMEOUT)
    )

    last_error = None

    for attempt in range(REQUEST_RETRIES):

        try:
            response = requests.request(
                method,
                url,
                timeout=timeout,
                verify=VERIFY_SSL,
                headers={
                    "Connection": "close",
                    "User-Agent": "HTTPS-Tunnel-Client/1.0",
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
                time.sleep(0.5 * (attempt + 1))
                continue

            raise last_error


def render_post(path, json_data):
    response = render_request(
        "POST",
        path,
        json=json_data,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )

    return response.json()


def render_get(path, params=None):
    response = render_request(
        "GET",
        path,
        params=params,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )

    return response.json()


# =========================================
# Send data to Render
# =========================================

def send_data(connection_id, data):

    if not data:
        return

    encoded = base64.b64encode(data).decode("ascii")

    render_post(
        "/data",
        {
            "token": TOKEN,
            "connection_id": connection_id,
            "data": encoded,
        }
    )


# =========================================
# Close remote connection
# =========================================

def close_remote(connection_id):

    try:

        render_post(
            "/close",
            {
                "token": TOKEN,
                "connection_id": connection_id,
            }
        )

    except requests.HTTPError as e:

        if (
            e.response is not None
            and e.response.status_code in (404, 410)
        ):
            return

        print(
            f"[{connection_id}] close HTTP error:",
            e
        )

    except Exception as e:

        print(
            f"[{connection_id}] close error:",
            e
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

                # הצלחה
                consecutive_errors = 0

            except requests.HTTPError as e:

                status = (
                    e.response.status_code
                    if e.response is not None
                    else None
                )

                # החיבור באמת לא קיים
                if status in (404, 410):

                    print(
                        f"[{connection_id}] "
                        f"Render connection no longer exists "
                        f"({status})"
                    )

                    break

                # שגיאות זמניות של Render
                if status in (502, 503, 504):

                    consecutive_errors += 1

                    print(
                        f"[{connection_id}] "
                        f"temporary Render error {status} "
                        f"(retry {consecutive_errors})"
                    )

                    if consecutive_errors >= 10:
                        break

                    time.sleep(
                        min(
                            0.5 * consecutive_errors,
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

                consecutive_errors += 1

                print(
                    f"[{connection_id}] "
                    f"Render receive connection error: "
                    f"{e} "
                    f"(retry {consecutive_errors})"
                )

                # חשוב:
                # לא סוגרים מיד את ה-TCP המקומי.
                # Render יכול להתנתק זמנית.
                if consecutive_errors >= 15:

                    print(
                        f"[{connection_id}] "
                        f"Too many Render errors"
                    )

                    break

                time.sleep(
                    min(
                        0.5 * consecutive_errors,
                        5
                    )
                )

                continue

            except Exception as e:

                consecutive_errors += 1

                print(
                    f"[{connection_id}] "
                    f"receive error: {e}"
                )

                if consecutive_errors >= 10:
                    break

                time.sleep(1)

                continue

            # =====================================
            # Process Render response
            # =====================================

            if not isinstance(result, dict):
                continue

            msg_type = result.get("type")

            # =====================================
            # No data yet
            # =====================================

            if msg_type == "timeout":
                continue

            # =====================================
            # Data from Runner
            # =====================================

            if msg_type == "data":

                encoded = result.get(
                    "data",
                    ""
                )

                if encoded:

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

            # =====================================
            # Remote connection closed
            # =====================================

            elif msg_type == "close":

                print(
                    f"[{connection_id}] "
                    f"Remote connection closed"
                )

                break

            # =====================================
            # Remote error
            # =====================================

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

def parse_target(client_socket):

    client_socket.settimeout(15)

    data = b""

    while b"\r\n\r\n" not in data:

        chunk = client_socket.recv(4096)

        if not chunk:
            return (
                None,
                None,
                None,
                b"",
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
            None,
            b"",
            None
        )

    request_line = lines[0]

    parts = request_line.split()

    if len(parts) < 2:
        return (
            None,
            None,
            None,
            b"",
            None
        )

    method = parts[0].upper()
    target = parts[1]

    # =========================================
    # HTTPS CONNECT
    # =========================================

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

    # =========================================
    # Normal HTTP
    # =========================================

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

        port = int(port_string)

    return (
        host,
        port,
        False,
        remaining,
        method
    )


# =========================================
# Handle local browser connection
# =========================================

def handle_client(client_socket):

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

            client_socket.close()
            return

        print(
            f"[{connection_id}] "
            f"{host}:{port}"
        )

        # =====================================
        # Open connection on Render
        # =====================================

        render_post(
            "/open",
            {
                "token": TOKEN,
                "connection_id": connection_id,
                "host": host,
                "port": port,
            }
        )

        # =====================================
        # HTTPS CONNECT
        # =====================================

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: Python-HTTPS-Tunnel\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n"
            )

        # =====================================
        # Normal HTTP
        # =====================================

        else:

            # עבור HTTP רגיל:
            # remaining מכיל מידע שכבר הגיע אחרי headers.
            #
            # אנחנו שולחים בקשה מינימלית תקינה.
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

        # =====================================
        # Start receiver
        # =====================================

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

        # =====================================
        # Send buffered data
        # =====================================

        if remaining:

            send_data(
                connection_id,
                remaining
            )

        # =====================================
        # Browser -> Render
        # =====================================

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

            send_data(
                connection_id,
                data
            )

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


# =========================================
# Start local proxy
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

    server.listen(100)

    print()
    print(
        "================================="
    )
    print(
        " HTTPS Tunnel Proxy"
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


# =========================================
# Main
# =========================================

if __name__ == "__main__":

    start_proxy()