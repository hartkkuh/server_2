import socket
import threading
import uuid
import base64
import requests
import urllib3
import time


# =========================================================
# Configuration
# =========================================================

RENDER_URL = "https://server-2-vddr.onrender.com"
TOKEN = "131232"


LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 3128

BUFFER_SIZE = 65536

VERIFY_SSL = False

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 15

REQUEST_RETRIES = 5

# קצר בכוונה.
# אנחנו לא רוצים שכל /recv יחכה 10 שניות.
RECV_TIMEOUT_SECONDS = 25

# זמן בין retries
RETRY_BASE_DELAY = 0.4


if not VERIFY_SSL:
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# =========================================================
# Thread-local HTTP Session
# =========================================================

thread_local = threading.local()


def get_session():

    session = getattr(
        thread_local,
        "session",
        None
    )

    if session is None:

        session = requests.Session()
        session.trust_env = False
        session.verify = VERIFY_SSL

        session.headers.update({
            "User-Agent":
                "HTTPS-Tunnel-Client/3.0"
        })

        thread_local.session = session

    return session


# =========================================================
# Render request
# =========================================================

def render_request(
    method,
    path,
    timeout=None,
    **kwargs
):

    url = RENDER_URL + path

    if timeout is None:
        timeout = (
            CONNECT_TIMEOUT,
            READ_TIMEOUT
        )

    last_error = None

    session = get_session()

    for attempt in range(
        REQUEST_RETRIES
    ):

        try:

            response = session.request(
                method,
                url,
                timeout=timeout,
                verify=VERIFY_SSL,
                **kwargs
            )

            response.raise_for_status()

            return response

        except requests.exceptions.HTTPError:

            # HTTP errors לא תמיד זמניים.
            raise

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:

            last_error = e

            if attempt + 1 >= REQUEST_RETRIES:
                raise last_error

            delay = min(
                RETRY_BASE_DELAY *
                (2 ** attempt),
                3
            )

            time.sleep(delay)

    raise last_error


# =========================================================
# POST JSON
# =========================================================

def render_post(
    path,
    json_data,
    timeout=None
):

    response = render_request(
        "POST",
        path,
        json=json_data,
        timeout=timeout
    )

    return response.json()


# =========================================================
# GET JSON
# =========================================================

def render_get(
    path,
    params=None,
    timeout=None
):

    response = render_request(
        "GET",
        path,
        params=params,
        timeout=timeout
    )

    return response.json()


# =========================================================
# Send data
# =========================================================

def send_data(
    connection_id,
    data
):

    if not data:
        return True

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
                "data": encoded,
            },
            timeout=(
                CONNECT_TIMEOUT,
                READ_TIMEOUT
            )
        )

        return True

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"send_data error: {e}"
        )

        return False


# =========================================================
# Close remote
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
            },
            timeout=(
                5,
                8
            )
        )

    except requests.HTTPError as e:

        status = (
            e.response.status_code
            if e.response is not None
            else None
        )

        if status not in (
            404,
            410
        ):

            print(
                f"[{connection_id}] "
                f"close HTTP error: {e}"
            )

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"close error: {e}"
        )


# =========================================================
# Receive loop
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
                    },
                    timeout=(
                        CONNECT_TIMEOUT,
                        RECV_TIMEOUT_SECONDS
                    )
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
                        f"Render temporary error "
                        f"{status}"
                    )

                    time.sleep(
                        min(
                            0.3 * errors,
                            3
                        )
                    )

                    continue

                print(
                    f"[{connection_id}] "
                    f"HTTP error: {e}"
                )

                break

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as e:

                errors += 1

                print(
                    f"[{connection_id}] "
                    f"Render receive error: {e}"
                )

                # חשוב:
                # לא סוגרים TCP מקומי מיד.
                if errors >= 30:

                    print(
                        f"[{connection_id}] "
                        f"Too many Render errors"
                    )

                    break

                time.sleep(
                    min(
                        0.2 * errors,
                        3
                    )
                )

                continue

            except Exception as e:

                errors += 1

                print(
                    f"[{connection_id}] "
                    f"receive error: {e}"
                )

                if errors >= 15:
                    break

                time.sleep(0.5)

                continue

            if not isinstance(
                result,
                dict
            ):
                continue

            msg_type = result.get(
                "type"
            )

            # -----------------------------------------
            # Poll timeout
            # -----------------------------------------

            if msg_type == "timeout":
                continue

            # -----------------------------------------
            # DATA
            # -----------------------------------------

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

            # -----------------------------------------
            # CLOSE
            # -----------------------------------------

            elif msg_type == "close":

                print(
                    f"[{connection_id}] "
                    f"Remote closed connection"
                )

                break

            # -----------------------------------------
            # ERROR
            # -----------------------------------------

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
# Parse proxy request
# =========================================================

def parse_target(
    client_socket
):

    client_socket.settimeout(
        15
    )

    data = b""

    while b"\r\n\r\n" not in data:

        chunk = client_socket.recv(
            8192
        )

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

    parts = lines[0].split()

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

            host = line[
                5:
            ].strip()

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
# Handle client
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

            client_socket.close()
            return

        print(
            f"[{connection_id}] "
            f"{method} "
            f"{host}:{port}"
        )

        # =================================================
        # Open remote connection
        # =================================================

        render_post(
            "/open",
            {
                "token": TOKEN,
                "connection_id":
                    connection_id,
                "host": host,
                "port": port,
            },
            timeout=(
                CONNECT_TIMEOUT,
                READ_TIMEOUT
            )
        )

        # =================================================
        # HTTPS
        # =================================================

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: Python-HTTPS-Tunnel/3.0\r\n"
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

            if not send_data(
                connection_id,
                request
            ):

                return

        # =================================================
        # Start receiver BEFORE sending buffered data
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
        # Send bytes already received with headers
        # =================================================

        if remaining:

            if not send_data(
                connection_id,
                remaining
            ):

                stop_event.set()
                return

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

            if not send_data(
                connection_id,
                data
            ):

                # אם Render לא זמין לרגע,
                # נותנים ל-receive loop להמשיך,
                # אבל לא נשלח אינסוף מידע שנאגר.
                if stop_event.is_set():
                    break

                time.sleep(0.2)

                continue

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
# Start local proxy
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

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_KEEPALIVE,
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

            client_socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1
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