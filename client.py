import socket
import threading
import uuid
import base64
import requests
import urllib3
import time


# ============================================================
# Configuration
# ============================================================

RENDER_URL = "https://server-2-vddr.onrender.com"

TOKEN = "131232"

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 3128

BUFFER_SIZE = 32768

# NetFree
VERIFY_SSL = False

# כמה זמן ממתינים ל-/recv
RECV_TIMEOUT = 35

# Timeout לחיבור TCP ל-Render
CONNECT_TIMEOUT = 15

# timeout לקריאת HTTP רגילה
READ_TIMEOUT = 45


if not VERIFY_SSL:
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# ============================================================
# Thread local HTTP sessions
# ============================================================

thread_local = threading.local()


def get_session():
    """
    לכל thread יש requests.Session משלו.

    היתרון:
    - Keep-Alive
    - פחות TLS handshakes
    - פחות overhead
    - אין שיתוף Session בין threads
    """

    session = getattr(
        thread_local,
        "session",
        None
    )

    if session is None:

        session = requests.Session()

        session.headers.update({
            "User-Agent":
                "HTTPS-Tunnel-Proxy/3.0",
            "Accept":
                "application/json",
            "Connection":
                "keep-alive",
        })

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=50,
            max_retries=0,
        )

        session.mount(
            "http://",
            adapter
        )

        session.mount(
            "https://",
            adapter
        )

        thread_local.session = session

    return session


# ============================================================
# Render request
# ============================================================

def render_request(
    method,
    path,
    *,
    json_data=None,
    params=None,
    timeout=None
):

    if timeout is None:
        timeout = (
            CONNECT_TIMEOUT,
            READ_TIMEOUT
        )

    session = get_session()

    url = RENDER_URL + path

    response = session.request(
        method,
        url,
        json=json_data,
        params=params,
        timeout=timeout,
        verify=VERIFY_SSL,
    )

    response.raise_for_status()

    return response


# ============================================================
# Open
# ============================================================

def render_open(
    connection_id,
    host,
    port
):

    response = render_request(
        "POST",
        "/open",
        json_data={
            "token": TOKEN,
            "connection_id": connection_id,
            "host": host,
            "port": port,
        },
        timeout=(
            CONNECT_TIMEOUT,
            READ_TIMEOUT
        )
    )

    return response.json()


# ============================================================
# Send data
# ============================================================

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

        response = render_request(
            "POST",
            "/data",
            json_data={
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

        response.close()

        return True

    except Exception as e:

        print(
            f"[{connection_id}] "
            f"send error: {e}"
        )

        return False


# ============================================================
# Receive data
# ============================================================

def receive_loop(
    client_socket,
    connection_id,
    stop_event
):

    session = get_session()

    try:

        while not stop_event.is_set():

            try:

                response = session.get(
                    RENDER_URL
                    + f"/recv/{connection_id}",
                    params={
                        "token": TOKEN
                    },
                    timeout=(
                        CONNECT_TIMEOUT,
                        RECV_TIMEOUT
                    ),
                    verify=VERIFY_SSL,
                )

                response.raise_for_status()

                result = response.json()

                response.close()

            except requests.exceptions.Timeout:

                # Long-poll timeout.
                # זה לא אומר שהחיבור נסגר.
                continue

            except requests.exceptions.ConnectionError as e:

                if stop_event.is_set():
                    break

                print(
                    f"[{connection_id}] "
                    f"Render receive connection error: "
                    f"{e}"
                )

                time.sleep(1)

                continue

            except requests.exceptions.HTTPError as e:

                status = (
                    e.response.status_code
                    if e.response is not None
                    else None
                )

                if status == 404:
                    print(
                        f"[{connection_id}] "
                        f"connection not found"
                    )
                    break

                if status == 410:
                    print(
                        f"[{connection_id}] "
                        f"connection closed"
                    )
                    break

                if status in (
                    502,
                    503,
                    504
                ):

                    print(
                        f"[{connection_id}] "
                        f"temporary Render error "
                        f"{status}"
                    )

                    time.sleep(1)

                    continue

                print(
                    f"[{connection_id}] "
                    f"HTTP error: {e}"
                )

                break

            except Exception as e:

                if stop_event.is_set():
                    break

                print(
                    f"[{connection_id}] "
                    f"receive error: {e}"
                )

                time.sleep(1)

                continue


            # =================================================
            # Process response
            # =================================================

            if not isinstance(
                result,
                dict
            ):
                continue

            msg_type = result.get(
                "type"
            )


            # -------------------------------------------------
            # Nothing available
            # -------------------------------------------------

            if msg_type == "timeout":
                continue


            # -------------------------------------------------
            # Remote data
            # -------------------------------------------------

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

                except Exception as e:

                    print(
                        f"[{connection_id}] "
                        f"Base64 error: {e}"
                    )

                    break

                if not data:
                    continue

                try:

                    client_socket.sendall(
                        data
                    )

                except (
                    BrokenPipeError,
                    ConnectionResetError,
                    OSError
                ) as e:

                    print(
                        f"[{connection_id}] "
                        f"local socket closed: {e}"
                    )

                    break


            # -------------------------------------------------
            # Remote closed
            # -------------------------------------------------

            elif msg_type == "close":

                print(
                    f"[{connection_id}] "
                    f"Remote closed connection"
                )

                break


            # -------------------------------------------------
            # Remote error
            # -------------------------------------------------

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


# ============================================================
# Close remote
# ============================================================

def close_remote(
    connection_id
):

    try:

        render_request(
            "POST",
            "/close",
            json_data={
                "token": TOKEN,
                "connection_id":
                    connection_id,
            },
            timeout=(
                5,
                10
            )
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
                False,
                b"",
                None
            )

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

    request_line = lines[0]

    parts = request_line.split()

    if len(parts) < 2:

        return (
            None,
            None,
            False,
            remaining,
            None
        )

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


# ============================================================
# Handle local connection
# ============================================================

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
        ) = parse_request(
            client_socket
        )

        if not host:

            client_socket.close()

            return


        print(
            f"[{connection_id}] "
            f"{'CONNECT ' if is_connect else ''}"
            f"{host}:{port}"
        )


        # ====================================================
        # Open remote TCP connection
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
                b"Proxy-Agent: HTTPS-Tunnel-Proxy\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n"
            )

        else:

            # HTTP proxy request.
            # שולחים את ה-request המקורי,
            # ולא בונים request חדש שעלול לשנות path.

            send_data(
                connection_id,
                (
                    method
                    + " "
                    + target
                    + " HTTP/1.1\r\n"
                ).encode(
                    "iso-8859-1"
                )
                if False
                else remaining
            )


        # ====================================================
        # Receiver
        # ====================================================

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


        # ====================================================
        # Data that arrived together with headers
        # ====================================================

        if remaining and is_connect:

            if not send_data(
                connection_id,
                remaining
            ):

                stop_event.set()

                return


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

            if not send_data(
                connection_id,
                data
            ):

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


# ============================================================
# Start local proxy
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


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    start_proxy()