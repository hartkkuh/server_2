import socket
import threading
import uuid
import base64
import requests
import urllib3

# =========================================
# Configuration
# =========================================

RENDER_URL = "https://server-2-vddr.onrender.com"
TOKEN = "131232"

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 3128

BUFFER_SIZE = 16384

# כרגע False בגלל תעודת NetFree.
# לאחר הגדרת CA מתאים אפשר להפוך ל-True.
VERIFY_SSL = False

if not VERIFY_SSL:
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )


# =========================================
# HTTP session
# =========================================

session = requests.Session()

# חשוב מאוד:
# requests לא ישתמש ב-HTTP_PROXY / HTTPS_PROXY
# של Windows.
#
# אחרת הוא עלול לשלוח את החיבור ל-Render
# בחזרה אל 127.0.0.1:3128 וליצור לולאה.
session.trust_env = False


# =========================================
# Render API
# =========================================

def render_post(path, json_data):
    url = RENDER_URL + path

    response = session.post(
        url,
        json=json_data,
        timeout=60,
        verify=VERIFY_SSL
    )

    response.raise_for_status()

    return response.json()


def render_get(path, params=None):
    url = RENDER_URL + path

    response = session.get(
        url,
        params=params,
        timeout=35,
        verify=VERIFY_SSL
    )

    response.raise_for_status()

    return response.json()


# =========================================
# Send data to Render
# =========================================

def send_data(connection_id, data):

    if not data:
        return

    encoded = base64.b64encode(
        data
    ).decode("ascii")

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

        # אם החיבור כבר נמחק בשרת - אין צורך
        # להפיל את ה-client.
        if e.response is not None and e.response.status_code == 404:
            return

        print(
            f"[{connection_id}] close error:",
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

def receive_loop(client_socket, connection_id, stop_event):

    try:

        while not stop_event.is_set():

            try:

                result = render_get(
                    f"/recv/{connection_id}",
                    {
                        "token": TOKEN
                    }
                )

            except requests.HTTPError as e:

                if (
                    e.response is not None
                    and e.response.status_code == 404
                ):
                    # Render כבר מחק את החיבור.
                    break

                raise

            msg_type = result.get("type")

            # אין כרגע מידע.
            if msg_type == "timeout":
                continue

            # נתונים מה-Runner.
            if msg_type == "data":

                encoded = result.get(
                    "data",
                    ""
                )

                if encoded:

                    data = base64.b64decode(
                        encoded
                    )

                    if data:

                        client_socket.sendall(
                            data
                        )

            # החיבור המרוחק נסגר.
            elif msg_type == "close":

                break

            # שגיאה מה-Runner.
            elif msg_type == "error":

                print(
                    f"[{connection_id}] "
                    f"Remote error:",
                    result.get("message")
                )

                break

    except Exception as e:

        if not stop_event.is_set():

            print(
                f"[{connection_id}] "
                f"receive error:",
                e
            )

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

        chunk = client_socket.recv(
            4096
        )

        if not chunk:
            return None, None, None, b"", b""

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
        return None, None, None, b"", b""

    request_line = lines[0]

    parts = request_line.split()

    if len(parts) < 2:
        return None, None, None, b"", b""

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
            method
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

    receiver = None

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

        # =================================
        # Open connection on Render
        # =================================

        render_post(
            "/open",
            {
                "token": TOKEN,
                "connection_id": connection_id,
                "host": host,
                "port": port,
            }
        )

        # =================================
        # HTTPS CONNECT
        # =================================

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: Python-HTTPS-Tunnel\r\n"
                b"\r\n"
            )

        # =================================
        # Normal HTTP request
        # =================================

        else:

            # כאן אנחנו מעבירים את הבקשה המקורית
            # כפי שהגיעה מהלקוח.
            #
            # header כבר נקרא ב-parse_target(),
            # לכן עבור HTTP רגיל צריך לשחזר
            # את הבקשה.
            #
            # עבור HTTPS רוב התעבורה עוברת דרך CONNECT,
            # ולכן זה הנתיב החשוב.

            request = (
                f"{method} http://{host}/ HTTP/1.1\r\n"
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

        # =================================
        # Start receiver
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
        # Send bytes already buffered
        # =================================

        if remaining:

            send_data(
                connection_id,
                remaining
            )

        # =================================
        # Browser -> Render
        # =================================

        client_socket.settimeout(None)

        while not stop_event.is_set():

            data = client_socket.recv(
                BUFFER_SIZE
            )

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
                f"error:",
                e
            )

    finally:

        stop_event.set()

        # =================================
        # Tell Render to close
        # =================================

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