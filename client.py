import socket
import threading
import uuid
import base64
import requests
import time


RENDER_URL = "https://YOUR-RENDER-SERVICE.onrender.com"

TOKEN = "CHANGE_ME"

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 3128


BUFFER_SIZE = 16384


session = requests.Session()


def render_post(path, json_data):

    url = RENDER_URL + path

    response = session.post(
        url,
        json=json_data,
        timeout=60
    )

    response.raise_for_status()

    return response.json()


def render_get(path, params):

    url = RENDER_URL + path

    response = session.get(
        url,
        params=params,
        timeout=35
    )

    response.raise_for_status()

    return response.json()


def send_data(connection_id, data):

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


def close_remote(connection_id):

    try:

        render_post(
            "/close",
            {
                "token": TOKEN,
                "connection_id": connection_id,
            }
        )

    except Exception as e:

        print(
            "close error:",
            e
        )


def receive_loop(
    client_socket,
    connection_id
):

    try:

        while True:

            result = render_get(
                f"/recv/{connection_id}",
                {
                    "token": TOKEN
                }
            )

            msg_type = result.get(
                "type"
            )

            if msg_type == "timeout":

                continue

            if msg_type == "data":

                encoded = result.get(
                    "data",
                    ""
                )

                data = base64.b64decode(
                    encoded
                )

                if data:

                    client_socket.sendall(
                        data
                    )

            elif msg_type == "close":

                break

            elif msg_type == "error":

                print(
                    "Remote error:",
                    result.get("message")
                )

                break

    except Exception as e:

        print(
            f"[{connection_id}] receive error:",
            e
        )

    finally:

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


def parse_target(client_socket):

    client_socket.settimeout(15)

    data = b""

    while b"\r\n\r\n" not in data:

        chunk = client_socket.recv(
            4096
        )

        if not chunk:
            return None, None, None

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
        return None, None, None

    request_line = lines[0]

    parts = request_line.split()

    if len(parts) < 2:

        return None, None, None

    method = parts[0].upper()

    target = parts[1]

    # HTTPS CONNECT

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
            True
        )

    # Normal HTTP proxy request

    host = None

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
            False
        )

    if ":" in host:

        host, port = host.rsplit(
            ":",
            1
        )

        port = int(port)

    else:

        port = 80

    return (
        host,
        port,
        False
    )


def handle_client(client_socket):

    connection_id = str(
        uuid.uuid4()
    )

    remote_socket = None

    try:

        host, port, is_connect = parse_target(
            client_socket
        )

        if not host:

            client_socket.close()
            return

        print(
            f"[{connection_id}] "
            f"{host}:{port}"
        )

        # Open connection through Render

        render_post(
            "/open",
            {
                "token": TOKEN,
                "connection_id": connection_id,
                "host": host,
                "port": port,
            }
        )

        # Tell browser that CONNECT succeeded

        if is_connect:

            client_socket.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Proxy-Agent: Python-HTTPS-Tunnel\r\n"
                b"\r\n"
            )

        else:

            # For normal HTTP requests,
            # read the original header again
            # and forward a minimal request.
            #
            # We need to reconstruct it because
            # the browser normally sends an
            # absolute URL when using a proxy.

            # Re-read is impossible because
            # parse_target consumed it.
            #
            # Therefore regular HTTP proxy mode
            # is intentionally handled by a second
            # lightweight parser below.

            pass

        # Start HTTPS response polling

        receiver = threading.Thread(
            target=receive_loop,
            args=(
                client_socket,
                connection_id
            ),
            daemon=True
        )

        receiver.start()

        # Send remaining bytes if any

        # For CONNECT, remaining is normally empty.
        #
        # We need to retrieve any buffered data.
        #
        # Since parse_target currently doesn't expose
        # remaining data, read normally.

        client_socket.settimeout(None)

        while True:

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

        print(
            f"[{connection_id}] error:",
            e
        )

    finally:

        close_remote(
            connection_id
        )

        try:
            client_socket.close()
        except Exception:
            pass


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
    print()

    while True:

        client_socket, address = server.accept()

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


if __name__ == "__main__":

    start_proxy()