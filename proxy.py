import base64
import os
import select
import socket
import urllib.request

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8080"))

PROXY_USER = os.environ.get("PROXY_USER", "testuser")
PROXY_PASSWORD = os.environ.get(
    "PROXY_PASSWORD",
    "change-this-password"
)

BUFFER_SIZE = 64 * 1024
CONNECT_TIMEOUT = 15
HTTP_TIMEOUT = 60
TUNNEL_IDLE_TIMEOUT = 300


class ProxyHandler(BaseHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    # =========================================================
    # Authentication
    # =========================================================

    def authenticate(self):

        header = self.headers.get("Proxy-Authorization")

        if not header:
            self.request_authentication()
            return False

        if not header.startswith("Basic "):
            self.request_authentication()
            return False

        try:
            encoded = header[6:].strip()

            decoded = base64.b64decode(
                encoded
            ).decode("utf-8")

            username, password = decoded.split(":", 1)

        except Exception:
            self.request_authentication()
            return False

        if (
            username != PROXY_USER
            or password != PROXY_PASSWORD
        ):
            self.request_authentication()
            return False

        return True

    def request_authentication(self):

        self.send_response(407)

        self.send_header(
            "Proxy-Authenticate",
            'Basic realm="Proxy"'
        )

        self.send_header(
            "Content-Length",
            "0"
        )

        self.end_headers()

    # =========================================================
    # Internal endpoints
    # =========================================================

    def do_GET(self):

        # -----------------------------------------------------
        # Container health check
        # -----------------------------------------------------

        if self.path == "/__health":

            body = b"OK\n"

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "text/plain"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(body)

            return

        # -----------------------------------------------------
        # Container public IP
        # -----------------------------------------------------

        if self.path == "/__container_ip":

            if not self.authenticate():
                return

            try:

                ip = urllib.request.urlopen(
                    "https://api.ipify.org",
                    timeout=10
                ).read().decode().strip()

                body = (
                    f"Container egress IP: {ip}\n"
                ).encode()

                self.send_response(200)

                self.send_header(
                    "Content-Type",
                    "text/plain"
                )

                self.send_header(
                    "Content-Length",
                    str(len(body))
                )

                self.end_headers()

                self.wfile.write(body)

                print(
                    f"[CONTAINER-IP] {ip}",
                    flush=True
                )

            except Exception as e:

                body = (
                    f"ERROR: {type(e).__name__}: {e}\n"
                ).encode()

                self.send_response(500)

                self.send_header(
                    "Content-Type",
                    "text/plain"
                )

                self.send_header(
                    "Content-Length",
                    str(len(body))
                )

                self.end_headers()

                self.wfile.write(body)

                print(
                    f"[CONTAINER-IP ERROR] {e}",
                    flush=True
                )

            return

        # -----------------------------------------------------
        # Normal HTTP Proxy GET
        # -----------------------------------------------------

        if not self.authenticate():
            return

        self.forward_http()

    # =========================================================
    # Other HTTP methods
    # =========================================================

    def do_HEAD(self):

        if not self.authenticate():
            return

        self.forward_http(head_only=True)

    def do_POST(self):

        if not self.authenticate():
            return

        self.forward_http()

    def do_PUT(self):

        if not self.authenticate():
            return

        self.forward_http()

    def do_DELETE(self):

        if not self.authenticate():
            return

        self.forward_http()

    def do_OPTIONS(self):

        if not self.authenticate():
            return

        self.forward_http()

    def do_PATCH(self):

        if not self.authenticate():
            return

        self.forward_http()

    # =========================================================
    # HTTP Proxy
    # =========================================================

    def forward_http(self, head_only=False):

        upstream = None

        try:

            parsed = urlsplit(self.path)

            if parsed.scheme != "http":

                self.send_error(
                    400,
                    "Use CONNECT for HTTPS"
                )

                return

            host = parsed.hostname

            if not host:

                self.send_error(
                    400,
                    "Invalid target host"
                )

                return

            port = parsed.port or 80

            path = parsed.path or "/"

            if parsed.query:
                path += "?" + parsed.query

            print(
                f"[HTTP] {self.command} "
                f"{host}:{port}{path}",
                flush=True
            )

            upstream = socket.create_connection(
                (host, port),
                timeout=CONNECT_TIMEOUT
            )

            upstream.settimeout(HTTP_TIMEOUT)

            # Request line
            request_line = (
                f"{self.command} "
                f"{path} "
                f"{self.request_version}\r\n"
            )

            upstream.sendall(
                request_line.encode("utf-8")
            )

            # Headers
            for key, value in self.headers.items():

                key_lower = key.lower()

                if key_lower in {
                    "proxy-authorization",
                    "proxy-connection",
                    "connection",
                    "keep-alive",
                }:
                    continue

                header = (
                    f"{key}: {value}\r\n"
                )

                upstream.sendall(
                    header.encode("utf-8")
                )

            upstream.sendall(
                b"\r\n"
            )

            # Request body
            content_length = self.headers.get(
                "Content-Length"
            )

            if content_length:

                remaining = int(
                    content_length
                )

                while remaining > 0:

                    chunk = self.rfile.read(
                        min(
                            BUFFER_SIZE,
                            remaining
                        )
                    )

                    if not chunk:
                        break

                    upstream.sendall(chunk)

                    remaining -= len(chunk)

            # Response
            self.stream_http_response(
                upstream,
                head_only
            )

        except Exception as e:

            print(
                f"[HTTP ERROR] "
                f"{type(e).__name__}: {e}",
                flush=True
            )

            try:

                self.send_error(
                    502,
                    "Bad Gateway"
                )

            except Exception:
                pass

        finally:

            if upstream:

                try:
                    upstream.close()
                except Exception:
                    pass

    # =========================================================
    # HTTP response streaming
    # =========================================================

    def stream_http_response(
        self,
        upstream,
        head_only=False
    ):

        # Read response headers
        data = b""

        while b"\r\n\r\n" not in data:

            chunk = upstream.recv(8192)

            if not chunk:
                return

            data += chunk

            if len(data) > 1024 * 1024:

                self.send_error(
                    502,
                    "Response headers too large"
                )

                return

        separator = data.index(
            b"\r\n\r\n"
        ) + 4

        header_data = data[:separator]
        body_data = data[separator:]

        # Send response headers directly.
        self.wfile.write(header_data)

        self.wfile.flush()

        if head_only:
            return

        if body_data:

            self.wfile.write(
                body_data
            )

            self.wfile.flush()

        # Continue streaming
        while True:

            try:

                chunk = upstream.recv(
                    BUFFER_SIZE
                )

            except socket.timeout:

                break

            if not chunk:
                break

            self.wfile.write(chunk)
            self.wfile.flush()

    # =========================================================
    # HTTPS CONNECT
    # =========================================================

    def do_CONNECT(self):

        if not self.authenticate():
            return

        target = self.path

        if ":" not in target:

            self.send_error(
                400,
                "CONNECT requires host:port"
            )

            return

        host, port_text = target.rsplit(
            ":",
            1
        )

        try:

            port = int(port_text)

        except ValueError:

            self.send_error(
                400,
                "Invalid port"
            )

            return

        if not host:

            self.send_error(
                400,
                "Invalid host"
            )

            return

        # Only standard HTTPS ports.
        if port not in {443, 8443}:

            self.send_error(
                403,
                "CONNECT port not allowed"
            )

            return

        upstream = None

        try:

            print(
                f"[CONNECT] {host}:{port}",
                flush=True
            )

            upstream = socket.create_connection(
                (host, port),
                timeout=CONNECT_TIMEOUT
            )

            upstream.settimeout(None)

            client = self.connection
            client.settimeout(None)

            # Tell client tunnel is ready.
            self.send_response(
                200,
                "Connection Established"
            )

            self.send_header(
                "Proxy-Agent",
                "PythonProxy"
            )

            self.end_headers()

            self.wfile.flush()

            # Raw TCP tunnel.
            self.tunnel(
                client,
                upstream
            )

        except Exception as e:

            print(
                f"[CONNECT ERROR] "
                f"{type(e).__name__}: {e}",
                flush=True
            )

            try:

                self.send_error(
                    502,
                    "Unable to connect to target"
                )

            except Exception:
                pass

        finally:

            if upstream:

                try:
                    upstream.close()
                except Exception:
                    pass

    # =========================================================
    # TCP tunnel
    # =========================================================

    def tunnel(
        self,
        client,
        upstream
    ):

        sockets = [
            client,
            upstream
        ]

        while True:

            try:

                readable, _, exceptional = select.select(
                    sockets,
                    [],
                    sockets,
                    TUNNEL_IDLE_TIMEOUT
                )

            except Exception:

                return

            if exceptional:

                return

            if not readable:

                print(
                    "[CONNECT] idle timeout",
                    flush=True
                )

                return

            for source in readable:

                try:

                    data = source.recv(
                        BUFFER_SIZE
                    )

                except Exception:

                    return

                if not data:

                    return

                if source is client:

                    destination = upstream

                else:

                    destination = client

                try:

                    destination.sendall(
                        data
                    )

                except Exception:

                    return

    # =========================================================

    def log_message(
        self,
        format,
        *args
    ):

        print(
            "[SERVER]",
            format % args,
            flush=True
        )


# =============================================================
# Server
# =============================================================

class ThreadedProxyServer(
    ThreadingHTTPServer
):

    daemon_threads = True
    allow_reuse_address = True


def main():

    print(
        "========================================",
        flush=True
    )

    print(
        "Python HTTP/HTTPS Proxy",
        flush=True
    )

    print(
        f"Listening on {HOST}:{PORT}",
        flush=True
    )

    print(
        f"Username: {PROXY_USER}",
        flush=True
    )

    print(
        "CONNECT ports: 443, 8443",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )

    server = ThreadedProxyServer(
        (HOST, PORT),
        ProxyHandler
    )

    try:

        server.serve_forever()

    except KeyboardInterrupt:

        print(
            "Stopping...",
            flush=True
        )

    finally:

        server.server_close()


if __name__ == "__main__":
    main()