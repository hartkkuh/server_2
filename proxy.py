import base64
import os
import select
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8080"))

PROXY_USER = os.environ.get("PROXY_USER", "testuser")
PROXY_PASSWORD = os.environ.get("PROXY_PASSWORD", "change-this-password")

BUFFER_SIZE = 64 * 1024
CONNECT_TIMEOUT = 15
SOCKET_TIMEOUT = 60


class ProxyHandler(BaseHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

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
            decoded = base64.b64decode(encoded).decode("utf-8")
            username, password = decoded.split(":", 1)
        except Exception:
            self.request_authentication()
            return False

        if username != PROXY_USER or password != PROXY_PASSWORD:
            self.request_authentication()
            return False

        return True

    def request_authentication(self):
        self.send_response(407)
        self.send_header(
            "Proxy-Authenticate",
            'Basic realm="Proxy"'
        )
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ---------------------------------------------------------
    # HTTP proxy
    # ---------------------------------------------------------

    def do_GET(self):
        if not self.authenticate():
            return

        self.forward_http()

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

    def forward_http(self, head_only=False):

        try:
            parsed = urlsplit(self.path)

            if parsed.scheme not in ("http", "https"):
                self.send_error(
                    400,
                    "Proxy requires absolute HTTP URL"
                )
                return

            host = parsed.hostname

            if not host:
                self.send_error(400, "Invalid target")
                return

            if parsed.port:
                port = parsed.port
            else:
                port = 443 if parsed.scheme == "https" else 80

            if parsed.scheme == "https":
                # HTTPS should normally arrive through CONNECT.
                self.send_error(
                    400,
                    "Use CONNECT for HTTPS"
                )
                return

            path = parsed.path or "/"

            if parsed.query:
                path += "?" + parsed.query

            upstream = socket.create_connection(
                (host, port),
                timeout=CONNECT_TIMEOUT
            )

            upstream.settimeout(SOCKET_TIMEOUT)

            request_line = (
                f"{self.command} {path} "
                f"{self.request_version}\r\n"
            )

            upstream.sendall(request_line.encode())

            # Forward headers, excluding proxy-specific headers.
            for key, value in self.headers.items():

                if key.lower() in {
                    "proxy-authorization",
                    "proxy-connection",
                    "connection",
                    "keep-alive",
                }:
                    continue

                header = f"{key}: {value}\r\n"
                upstream.sendall(header.encode())

            upstream.sendall(b"\r\n")

            # Forward request body if present.
            content_length = self.headers.get("Content-Length")

            if content_length:
                remaining = int(content_length)

                while remaining > 0:
                    chunk = self.rfile.read(
                        min(BUFFER_SIZE, remaining)
                    )

                    if not chunk:
                        break

                    upstream.sendall(chunk)
                    remaining -= len(chunk)

            # Read upstream response.
            response = self.read_http_response(upstream)

            if not response:
                self.send_error(502, "Empty upstream response")
                upstream.close()
                return

            header_data, body = response

            self.wfile.write(header_data)

            if body:
                self.wfile.write(body)

            # Continue streaming the response.
            while True:

                try:
                    data = upstream.recv(BUFFER_SIZE)
                except socket.timeout:
                    break

                if not data:
                    break

                self.wfile.write(data)
                self.wfile.flush()

            upstream.close()

        except Exception as e:

            print(
                f"[HTTP ERROR] {type(e).__name__}: {e}",
                flush=True
            )

            try:
                self.send_error(502, "Bad Gateway")
            except Exception:
                pass

    def read_http_response(self, sock):

        data = b""

        while b"\r\n\r\n" not in data:

            chunk = sock.recv(8192)

            if not chunk:
                return None

            data += chunk

            if len(data) > 1024 * 1024:
                return None

        separator = data.index(b"\r\n\r\n") + 4

        headers = data[:separator]
        body = data[separator:]

        return headers, body

    # ---------------------------------------------------------
    # HTTPS CONNECT
    # ---------------------------------------------------------

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

        host, port_text = target.rsplit(":", 1)

        try:
            port = int(port_text)
        except ValueError:
            self.send_error(400, "Invalid port")
            return

        if not host:
            self.send_error(400, "Invalid host")
            return

        # Safety: only allow normal TLS ports.
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

            # Tell the browser that the tunnel is established.
            self.send_response(200, "Connection Established")
            self.send_header("Proxy-Agent", "PythonProxy")
            self.end_headers()

            # From this point onward, this socket becomes a raw
            # bidirectional TCP tunnel.
            client = self.connection

            client.settimeout(None)

            self.tunnel(client, upstream)

        except Exception as e:

            print(
                f"[CONNECT ERROR] {type(e).__name__}: {e}",
                flush=True
            )

            try:
                self.send_error(502, "Unable to connect to target")
            except Exception:
                pass

        finally:

            if upstream:
                try:
                    upstream.close()
                except Exception:
                    pass

    def tunnel(self, client, upstream):

        sockets = [client, upstream]

        while True:

            try:

                readable, _, exceptional = select.select(
                    sockets,
                    [],
                    sockets,
                    300
                )

            except Exception:
                break

            if exceptional:
                break

            if not readable:
                break

            for sock in readable:

                try:
                    data = sock.recv(BUFFER_SIZE)

                except Exception:
                    return

                if not data:
                    return

                if sock is client:
                    destination = upstream
                else:
                    destination = client

                try:
                    destination.sendall(data)

                except Exception:
                    return

    # ---------------------------------------------------------

    def log_message(self, format, *args):

        print(
            "[HTTP]",
            format % args,
            flush=True
        )


class ThreadedProxyServer(ThreadingHTTPServer):

    daemon_threads = True

    allow_reuse_address = True


def main():

    print(
        f"Proxy starting on {HOST}:{PORT}",
        flush=True
    )

    print(
        f"Username: {PROXY_USER}",
        flush=True
    )

    server = ThreadedProxyServer(
        (HOST, PORT),
        ProxyHandler
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        pass

    finally:
        server.server_close()


if __name__ == "__main__":
    main()