import base64
import http.client
import os
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit

USERNAME = os.environ.get("PROXY_USER", "test")
PASSWORD = os.environ.get("PROXY_PASSWORD", "change-me")

PORT = int(os.environ.get("PORT", "8080"))


class ProxyHandler(BaseHTTPRequestHandler):

    def authenticate(self):
        header = self.headers.get("Proxy-Authorization")

        if not header or not header.startswith("Basic "):
            self.send_response(407)
            self.send_header("Proxy-Authenticate", 'Basic realm="Proxy"')
            self.end_headers()
            return False

        try:
            encoded = header[6:]
            decoded = base64.b64decode(encoded).decode("utf-8")
            user, password = decoded.split(":", 1)
        except Exception:
            return False

        return user == USERNAME and password == PASSWORD

    def do_GET(self):

        if not self.authenticate():
            self.send_response(407)
            self.send_header("Proxy-Authenticate", 'Basic realm="Proxy"')
            self.end_headers()
            return

        target = self.path

        if not target.startswith(("http://", "https://")):
            self.send_error(400, "Proxy requires absolute URL")
            return

        parsed = urlsplit(target)

        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)

        if not host:
            self.send_error(400)
            return

        try:
            conn = http.client.HTTPConnection(host, port, timeout=20)

            headers = {}

            for key, value in self.headers.items():
                if key.lower() not in {
                    "proxy-authorization",
                    "proxy-connection",
                    "connection",
                }:
                    headers[key] = value

            path = parsed.path or "/"

            if parsed.query:
                path += "?" + parsed.query

            conn.request("GET", path, headers=headers)

            response = conn.getresponse()

            self.send_response(response.status)

            for key, value in response.getheaders():
                if key.lower() not in {
                    "connection",
                    "transfer-encoding",
                }:
                    self.send_header(key, value)

            self.end_headers()

            while True:
                data = response.read(65536)

                if not data:
                    break

                self.wfile.write(data)

            conn.close()

        except Exception as e:
            self.send_error(502, str(e))

    def log_message(self, format, *args):
        print(format % args, flush=True)


server = HTTPServer(("0.0.0.0", PORT), ProxyHandler)

print(f"Proxy listening on 0.0.0.0:{PORT}", flush=True)

server.serve_forever()