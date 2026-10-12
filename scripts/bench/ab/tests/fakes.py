"""Tiny in-process HTTP servers the tests point the harness at."""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class Server:
    """A server whose behaviour is a function (method, path, headers, body) -> (status, body, headers)."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _do(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n).decode() if n else ""
                outer.calls.append((self.command, self.path, dict(self.headers), body))
                status, out, hdr = outer.handler(self.command, self.path, dict(self.headers), body)
                data = out.encode() if isinstance(out, str) else out
                self.send_response(status)
                for k, v in (hdr or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_DELETE = do_PUT = _do

        self.httpd = HTTPServer(("127.0.0.1", 0), H)
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=lambda: self.httpd.serve_forever(poll_interval=0.01), daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class FakeToxiproxy(Server):
    """toxiproxy's API for the one proxy `s3`: POST adds (409 when present), DELETE removes, GET lists."""

    def __init__(self, broken=False, ignores=False):
        self.toxics = {}
        self.broken, self.ignores = broken, ignores
        super().__init__(self.handle)

    def handle(self, method, path, headers, body):
        if self.broken:
            return 500, "boom", {}
        if path == "/proxies/s3/toxics" and method == "GET":
            return 200, json.dumps(list(self.toxics.values())), {}
        if path == "/proxies/s3/toxics" and method == "POST":
            t = json.loads(body)
            if t["name"] in self.toxics:
                return 409, "exists", {}
            if not self.ignores:
                self.toxics[t["name"]] = t
            return 200, json.dumps(t), {}
        if path.startswith("/proxies/s3/toxics/"):
            name = path.rsplit("/", 1)[1]
            if method == "POST":
                if not self.ignores:
                    self.toxics[name] = json.loads(body)
                return 200, body, {}
            if method == "DELETE":
                return (204, "", {}) if self.toxics.pop(name, None) else (404, "no", {})
        return 404, "", {}
