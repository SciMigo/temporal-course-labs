"""A model with state: the fake LLM behind lab 3.3.

    python model_server.py            # http://127.0.0.1:8765

GET /decide?prompt=...  answers "next: continue" the first time it is asked and "next: finish"
every time after that, and prints one line per request — so you can count how often the model
was really called. GET /reset starts the count over.

That is all a real model shares with this one for our purposes: asking it twice does not give
the same answer twice, and every ask is billed.
"""
from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

REQUESTS = 0


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        global REQUESTS
        url = urlparse(self.path)
        if url.path == "/reset":
            REQUESTS = 0
            body = "reset"
        elif url.path == "/decide":
            REQUESTS += 1
            body = "next: continue" if REQUESTS == 1 else "next: finish"
            prompt = parse_qs(url.query).get("prompt", [""])[0]
            print(f"model call #{REQUESTS}: {prompt[:50]!r} -> {body!r}", flush=True)
        else:
            self.send_response(404)
            self.end_headers()
            return
        data = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args) -> None:  # quiet: only our own lines
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    print(f"model server on http://127.0.0.1:{args.port}  (GET /decide, GET /reset)", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
