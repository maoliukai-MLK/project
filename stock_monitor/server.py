#!/usr/bin/env python3
"""
Stock Monitor Web Demo - stdlib http.server + index.html
GET  /                -> index.html
GET  /api/quote       -> {market, code} -> monitor.quote()
GET  /api/markets     -> 支持的市场 + 示例代码
"""
import json
import os
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import monitor

BASE = os.path.dirname(os.path.abspath(__file__))
INDEX = os.path.join(BASE, "index.html")
PORT = int(os.environ.get("PORT", 8000))

MARKETS = {
    "hs": {"label": "沪深 A 股", "placeholder": "如 002334 / 600519",
           "examples": [("002334", "英威腾"), ("301007", "德迈仕")]},
    "us": {"label": "美股",      "placeholder": "如 TSLA / AAPL",
           "examples": [("TSLA", "特斯拉"), ("AAPL", "苹果")]},
    "hk": {"label": "港股",      "placeholder": "如 00700 / 09988",
           "examples": [("00700", "腾讯"), ("09988", "阿里巴巴")]},
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write(f"[{self.log_date_time_string()}] {fmt % args}\n")

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self.send_error(404, "Not Found")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/" or u.path == "/index.html":
            self._send_file(INDEX, "text/html; charset=utf-8")
            return
        if u.path == "/api/markets":
            self._send_json(MARKETS)
            return
        if u.path == "/api/quote":
            qs = urllib.parse.parse_qs(u.query)
            market = (qs.get("market", [""])[0] or "").lower()
            code = (qs.get("code", [""])[0] or "").strip()
            if market not in MARKETS:
                self._send_json({"error": f"不支持的市场: {market}"}, 400)
                return
            if not code:
                self._send_json({"error": "缺少 code 参数"}, 400)
                return
            try:
                data = monitor.quote(market, code)
                self._send_json(data)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        self.send_error(404, "Not Found")


def main():
    print(f"📊 Stock Monitor Web Demo")
    print(f"   打开 http://localhost:{PORT}/")
    print(f"   按 Ctrl+C 停止")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
