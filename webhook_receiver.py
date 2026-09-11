from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        print("\n--- Grafana Alert Received ---")
        print(body.decode("utf-8"))

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

server = HTTPServer(("0.0.0.0", 5001), Handler)

print("Webhook receiver listening on port 5001...")
server.serve_forever()