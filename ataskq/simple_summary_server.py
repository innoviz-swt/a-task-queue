import http.server
import socketserver
import socket 

from .task_runner import TaskRunner
PORT = 8000


def run_server(task_runner:TaskRunner, port=8000, background=False):
    # run server in background process
    if background:
        import multiprocessing
        p = multiprocessing.Process(target=run_server, args=(task_runner, port), daemon=True)
        p.start()
    
        return p
    class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            """Handle GET requests"""
            # print('GET request received')
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            return self.wfile.write(bytes(task_runner.summary_html(), 'utf-8'))

    with socketserver.TCPServer(("", PORT), SimpleHTTPRequestHandler, bind_and_activate=False) as server:
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.server_bind()
        server.server_activate()
        print("Server listening on port", PORT)
        server.serve_forever()
