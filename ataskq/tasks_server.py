from curses.ascii import ESC
import http.server
import socketserver
import socket 
from urllib.parse import urlparse
from http import HTTPStatus

from .task_runner import TaskRunner, EQueryType
PORT = 8000


def run_server(task_runner:TaskRunner, port=8000, background=False):
    # run server in background process
    if background:
        import multiprocessing
        p = multiprocessing.Process(target=run_server, args=(task_runner, port), daemon=True)
        p.start()
    
        return p
    class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
        def html(self, query_type:EQueryType):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            return self.wfile.write(bytes(task_runner.html(query_type), 'utf-8'))

        def do_404(self):
            self.send_error(HTTPStatus.NOT_FOUND, 'Not found')
            return self.wfile.write(bytes('Not found'), 'utf-8')


        def do_GET(self):
            """Handle GET requests"""
            parsed_url = urlparse(self.path)    
            if parsed_url.path == '/' or parsed_url.path == '/tasks_summary':
                return self.html(EQueryType.TASKS_SUMMARY) 
            if parsed_url.path == '/' or parsed_url.path == '/num_units_summary':
                return self.html(EQueryType.NUM_UNITS_SUMMARY)
            elif parsed_url.path == '/tasks':
                return self.html(EQueryType.TASKS) 
            else:
                return self.do_404()

    with socketserver.TCPServer(("", PORT), SimpleHTTPRequestHandler, bind_and_activate=False) as server:
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.server_bind()
        server.server_activate()
        print("Server listening on port", PORT)
        server.serve_forever()