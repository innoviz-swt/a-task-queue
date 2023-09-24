import http.server
import socketserver
import socket
import webbrowser
from urllib.parse import urlparse
from http import HTTPStatus

from .db_handler import EQueryType, DBHandler


def run_server(db_hanlder: DBHandler, port=8000, popup=False, print_logs=True, background=False, host='localhost', popup_ip_host=False):
    # run server in background process
    if background:
        import multiprocessing
        p = multiprocessing.Process(
            target=run_server, args=(db_hanlder,), kwargs=dict(port=port, print_logs=print_logs, popup=popup), daemon=True)
        p.start()
        
        if popup:
            if popup_ip_host:
                host = socket.gethostbyname(socket.gethostname())
            webbrowser.open(f"http://{host}:{port}/?auto_refresh=true", 1)


        return p

    class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            if print_logs:
                http.server.SimpleHTTPRequestHandler.log_message(self, format, *args)

        def html(self, query_type: EQueryType):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            return self.wfile.write(bytes(db_hanlder.html(query_type), 'utf-8'))

        def do_404(self):
            self.send_error(HTTPStatus.NOT_FOUND, 'Not found')
            return self.wfile.write(bytes('Not found'), 'utf-8')

        def do_GET(self):
            """Handle GET requests"""
            parsed_url = urlparse(self.path)

            # handle favicon
            if self.path == '/favicon.ico':
                self.send_response(200)
                self.send_header('Content-type', 'image/x-icon')
                self.end_headers()
                return
            
            # rout to relevant query type
            query_types = {
                '/': EQueryType.JOBS_STATUS,
                '/tasks': EQueryType.TASKS,
                '/tasks_status': EQueryType.TASKS_STATUS,
                '/jobs': EQueryType.JOBS,
                '/jobs_status': EQueryType.JOBS_STATUS,
            } 

            query_type = query_types.get(parsed_url.path)
            if query_type is None:
                return self.do_404()

            return self.html(query_type)

    if isinstance(port, int):
        ports = [port]
    elif isinstance(port, (tuple, list)):
        if not (2 <= len(port) <= 3):
            raise RuntimeError("Unsupproted port, can be either int or (start, end, step) where step is optional")
        else:
            ports = range(*port)
    else:
        raise RuntimeError("Unsupproted port, can be either int or (start, end, step) where step is optional")

    for port in ports:
        try:
            with socketserver.TCPServer((host, port), SimpleHTTPRequestHandler, bind_and_activate=False) as server:
                server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.server_bind()
                server.server_activate()
                if print_logs:
                    print("Server listening on port", port)
                server.serve_forever()

        except OSError as ex:
            if ex.args[0] == 98:
                continue
            else:
                raise RuntimeError("Unexpected error when opening browser")
