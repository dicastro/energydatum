from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer

import constants

PORT = 8080

class FakeRedirect(SimpleHTTPRequestHandler):
    def __init__(self, *args):
        super().__init__(*args, directory='./docs')

    def do_GET(self):
        print(self.path)

        if self.path.startswith(constants.CONTEXT_PATH):
            self.send_response(302)
            new_path = f'http://localhost:{PORT}{self.path.replace(constants.CONTEXT_PATH, "")}'
            self.send_header('Location', new_path)
            self.end_headers()
        else:
            super().do_GET()


print('Starting server on port 8080 ...')
TCPServer(('', PORT), FakeRedirect).serve_forever()
