#!/usr/bin/env python3
import click
import logging
import socket
import socketserver
import threading
from urllib.parse import urlparse
from queue import Queue

FORMAT_CONS = '%(asctime)s %(name)-26s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT_CONS)

class MJEPGClient(threading.Thread):
    def __init__(self, mjpegurl):
        threading.Thread.__init__(self)
        self.url_components = urlparse(mjpegurl)
        self.ready = []
        self.receivers = 0
        self.log = logging.getLogger('MJEPGClient')


    def run(self):
        client_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        remote_host = self.url_components.netloc.split(':')[0]
        remote_port = int(self.url_components.port) if self.url_components.port is not None else 80
        self.log.info('Connecting to {}:{}'.format(remote_host, remote_port))
        client_socket.connect((remote_host, remote_port))
        get_request = "GET {}?{} HTTP/1.1\r\n\r\n".format(
            self.url_components.path,
            self.url_components.query
        ).encode('utf-8')
        self.log.debug("Request: {}".format(get_request))
        client_socket.send(get_request)
        boundary = None
        while not boundary:
            data = client_socket.recv(1024)
            if not data:
                break
            if b'boundary=' in data:
                for item in data.decode('utf-8').split():
                    if 'boundary=' in item:
                        boundary = b'\r\n\r\n%s\r\n' % item.split('=')[1].encode('utf-8')
                        self.log.debug('Multipart boundary: {}'.format(boundary))
                        break
        buffer = bytes()
        current_offset = 0
        test_counter = 0
        while self.receivers > 0:
            buffer += client_socket.recv(1024)
            offset = buffer.find(boundary, current_offset)
            if offset == -1:
                current_offset = len(buffer) - len(boundary)
            else:
                for r in self.ready:
                    r.out_queue.put(buffer[:offset])
                    self.ready.remove(r)
                test_counter = test_counter + 1
                current_offset = 0
                buffer = buffer[offset+len(boundary):]
        self.log.info('Closing down.')

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.log = logging.getLogger('ThreadedTCPRequestHandler')
        self.log.info('New connection from: {}'.format(self.client_address[0]))
        if self.server.mjpegclient is None or not self.server.mjpegclient.is_alive():
            self.log.info("No MJPEGClient. Starting new one.")
            self.server.mjpegclient = MJEPGClient(self.server.mjpegurl)
            self.server.mjpegclient.receivers += 1
            self.server.mjpegclient.start()
        else:
            # Need this else clause because we need to increment before starting above
            self.server.mjpegclient.receivers += 1
        self.out_queue = Queue()
        request_buffer = bytes()
        for i in range(10):
            request_buffer += self.request.recv(1024)
            if b'\r\n\r\n' in request_buffer:
                # We have one purpose only.
                # We don't care what the client has to say.
                # ...but let's log it anyway.
                self.log.debug(request_buffer)
                break

        self.request.send(b'HTTP/1.0 200 OK\r\n')
        self.request.send(b'Connection: Close\r\n')
        self.request.send(b'Server: mjpeg-proxy\r\n')
        self.request.send(b'Content-Type: multipart/x-mixed-replace; boundary=--myboundary\r\n\r\n')
        while True:
            try:
                self.server.mjpegclient.ready.append(self)
                frame = self.out_queue.get()
                self.request.send(b'--myboundary\r\n')
                self.request.send(frame)
            except BrokenPipeError:
                self.log.debug('Connection closed from {}: Broken pipe.'.format(self.client_address[0]))
                break
            except ConnectionResetError:
                self.log.debug('Connection closed from {}: Connection reset by peer.'.format(self.client_address[0]))
                break
        self.server.mjpegclient.receivers -= 1

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def __init__(self, bindhost, handler, mjpegurl):
        socketserver.TCPServer.__init__(self, bindhost, handler)
        self.mjpegurl = mjpegurl
        self.mjpegclient = None


@click.command()
@click.argument(
    'mjpegurl',
    required=True,
)
@click.option(
    '--listenhost',
    '-l',
    default='localhost',
    help='Address/host to bind to'
)
@click.option(
    '--listenport',
    '-p',
    default=8080,
    type=int,
    help='Port to bind to'
)
def cli(mjpegurl, listenhost, listenport):

    socketserver.TCPServer.allow_reuse_address = True
    server = ThreadedTCPServer((listenhost, listenport), ThreadedTCPRequestHandler, mjpegurl)

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    server_thread.join()
    server.shutdown()
    server.server_close()

if __name__ == '__main__':
    cli()
