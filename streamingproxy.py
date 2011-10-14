from twisted.internet import reactor
from twisted.web import http
from twisted.web.proxy import Proxy, ProxyRequest, ProxyClientFactory, ProxyClient
from StringIO import StringIO

from cachedFile import cachedFile

class InterceptingProxyRequest(ProxyRequest):
    files = { }

    def get(self, uri, *args, **kwargs):
        if uri not in self.files:
            self.files[uri] = cachedFile(uri, *args, **kwargs)

        return self.files[uri]

    def process(self):

        headers = self.getAllHeaders().copy()

        self.content.close()

        if self.method != 'GET':
            return self.error_501()

        if self.clientproto != 'HTTP/1.0':
            return self.error_505()

        self.file = self.get(self.uri, headers)
        self.file.readToMe(self, reactor)

    def error_501():
        self.transport.write("HTTP/1.0 501 Not implemented\r\n")
        self.transport.write("Content-Type: text/html\r\n")
        self.transport.write("\r\n")
        self.transport.write('''<H1>Only GET requests are supported!</H1>''')
        self.transport.loseConnection()

    def error_505():
        self.transport.write("HTTP/1.0 505 HTTP Version Not Supported\r\n")
        self.transport.write("Content-Type: text/html\r\n")
        self.transport.write("\r\n")
        self.transport.write('''<H1>Only HTTP/1.0 is supported!</H1>''')
        self.transport.loseConnection()

    def connectionLost(self, reason):
        print "lost connection!"
        if self.file is not None:
            self.file.stopReading()
        # ProxyRequest.connectionLost(self, reason)

class InterceptingProxy(Proxy):
    requestFactory = InterceptingProxyRequest

factory = http.HTTPFactory()
factory.protocol = InterceptingProxy

reactor.listenTCP(1234, factory)
reactor.run()
