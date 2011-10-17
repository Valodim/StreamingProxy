from twisted.internet import reactor, defer
from twisted.web import http
from twisted.web.proxy import Proxy, ProxyRequest, ProxyClientFactory, ProxyClient
from StringIO import StringIO

from cachedFile import cachedFile

class InterceptingProxyRequest(ProxyRequest):
    files = { }

    def get(self, uri, *args, **kwargs):
        return cachedFile(uri, *args, **kwargs)

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

        # got a range request?
        if 'range' in headers and headers['range'][0:6] == 'bytes=':
            self.range_from, self.range_to = headers['range'][6:].split('-')
        else:
            self.range_from, self.range_to = (None,None)

        print self.range_from, " ", self.range_to

        self.file = self.get(self.uri, headers)

        d = self.file.getInfo()
        d.addCallback(self.process2)

    def process2(self, info):
        print "here we go!", info

        self.transport.write("HTTP/1.0 200 OK\r\n")

        if 'type' in info and info['type']:
            self.transport.write("content-type: %s\r\n" % info['type'])
        if 'etag' in info and info['etag']:
            self.transport.write("etag: %s\r\n" % info['etag'])

        if self.range_from is None and self.range_to is None:
            self.transport.write("content-length: %s\r\n" % info['length'])

        self.transport.write("accept-ranges: bytes\r\n")

        self.transport.write("\r\n")

        self.file.request(self, self.range_from, self.range_to)

    def error_cnc(self, reason):
        print "ERROR: ", reason
        self.transport.write("HTTP/1.0 501 Gateway error\r\n")
        self.transport.write("Content-Type: text/html\r\n")
        self.transport.write("\r\n")
        self.transport.write('''<H1>Could not connect</H1>''')
        self.transport.loseConnection()

    def error_501(self):
        self.transport.write("HTTP/1.0 501 Not implemented\r\n")
        self.transport.write("Content-Type: text/html\r\n")
        self.transport.write("\r\n")
        self.transport.write('''<H1>Only GET requests are supported!</H1>''')
        self.transport.loseConnection()

    def error_505(self):
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
