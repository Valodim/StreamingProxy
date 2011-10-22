import sys

from twisted.python import log
from twisted.internet import reactor
from twisted.web import http
from twisted.web.proxy import Proxy, ProxyRequest

import CachedFile

class StreamingProxyRequest(ProxyRequest):

    def process(self):

        headers = self.getAllHeaders().copy()

        # got a range request?
        if 'range' in headers and headers['range'][0:6] == 'bytes=':
            self.range_from, self.range_to = headers['range'][6:].split('-')
            try:
                self.range_to = int(self.range_to)
            except ValueError:
                self.range_to = None
            self.range_from = int(self.range_from)
        else:
            self.range_from, self.range_to = (None,None)

        # get the multiplexing cachedFile instance
        self.file = CachedFile.cachedFileGet(self.uri)

        d = self.file.getInfo()
        if self.range_from is None:
            print "full request"
            d.addCallback(self.fullRequest)
        else:
            d.addCallback(self.rangeRequest)
        d.addErrback(self.someError)


    def connectionLost(self, reason):
        print "lost connection!"
        # if self.file is not None:
            # self.file.stopReading()
        # ProxyRequest.connectionLost(self, reason)


    def fullRequest(self, x = None):

        self.transport.write("HTTP/1.0 200 OK\r\n")
        self.transport.write("content-length: %d\r\n" % (self.file.length))

        self.transport.write("content-type: video/x-matroska\r\n")
        self.transport.write("accept-range: bytes\r\n")
        self.transport.write("connection: close\r\n")

        self.transport.write("\r\n")

        self.file.request(self.transport, 0, self.file.length)


    def rangeRequest(self, x = None):
        if self.range_to is None:
            self.range_to = self.file.length

        range_len = self.range_to-self.range_from

        print "ranged request:", self.range_from, "-", self.range_to, "(", (range_len), ")"

        self.transport.write("HTTP/1.0 206 Partial Content\r\n")
        self.transport.write("content-length: %d\r\n" % (range_len))
        self.transport.write("content-range: bytes %d-%d/%d\r\n" % (self.range_from,self.range_to,self.file.length))

        self.transport.write("content-type: video/x-matroska\r\n")
        self.transport.write("accept-range: bytes\r\n")
        self.transport.write("connection: close\r\n")

        self.transport.write("\r\n")

        self.file.request(self.transport, self.range_from, self.range_to)


    def someError(self, x = None):
        if x:
            x.printTraceback()

        print >> sys.stderr, 'Could not retrieve info'

        self.transport.write("HTTP/1.0 501 Gateway Error\r\n")
        self.transport.write("connection: close\r\n")

        self.transport.write("\r\n")

        self.transport.write("Something bad happened :(")


class StreamingProxy(Proxy):
    requestFactory = StreamingProxyRequest


log.startLogging(sys.stdout)

factory = http.HTTPFactory()
factory.protocol = StreamingProxy

reactor.listenTCP(1234, factory)
reactor.run()
