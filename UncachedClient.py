from twisted.web.http import HTTPClient
from twisted.internet.protocol import ClientFactory

class UncachedClient(HTTPClient):

    def __init__(self, host, rest, range_from, range_to, direct):

        self.host = host
        self.rest = rest

        self.range_from = range_from
        self.range_to = range_to

        self.written = 0
        self.direct = direct

    def connectionMade(self):

        self.sendCommand('GET', self.rest)

        self.sendHeader('host', self.host)
        self.sendHeader('range', 'bytes=%d-%d' % (self.range_from, self.range_to) )
        self.sendHeader('connection', 'close')

        print 'requesting (uncached) range', self.range_from, 'to', self.range_to, '(', self.range_to-self.range_from, ' bytes)'

        self.endHeaders()

        r = self.direct.handleDirectChunk(None)

    def handleResponsePart(self, data):
        self.direct.handleDirectChunkData(data)
        self.written += len(data)

    def handleResponseEnd(self):
        if self.direct:
            # notify of this end, just to be sure (redundant is not problematic)
            self.direct.handleDirectChunkEnd(None)
            self.direct = None
            print "uncached response end,", self.written, " bytes total"

class UncachedClientFactory(ClientFactory):
    protocol = UncachedClient

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def buildProtocol(self, addr):
        return self.protocol(*self.args, **self.kwargs)

    def clientConnectionFailed(self, connector, reason):
        print "FAIL - Connection Error!"
        # self.father.fail()
