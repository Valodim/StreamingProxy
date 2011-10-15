from twisted.web.http import HTTPClient
from twisted.internet.protocol import ClientFactory

class CacheClient(HTTPClient):
    def __init__(self, rest, headers, father, *args, **kwargs):
        print "+ connection", self

        self.rest = rest
        self.headers = headers
        self.father = father

    def connectionMade(self):
        self.sendCommand('GET', self.rest)
        self.sendHeader('host', self.headers['host'])
        # for header in self.headers:
            # self.sendHeader(header, self.headers[header])
        self.endHeaders()

    def handleResponsePart(self, buffer):
        self.father.me.transport.write(buffer)

    def handleResponseEnd(self):
        self.transport.loseConnection()
        self.father.eof()

class CacheClientFactory(ClientFactory):
    protocol = CacheClient

    def __init__(self, rest, headers, father):
        self.father = father
        self.rest = rest
        self.headers = headers

    def buildProtocol(self, addr):
        return self.protocol(self.rest, self.headers, self.father)

    def clientConnectionFailed(self, connector, reason):
        print "FAIL - Connection Error!"
        self.father.fail()
