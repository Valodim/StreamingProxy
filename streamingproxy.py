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

        self.content.seek(0,  0)
        data =  self.content.read()

        self.file = self.get(self.uri, headers, data)
        self.file.readToMe(self, reactor)

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
