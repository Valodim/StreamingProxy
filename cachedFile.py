import urlparse

from twisted.web.proxy import Proxy, ProxyRequest, ProxyClientFactory, ProxyClient

class InterceptingProxyClient(ProxyClient):
    def __init__(self, *args, **kwargs):
        print "+ connection", self
        ProxyClient.__init__(self, *args, **kwargs)

    def handleHeader(self, key, value):
        print key, " - ", value
        ProxyClient.handleHeader(self, key, value)

    # def handleEndHeaders(self):
        # print "done headers."
        # ProxyClient.handleEndHeaders(self)

      #def handleResponsePart(self, buffer):
            #print buffer
            #if self.image_parser:
                  #self.image_parser.feed(buffer)
            #else:
                  #ProxyClient.handleResponsePart(self, buffer)

    def handleResponseEnd(self):
        print "- connection", self
        # if self.image_parser:
            # image = self.image_parser.close()
            # try:
                # format = image.format
                  # image = image.rotate(180)
                  # s = StringIO()
                  # image.save(s, format)
                  # buffer = s.getvalue()
            # except:
                # buffer = ""
            # ProxyClient.handleHeader(self, "Content-Length", len(buffer))
            # ProxyClient.handleEndHeaders(self)
            # ProxyClient.handleResponsePart(self, buffer)
        ProxyClient.handleResponseEnd(self)

class InterceptingProxyClientFactory(ProxyClientFactory):
    protocol = InterceptingProxyClient

class cachedFile(object):
    ports = {"http" : 80}

    def __init__(self, uri, headers):
        parsed =  urlparse.urlparse(uri)

        self.protocol =  parsed[0]
        self.host =  parsed[1]
        self.port =  self.ports[self.protocol]

        if ':' in self.host:
            self.host,  self.port =  self.host.split(':')
            self.port =  int(self.port)
        self.rest =  urlparse.urlunparse(('',  '')  +  parsed[2:])
        if not self.rest:
            self.rest = self.rest +  '/'

        if 'host' not in headers:
            headers['host'] = self.host
        self.headers = headers

    def readToMe(self, me, reactor):
        clientFactory = InterceptingProxyClientFactory('GET', self.rest, 'HTTP/1.0', self.headers, { }, me)

        self.nection = reactor.connectTCP(self.host, self.port, clientFactory)

    def stopReading(self):
        self.nection.disconnect()
