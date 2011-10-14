import urlparse

from twisted.web.proxy import Proxy, ProxyRequest, ProxyClientFactory, ProxyClient

class InterceptingProxyClient(ProxyClient):
    def __init__(self, *args, **kwargs):
        print "+ connection", self
        ProxyClient.__init__(self, *args, **kwargs)

    def handleStatus(self, version, code, message):
        if message:
            # Add a whitespace to message, this allows empty messages
            # transparently
            message = " %s" % (message,)
        self.father.transport.write("%s %s%s\r\n" % (version, code, message))

    def handleHeader(self, key, value):

        # save some important ones
        if key.lower() == 'content-type':
            self.father.setContentType(value)

        if key.lower() == 'length':
            self.father.setContentLength(int(value))

        # print key, " -- ", value
        self.father.transport.write("%s: %s\r\n" % (key, value))

    def handleEndHeaders(self):
        self.father.transport.write("\r\n")

    def handleResponsePart(self, buffer):
        self.father.transport.write(buffer)

    def handleResponseEnd(self):
        self.transport.loseConnection()
        self.father.channel.transport.loseConnection()

    def handleStatus(self, version, code, message):
        if message:
            # Add a whitespace to message, this allows empty messages transparently
            message = " %s" % (message,)
            self.father.transport.write("%s %s%s\r\n" % (version, code, message))

    def handleResponseEnd(self):
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

    def clientConnectionFailed(self, connector, reason):
        self.father.error_cnc(reason)

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

        print headers

    def setContentType(self, type):
        self.type = type

    def setLength(self, length):
        self.length = length

    def request(self, me, reactor, range_from, range_to):
        self.transport = me.transport
        self.me = me

        clientFactory = InterceptingProxyClientFactory('GET', self.rest, 'HTTP/1.0', self.headers, { }, self)
        self.nection = reactor.connectTCP(self.host, self.port, clientFactory)

    def finish(self):
        print "finish."
        self.me.finish()

    def stopReading(self):
        self.nection.disconnect()
