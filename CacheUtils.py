import urlparse

from twisted.internet import reactor, defer
from twisted.web.client import HTTPClientFactory

def getUriHEAD(uri, headers):
    parsed =  urlparse.urlparse(uri)

    protocol =  parsed[0]
    host = parsed[1]
    port = 80

    if ':' in host:
        host,  port =  host.split(':')
        port =  int(port)
    rest =  urlparse.urlunparse(('',  '')  +  parsed[2:])
    if not rest:
        rest = rest +  '/'

    if 'host' not in headers:
        headers['host'] = host

    clientFactory = HTTPClientFactory(uri, 'HEAD', headers=headers)
    reactor.connectTCP(host, port, clientFactory)

    d = defer.Deferred()
    def cb(_):
        d.callback(clientFactory.response_headers)

    clientFactory.deferred.addCallback(cb)

    return d

if __name__ == "__main__":

    d = getUriHEAD('http://www.heise.de', { })
    # d = getUriHEAD('http://cupcake:81/series/My%20Little%20Pony%3a%20Friendship%20is%20Magic/My%20Little%20Pony%3a%20Friendship%20is%20Magic%20S01E20%20Green%20Isn%27t%20Your%20Color.mkv', { })

    def printf(x):
        print x
    d.addCallback(printf)

    reactor.run()
