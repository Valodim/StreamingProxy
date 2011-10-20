import os
import sys

from twisted.web.http import HTTPClient
from twisted.internet.protocol import ClientFactory

class CacheClient(HTTPClient):

    fd = None

    def __init__(self, file, host, rest, path, chunk_first, chunk_last, chunksize):

        self.file = file

        self.host = host
        self.rest = rest

        self.path = path

        self.chunk_first = chunk_first
        self.chunk_last = chunk_last
        self.chunksize = chunksize

        self.chunk = self.chunk_first

    def connectionMade(self):

        self.sendCommand('GET', self.rest)

        self.sendHeader('host', self.host)
        self.sendHeader('range', 'bytes=%d-%d' % (self.chunk_first*self.chunksize, (self.chunk_last+1)*self.chunksize-1) )
        self.sendHeader('connection', 'close')

        print 'requesting chunks', self.chunk_first, 'to', self.chunk_last

        self.endHeaders()

    def handleResponsePart(self, data):

        # need a new file descriptor?
        if not self.fd:
            if self.chunk > self.chunk_last:
                print >> sys.stderr, 'wrote the last chunk, got', len(data), 'bytes left? huh.'
                self.transport.loseConnection()
                return

            self.fd = open(self.path + os.path.sep + str(self.chunk), 'wb')
            print "writing chunk", self.chunk
            self.written = 0

        write_len = self.chunksize-self.written
        if write_len > len(data):
            write_len = len(data)

        self.fd.write(data[:write_len])
        self.written += write_len

        if self.written == self.chunksize:
            self.file.handleGotChunk(self.chunk)
            self.chunk += 1
            self.fd.close()
            self.fd = None

            if len(data) > write_len:
                self.handleResponsePart(data[write_len:])

    def handleResponseEnd(self):
        print "response end :)"

class CacheClientFactory(ClientFactory):
    protocol = CacheClient

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def buildProtocol(self, addr):
        return self.protocol(*self.args, **self.kwargs)

    def clientConnectionFailed(self, connector, reason):
        print "FAIL - Connection Error!"
        # self.father.fail()
