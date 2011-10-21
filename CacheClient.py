import os
import sys

from StringIO import StringIO

from twisted.web.http import HTTPClient
from twisted.internet.protocol import ClientFactory

class CacheClient(HTTPClient):

    fd = None

    def __init__(self, file, host, rest, path, chunk_first, chunk_last):

        self.file = file

        self.host = host
        self.rest = rest

        self.path = path

        self.chunk_first = chunk_first
        self.chunk_last = chunk_last

        self.chunk = self.chunk_first

        self.chunk_buffer = StringIO()
        self.written = 0

        self.directs = [ ]

    def connectionMade(self):

        self.sendCommand('GET', self.rest)

        self.sendHeader('host', self.host)
        self.sendHeader('range', 'bytes=%d-%d' % (self.chunk_first*self.file.chunksize, (self.chunk_last+1)*self.file.chunksize-1) )
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

            # prepare file handle and direct passthrough
            self.fd = open(self.path + os.path.sep + str(self.chunk), 'wb')
            self.file.handleActiveChunk(self.chunk, self)

            print "writing chunk", self.chunk
            self.written = 0
            self.chunk_buffer = StringIO()

        write_len = self.file.chunksize-self.written
        if write_len > len(data):
            write_len = len(data)

        # write to file (we are primarily a cache, after all)
        self.fd.write(data[:write_len])
        # buffer for later joining of direct consumers
        self.chunk_buffer.write(data[:write_len])
        self.written += write_len

        # send data to direct receivers
        if self.directs:
            for direct in self.directs:
                direct.handleDirectChunkData(self.chunk_buffer, self.written)

        if self.written == self.file.chunksize or (self.chunk == self.chunk_last and self.written == (self.file.length % self.file.chunksize) ):
            print "finished chunk", self.chunk

            self.file.handleGotChunk(self.chunk)

            self.fd.close()
            self.fd = None
            self.chunk_buffer.close()
            self.chunk_buffer = None

            if self.directs:
                for direct in self.directs:
                    direct.handleDirectChunkEnd(self.chunk)
                self.directs = [ ]

            self.chunk += 1

            if len(data) > write_len:
                self.handleResponsePart(data[write_len:])

        if self.written > self.file.chunksize:
            print 'WTF: written > chunksize? should never happen!'

    def handleResponseEnd(self):
        # notify of this end, just to be sure (redundant is not problematic)
        if self.directs:
            for direct in self.directs:
                direct.handleDirectChunkEnd(self.chunk)
            del self.directs[self.directs.index(direct)]
        print "response end :)"

    def registerConsumer(self, direct):
        # just a sanity check
        if direct in self.directs:
            print >> sys.stderr, 'WTF: duplicate direct consumer registration'
            return

        # say we're here to supply
        r = direct.handleDirectChunk(self.chunk)
        if not r:
            return

        # make a note for later
        self.directs.append(direct)

        # send what we have so far (the consumer takes care of offset himself!)
        if self.chunk_buffer:
            direct.handleDirectChunkData(self.chunk_buffer, self.written)

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
