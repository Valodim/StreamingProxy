import os
import sys

from StringIO import StringIO

from twisted.web.http import HTTPClient
from twisted.internet.protocol import ClientFactory

class CacheClient(HTTPClient):

    fd = None

    def __init__(self, file, host, rest, path, chunk_first, chunk_last, offset = 0):

        self.file = file

        self.host = host
        self.rest = rest

        self.path = path

        self.chunk_first = chunk_first
        self.chunk_last = chunk_last
        self.offset = offset

        self.chunk = self.chunk_first

        self.chunk_buffer = StringIO()
        self.written = 0

        self.directs = [ ]

    def connectionMade(self):
        """
            Once a connection is made, this method sends the HTTP headers to
            request the desired data from the server.
        """

        self.sendCommand('GET', self.rest)

        self.sendHeader('host', self.host)
        if self.offset:
            self.sendHeader('range', 'bytes=%d-%d' % (self.chunk_first*self.file.chunksize + self.offset, (self.chunk_last+1)*self.file.chunksize-1) )
        else:
            self.sendHeader('range', 'bytes=%d-%d' % (self.chunk_first*self.file.chunksize, (self.chunk_last+1)*self.file.chunksize-1) )
        self.sendHeader('connection', 'close')

        print 'requesting chunks', self.chunk_first, 'to', self.chunk_last, 'with offset', self.offset

        self.endHeaders()

    def handleResponsePart(self, data):
        """
            The heart of the class. This is called when new data is retrieved,
            which will be forwarded into the disk cache as well as passthrough
            clients.

            TODO: break this down into smaller methods?
        """

        # is this request stale (ie, no longer queued)?
        if not self.offset and not self.file.isQueued(self.chunk):
            print 'dropping download of stale chunk', self.chunk

            # notify of this end, just to be sure (redundant is not problematic)
            if self.directs:
                for direct in self.directs:
                    direct.handleDirectChunkEnd(self.chunk)
                self.directs = [ ]

            self.file.handleGotChunk(self.chunk, True)
            self.transport.loseConnection()
            return

        # need a new file descriptor?
        if not self.fd:
            if self.chunk > self.chunk_last:
                print >> sys.stderr, 'wrote the last chunk, got', len(data), 'bytes left? huh.'
                self.transport.loseConnection()
                return

            self.chunk_buffer = StringIO()
            self.file.handleActiveChunk(self.chunk, self)
            if not self.offset:
                # prepare file handle and direct passthrough
                self.fd = open(self.path + os.path.sep + str(self.chunk), 'wb')

                self.written = 0
                print "processing chunk", self.chunk

            else:
                # to avoid getting into this very loop again
                self.fd = True

                self.chunk_buffer.seek(self.offset)
                self.written = self.offset

                print "processing chunk", self.chunk, 'from byte', self.offset



        write_len = self.file.chunksize-self.written
        if write_len > len(data):
            write_len = len(data)

        # write to file (we are primarily a cache, after all)
        # if there is an offset, we are not caching to disk
        if not self.offset:
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

            self.file.handleGotChunk(self.chunk, not not self.offset)

            if not self.offset:
                self.fd.close()
            self.fd = None
            self.chunk_buffer.close()
            self.chunk_buffer = None

            if self.directs:
                for direct in self.directs:
                    direct.handleDirectChunkEnd(self.chunk)
                self.directs = [ ]

            self.chunk += 1
            self.offset = None

            if len(data) > write_len:
                self.handleResponsePart(data[write_len:])

        if self.written > self.file.chunksize:
            print 'WTF: written > chunksize? should never happen!'

    def handleResponseEnd(self):
        """
            Handler for the end of retrieved data. Forwards the info to
            passthrough clients.

            TODO: This is called twice frequently.. I wonder why?
        """

        # notify of this end, just to be sure (redundant is not problematic)
        if self.directs:
            for direct in self.directs:
                direct.handleDirectChunkEnd(self.chunk)
            self.directs = [ ]
        print "response end :)"

    def registerConsumer(self, direct):
        """
            This is called by CachedRequest (or any compatible object) to
            request passthrough data.

            TODO: rename this method, it's confusing with Twisted's interfaces!
        """
        # just a sanity check
        if direct in self.directs:
            print >> sys.stderr, 'WTF: duplicate direct consumer registration'
            return

        # say we're here to supply
        r = direct.handleDirectChunk(self.chunk, self.offset)
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
