import urlparse
import hashlib
import os

from twisted.internet import reactor, defer
from twisted.web.proxy import Proxy, ProxyRequest
from twisted.protocols.basic import FileSender

from zope.interface import implements

from cachedChunk import cachedChunk
from CacheSender import CacheSender

import utils

class cachedFile(object):
    ports = {"http" : 80}

    requests = [ ]

    active_chunks = { }

    got_info = False

    def __init__(self, uri, headers):
        """
        What this is supposed to do:
         - find out length of file, possibly get ETag
         - determine reasonable chunk size
        """

        self.uri = uri
        self.hash = hashlib.sha1(uri).digest()

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

        self.d = utils.getUriHEAD(uri, headers)
        def info(headers):
            self.got_info = True

            self.length = int(headers['content-length'][0])
            self.type = headers['content-type'][0]
            self.etag = headers['etag'][0]

            self.ranged = headers['accept-ranges'][0] == 'bytes'

            # in kb, so this is 10MB
            self.chunksize = 10*1024
            self.chunks = self.length / self.chunksize +1

            print "got info. length:", self.length, ', type:', self.type, ', etag:', self.etag, ', accepts range' if self.ranged else ''
            print "chunksize ", self.chunksize, ", using ", self.chunks, "chunks"

        self.d.addCallback(info)

    def getInfo(self):
        d = defer.Deferred()
        def doGetSize(x):
            d.callback({
                'length': self.length,
                'type': self.type,
                'etag': self.etag,
            })
        self.d.addCallback(doGetSize)

        return d

    def request(self, me, range_from, range_to):
        """
        What this is supposed to do:
         - add first requested chunk to request queue
        """

        # do we need to wait for this?
        if not self.got_info:
            def doReq(x):
                self.request(me, range_from, range_to)
            self.d.addCallback(doReq)
            return

        chunk_first = (int(range_from) / self.chunksize) if range_from is not None else 0
        chunk_last = (int(range_to) / self.chunksize) if range_to is not None else self.chunks

        print "requesting: ", chunk_first, "-", chunk_last

        if chunk_first != 0 or chunk_last != self.chunks:
            print "not yet implemented - error!"
            return

        r = CachedRequest(self, me.transport, chunk_first, chunk_last, range_from % self.chunksize if range_from is not None else 0)

    def finish(self):
        print "finish."
        self.me.finish()

    def stopReading(self):
        return

        if self.nection is not None:
            self.nection.disconnect()

class CachedRequest(object):
    """ 
        This class delivers a specific range of a file to a consumer,
        taking care of all intermediate caching (at some point :) )
    """

    chunksize = 10*1024*1024

    def __init__(self, f, consumer, range_from, range_to):
        self.f = f

        self.chunk_first = range_from / self.chunksize
        self.chunk_last = range_to / self.chunksize
        self.chunk_offset = range_from % self.chunksize
        self.chunk_last_length = range_to % self.chunksize

        # start with first chunk :)
        self.chunk = self.chunk_first

        self.consumer = consumer

        self.sendChunk(None)

    def sendChunk(self, x):

        if self.chunk > self.chunk_last:
            print 'EOF :)'
            self.consumer.loseConnection()
            return

        # open first chunk and skip some
        # fd = open(f.path + os.path.sep + str(chunk_first+1), 'rb')
        # fd.seek(chunk_offset)
        fd = open('/home/valodim/space/My Little Pony: Friendship is Magic S01E04 Applebuck Season.mkv', 'rb')

        print "at chunk", self.chunk

        # seek to chunk offset in file, possibly with added offset within the chunk
        if self.chunk == self.chunk_first and self.chunk_offset:
            fd.seek(self.chunk * self.chunksize + self.chunk_offset)
        else:
            fd.seek(self.chunk * self.chunksize)

        self.chunk += 1

        # connect the producer
        self.producer = CacheSender()
        if self.chunk_last_length and (self.chunk-1) == self.chunk_last:
            d = self.producer.beginFileTransfer(fd, self.chunk_last_length, self.consumer)
        else:
            d = self.producer.beginFileTransfer(fd, self.chunksize, self.consumer)

        d.addCallback(self.sendChunk)

        def failure(x):
            x.printTraceback()
            self.consumer.loseConnection()
        d.addErrback(failure)

