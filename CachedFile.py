import urlparse
import hashlib
import os

from twisted.internet import reactor, defer
from twisted.web.proxy import Proxy, ProxyRequest
from twisted.protocols.basic import FileSender

from zope.interface import implements

from CacheClient import CacheClientFactory
from CacheSender import CacheSender
import CacheUtils

class CachedFile(object):
    ports = {"http" : 80}

    missing_chunks = None

    got_info = False

    def __init__(self, uri):
        """
        What this is supposed to do:
         - find out length of file, possibly get ETag
         - determine reasonable chunk size
        """

        self.uri = uri
        self.hash = hashlib.sha1(uri).hexdigest()

        self.path = '/home/valodim/space/mlp/' + self.hash + '/'
        if not os.access(self.path, os.F_OK):
            os.mkdir(self.path)

        parsed =  urlparse.urlparse(uri)

        self.protocol =  parsed[0]
        self.host =  parsed[1]
        self.port =  self.ports[self.protocol]

        if ':' in self.host:
            self.host,  self.port =  self.host.split(':')
            self.port =  int(self.port)
        self.rest = urlparse.urlunparse(('',  '')  +  parsed[2:])
        if not self.rest:
            self.rest = self.rest +  '/'

        self.headers = { }
        self.headers['host'] = self.host

    def getInfo(self):

        d = defer.Deferred()

        # no info in cache? request it
        if not self.got_info:
            di = CacheUtils.getUriHEAD(self.uri, self.headers)
            di.addCallback(self.handleInfo)

            # and once we have it, fire our deferred
            def cb(x):
                d.callback(self)
            di.addCallback(cb)
            return d

        # the info is available - report success right away
        d.callback(self)
        return d


    def handleInfo(self, headers):
        self.got_info = True

        self.length = int(headers['content-length'][0])
        self.type = headers['content-type'][0]
        self.etag = headers['etag'][0]

        self.ranged = headers['accept-ranges'][0] == 'bytes'

        # in bytes, so this is 10MB
        self.chunksize = 8*1024*1024
        self.chunks = self.length / self.chunksize +1

        print "got info. length:", self.length, ', type:', self.type, ', etag:', self.etag, ', accepts range' if self.ranged else ''
        print "chunksize ", self.chunksize, ", using ", self.chunks, "chunks"


    def request(self, consumer, range_from, range_to):
        print 'got a request from', range_from, range_to

        chunk_first = range_from / self.chunksize
        chunk_last = range_to / self.chunksize
        chunk_offset = range_from % self.chunksize
        chunk_last_length = range_to % self.chunksize

        self.cacheUpdate(chunk_first, chunk_last)

        CachedRequest(self, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length)


    def cacheUpdate(self, chunk_first, chunk_last):

        if self.missing_chunks is not None:
            return

        self.missing_chunks = [ ]

        idx = None
        first_missing = False

        for i in range(chunk_first, chunk_last+1):
            path = self.path + os.path.sep + str(i)

            if not os.access(path, os.F_OK):
                first_missing = True
                if not idx:
                    idx = i
            elif idx:
                self.missing_chunks.append( (idx, i-1) )
                idx = None

        if self.missing_chunks and (True or first_missing):
            cliFac = CacheClientFactory(
                    self,
                    'cupcake:81',
                    '/series/My%20Little%20Pony%3a%20Friendship%20is%20Magic/My%20Little%20Pony%3a%20Friendship%20is%20Magic%20S01E04%20Applebuck%20Season.mkv',
                    self.path,
                    self.missing_chunks[0][0],
                    self.missing_chunks[0][1],
                    self.chunksize
                )
            reactor.connectTCP('cupcake', 81, cliFac)


class CachedRequest(object):
    """
        This class delivers a specific range of a file to a consumer,
        taking care of all intermediate caching (at some point :) )
    """

    def __init__(self, file, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length):
        self.file = file

        self.chunk_first = chunk_first
        self.chunk_last = chunk_last
        self.chunk_offset = chunk_offset
        self.chunk_last_length = chunk_last_length

        # start with first chunk :)
        self.chunk = self.chunk_first

        self.consumer = consumer

        self.sendChunk()

    def sendChunk(self, x = None):

        if self.chunk > self.chunk_last:
            print 'EOF :)'
            self.consumer.loseConnection()
            return

        print "at chunk", self.chunk

        # see if chunk exists
        path = self.path + os.path.sep + str(self.chunk)

        fd = open(self.path + os.path.sep + str(self.chunk), 'rb')

        # possibly seek to added offset within the first chunk
        if self.chunk == self.chunk_first and self.chunk_offset:
            fd.seek(self.chunk_offset)

        self.chunk += 1

        # connect the producer
        self.producer = CacheSender()
        if self.chunk_last_length and (self.chunk-1) == self.chunk_last:
            d = self.producer.beginFileTransfer(fd, self.chunk_last_length, self.consumer)
        else:
            d = self.producer.beginFileTransfer(fd, self.file.chunksize, self.consumer)

        d.addCallback(self.sendChunk)

        def failure(x):
            x.printTraceback()
            self.consumer.loseConnection()
        d.addErrback(failure)

