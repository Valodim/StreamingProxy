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

    ports = { "http" : 80 }

    # already cached chunks
    chunks_cached = [ ]
    # queued chunks
    chunks_queued = [ ]

    chunks_waiting = { }

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
        self.chunks = self.length / self.chunksize

        print "got info. length:", self.length, ', type:', self.type, ', etag:', self.etag, ', accepts range' if self.ranged else ''
        print "chunksize ", self.chunksize, ", using ", self.chunks, "chunks"

        self.cacheUpdate()

        # fetch first and last chunk by default
        # self.waitForChunk(0, doPreload=False)
        # self.waitForChunk(self.chunks, doPreload=False)

    def request(self, consumer, range_from, range_to):
        print 'got a request from', range_from, range_to

        chunk_first = range_from / self.chunksize
        chunk_last = range_to / self.chunksize
        chunk_offset = range_from % self.chunksize
        chunk_last_length = range_to % self.chunksize

        req = CachedRequest(self, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length)
        # req.d.addCallback()

    def cacheUpdate(self):

        for i in range(0, self.chunks+1):
            # got it?
            if i in self.chunks_cached:
                continue

            path = self.path + os.path.sep + str(i)
            if os.access(path, os.F_OK):
                self.chunks_cached.append( i )

    def anticipateChunk(self, chunk, *args, **kwargs):
        if chunk not in self.chunks_cached and chunk not in self.chunks_queued:
            self.waitForChunk(chunk, *args, **kwargs)

    def waitForChunk(self, chunk, preload = 5):

        self.cacheUpdate()

        if chunk in self.chunks_cached:
            d = defer.Deferred()
            d.callback(chunk)
            return d

        if chunk in self.chunks_queued:
            # mark this one as waiting
            if chunk not in self.chunks_waiting:
                self.chunks_waiting[chunk] = [ ]

            d = defer.Deferred()
            self.chunks_waiting[chunk].append(d)
            return d


        # find all missing, starting from requested
        start = chunk
        # preload more chunks (max. 5)
        if preload:
            end = min(start+preload, self.chunks)
            for i in range(start+1, min(start+preload, self.chunks)+1):
                if i in self.chunks_cached or i in self.chunks_queued:
                    end = i-1
                    break
        else:
            end = start

        self.chunks_queued += list(range(start,end))

        # initiate wait for chunk
        cliFac = CacheClientFactory(
                self,
                'cupcake:81',
                '/series/My%20Little%20Pony%3a%20Friendship%20is%20Magic/My%20Little%20Pony%3a%20Friendship%20is%20Magic%20S01E04%20Applebuck%20Season.mkv',
                self.path,
                start,
                end,
                self.chunksize
            )
        reactor.connectTCP('cupcake', 81, cliFac)

        # mark this one as waiting
        if chunk not in self.chunks_waiting:
            self.chunks_waiting[chunk] = [ ]

        # and notify once it's done :)
        d = defer.Deferred()
        self.chunks_waiting[chunk].append(d)
        return d

    def handleGotChunk(self, chunk):
        print 'got chunk: ', chunk
        self.chunks_cached.append(chunk)
        if chunk in self.chunks_queued:
            del self.chunks_queued[self.chunks_queued.index(chunk)]
        else:
            print 'debug: got unqueued chunk handle.. should this happen?'
        if chunk in self.chunks_waiting:
            for w in self.chunks_waiting[chunk]:
                w.callback(chunk)
            del self.chunks_waiting[chunk]

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

        self.d = defer.Deferred()

        self.sendChunk()

    def sendChunk(self, x = None):

        if self.chunk > self.chunk_last:
            self.consumer.loseConnection()
            self.d.callback(self)
            return

        # this is the file this particular chunk work with
        path = self.file.path + os.path.sep + str(self.chunk)

        # see if chunk exists
        if not os.path.isfile(path):
            print "waiting for chunk", self.chunk
            d = self.file.waitForChunk(self.chunk)
            d.addCallback(self.sendChunk)
            return

        print "at chunk", self.chunk

        # anticipate the next three chunks
        if self.chunk+3 < self.chunk_last:
            self.file.anticipateChunk(self.chunk+3)
        else:
            # or possibly just anticipate until our last chunk
            self.file.anticipateChunk(self.chunk_last)

        fd = open(self.file.path + os.path.sep + str(self.chunk), 'rb')

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
            self.d.errback(x)
        d.addErrback(failure)

