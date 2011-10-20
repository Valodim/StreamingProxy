import urlparse
import hashlib
import os

from twisted.internet import reactor, defer
from twisted.web.proxy import Proxy, ProxyRequest
from twisted.protocols.basic import FileSender

from zope.interface import implements

from CacheClient import CacheClientFactory
from CachedRequest import CachedRequest, UncachedRequest
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
        if 'etag' in headers:
            self.etag = headers['etag'][0]

        self.ranged = 'accept-ranges' in headers and headers['accept-ranges'][0] == 'bytes'

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
        chunk_first = range_from / self.chunksize
        chunk_last = range_to / self.chunksize

        # if this is a small request (< 128kb), don't do any caching
        if range_to-range_from < 128*1024:
            # also, if it is not completely cached
            if not (chunk_first in self.chunks_cached and chunk_last in self.chunks_cached):
                req = UncachedRequest(self, consumer, range_from, range_to)
                return

        chunk_offset = range_from % self.chunksize
        chunk_last_length = range_to % self.chunksize

        req = CachedRequest(self, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length)
        # req.d.addCallback()

    def cacheUpdate(self):

        for i in range(0, self.chunks+1):
            # got it?
            if i in self.chunks_cached or i in self.chunks_queued:
                continue

            path = self.path + os.path.sep + str(i)
            if os.access(path, os.F_OK):
                stats = os.stat(path)
                if stats.st_size != self.chunksize and not (i == self.chunks and stats.st_size == (self.length % self.chunksize) ):
                    print 'found file with bad size - deleting chunk', i
                    os.unlink(path)
                else:
                    self.chunks_cached.append( i )

    def isCached(self, chunk):
        return chunk in self.chunks_cached

    def anticipateChunk(self, chunk, *args, **kwargs):
        if chunk not in self.chunks_cached and chunk not in self.chunks_queued:
            self.waitForChunk(chunk, *args, **kwargs)

    def waitForChunk(self, chunk, preload = 5, direct = None):

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

        self.chunks_queued += list(range(start,end+1))

        # initiate wait for chunk
        cliFac = CacheClientFactory(
                self,
                self.host + ( (':'+str(self.port)) if self.port != 80 else '' ),
                self.rest,
                self.path,
                start,
                end,
                direct
            )
        reactor.connectTCP(self.host, self.port, cliFac)

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
