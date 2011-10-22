import urlparse
import hashlib
import os
import sys

from twisted.internet import reactor, defer
from twisted.web.proxy import Proxy, ProxyRequest
from twisted.protocols.basic import FileSender

from zope.interface import implements

from CacheClient import CacheClientFactory
from CachedRequest import CachedRequest, UncachedRequest
import CacheUtils

cachedFiles = { }

def cacheGet(uri, *args, **kwargs):
    # return cachedFile(uri, *args, **kwargs)

    if uri not in cachedFiles:
        cachedFiles[uri] = CachedFile(uri, *args, **kwargs)

    return cachedFiles[uri]

class CachedFile(object):

    ports = { "http" : 80 }

    # already cached chunks
    chunks_cached = [ ]
    # queued chunks
    chunks_queued = [ ]
    # active chunks
    chunks_active = { }

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

        if self.got_info:
            d = defer.Deferred()
            d.callback(self)
            return d

        # no info in cache? request it
        d = CacheUtils.getUriHEAD(self.uri, self.headers)
        d.addCallback(self.handleInfo)

        # and once we have it, fire our deferred
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
                req.d.addCallback(consumer.loseConnection)
                return

        chunk_offset = range_from % self.chunksize
        chunk_last_length = range_to % self.chunksize

        req = CachedRequest(self, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length)
        req.d.addCallback(consumer.loseConnection)

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

    def isQueued(self, chunk):
        return chunk in self.chunks_queued

    def isCached(self, chunk):
        return chunk in self.chunks_cached

    def anticipateChunk(self, chunk, *args, **kwargs):
        if chunk not in self.chunks_cached and chunk not in self.chunks_queued and chunk not in self.chunks_active:
            self.waitForChunk(chunk, *args, **kwargs)

    def waitForChunk(self, chunk, preload = 5, passthrough=True, offset=None):

        # it's already there? this shouldn't happen..
        if chunk in self.chunks_cached:
            print >> sys.stderr, 'WTF: waitForChunk called on cached chunk?'
            d = defer.Deferred()
            d.callback(None)
            return d

        # it's in progress - tell them about it :)
        if chunk in self.chunks_active and passthrough:
            d = defer.Deferred()
            # they might get a passthrough conection from this reference yet
            d.callback(self.chunks_active[chunk])
            return d

        # it's queued - but notify once this one is done
        if chunk in self.chunks_queued:
            # mark this one as waiting (for another chance to get passthrough at a later point)
            if chunk not in self.chunks_waiting:
                self.chunks_waiting[chunk] = [ ]

            d = defer.Deferred()
            self.chunks_waiting[chunk].append(d)
            return d

        # if we got this far, there is no cache and no loading whatsoever on this chunk!
        if not passthrough:
            print 'WTF: passthrough block at a non-queued request...'

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

        # if we have an offset, don't count the first piece as queued
        if not offset:
            self.chunks_queued += list(range(start,end+1))
        else:
            self.chunks_queued += list(range(start+1,end+1))

        # initiate wait for chunk
        cliFac = CacheClientFactory(
                self,
                self.host + ( (':'+str(self.port)) if self.port != 80 else '' ),
                self.rest,
                self.path,
                start,
                end,
                offset
            )
        reactor.connectTCP(self.host, self.port, cliFac)

        # mark this one as waiting
        if chunk not in self.chunks_waiting:
            self.chunks_waiting[chunk] = [ ]

        # and notify once it's done :)
        d = defer.Deferred()
        self.chunks_waiting[chunk].append(d)
        return d

    def handleActiveChunk(self, chunk, producer):
        print 'active chunk: ', chunk

        if chunk in self.chunks_active:
            print 'WTF: got a second producer for a chunk?!'

        self.chunks_active[chunk] = producer

        # do we have any waiting for this particular chunk?
        if chunk in self.chunks_waiting and self.chunks_waiting[chunk]:
            for w in self.chunks_waiting[chunk]:
                print 'notifying a waiting, active'
                w.callback(producer)
            del self.chunks_waiting[chunk]

    def handleGotChunk(self, chunk, partial = False):

        if chunk in self.chunks_queued:
            del self.chunks_queued[self.chunks_queued.index(chunk)]
        elif not partial:
            print 'debug: got unqueued chunk handle.. should this happen?'

        if chunk in self.chunks_active:
            del self.chunks_active[chunk]
        else:
            print 'debug: got unactive chunk handle.. should this happen?'

        if partial:
            if chunk in self.chunks_waiting:
                print 'debug: got a partial for a chunk still being waited for?'

            print 'finished caching chunk partially: ', chunk
            return

        print 'finished caching chunk: ', chunk
        self.chunks_cached.append(chunk)

        if chunk in self.chunks_waiting:
            for w in self.chunks_waiting[chunk]:
                w.callback(None)
            del self.chunks_waiting[chunk]
