import urlparse
import hashlib

from twisted.internet import reactor, defer
from twisted.web.proxy import Proxy, ProxyRequest

from cachedChunk import cachedChunk
import utils

class cachedFile(object):
    ports = {"http" : 80}

    requests = [ ]

    active_chunks = { }

    got_info = False

    def __init__(self, uri, headers):
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

            # in kb, so this is 10MB
            self.chunksize = 10*1024
            self.chunks = self.length / self.chunksize +1

            print "got info. length:", self.length, ', type:', self.type, ', etag:', self.etag
            print "chunksize ", self.chunksize, ", using ", self.chunks, "chunks"

        self.d.addCallback(info)

    def getInfo(self):
        d = defer.Deferred()
        def doGetSize(x):
            d.callback({
                'content-length': self.length,
                'content-type': self.type,
                'etag': self.etag,
            })
        self.d.addCallback(doGetSize)

        return d

    def request(self, me, range_from, range_to):
        """
        What this is supposed to do:
         - find out length of file, possibly get ETag
         - determine reasonable chunk size
         - add first requested chunk to request queue
        """

        # do we need to wait for this?
        if not self.got_info:
            def doReq(x):
                self.request(me, range_from, range_to)
            self.d.addCallback(doReq)
            return

        chunk_first = (range_from / self.chunksize) if range_from is not None else 0
        chunk_last = (range_to / self.chunksize) if range_to is not None else self.chunks

        print "requesting: ", chunk_first, "-", chunk_last

        if chunk_first == chunk_last:
            return

        if chunk_first not in self.active_chunks:
            self.active_chunks[chunk_first] = cachedChunk(self, chunk_first)

        self.active_chunks[chunk_first].read(me, (range_from % self.chunksize) if range_from is not None else None)

    def finish(self):
        print "finish."
        self.me.finish()

    def stopReading(self):
        return

        if self.nection is not None:
            self.nection.disconnect()
