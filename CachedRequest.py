import os

from twisted.internet import defer, interfaces, reactor
from zope.interface import implements

from CacheSender import CacheSender
from UncachedClient import UncachedClientFactory

class CachedRequest(object):
    """
        This class delivers a specific range of a file to a consumer,
        taking care of all intermediate caching (at some point :) )
    """

    implements(interfaces.IPushProducer)

    def __init__(self, file, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length):
        self.file = file

        self.chunk_first = chunk_first
        self.chunk_last = chunk_last
        self.chunk_offset = chunk_offset
        self.chunk_last_length = chunk_last_length

        # start with first chunk :)
        self.chunk = self.chunk_first

        self.consumer = consumer

        self.direct_chunk = None
        self.direct_sent = 0
        self.direct_pause = False

        self.d = defer.Deferred()

        self.sendChunk()

    def sendChunk(self, producer = None):

        print 'sendchunk', self

        if self.chunk > self.chunk_last:
            self.consumer.loseConnection()
            self.d.callback(self)
            return

        # are we retrieving directly right now?
        if self.direct_chunk is not None:
            return

        # being offered a producer?
        if producer is not None:
            producer.registerConsumer(self)
            return

        # see if the requested chunk exists
        if not self.file.isCached(self.chunk):
            print "waiting for chunk", self.chunk
            d = self.file.waitForChunk(self.chunk)
            d.addCallback(self.sendChunk)
            return

        # anticipate the next three chunks
        if self.chunk+3 < self.chunk_last:
            self.file.anticipateChunk(self.chunk+3)
        else:
            # or possibly just anticipate until our last chunk
            self.file.anticipateChunk(self.chunk_last)

        fd = open(self.file.path + os.path.sep + str(self.chunk), 'rb')

        print "sending from cache", self.chunk

        # possibly seek to added offset within the first chunk
        if self.chunk == self.chunk_first and self.chunk_offset:
            fd.seek(self.chunk_offset)

        self.chunk += 1
        # at this point, if it happens to be useful, we may direct connect again
        self.direct_pause = False

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

    def handleDirectChunk(self, chunk):
        # debug: don't passthrough chunk 0!
        # if chunk == 0:
            # return False

        if self.chunk != chunk:
            print 'WTF: turning down direct stream, wrong chunk?!'
            return False

        if self.direct_pause:
            print 'turning down direct stream, pause indicates we can wait'
            return False

        if self.chunk > self.chunk_last:
            print 'turning down direct stream, we are past our chunk'
            self.sendChunk()
            return False

        # if this is the correct chunk, and is not yet available from cache
        if self.file.isCached(chunk):
            print 'turning down direct stream, it\'s already cached (should this happen?)'
            return False

        print 'starting direct passthrough:', chunk

        # notify that we'll be producin'
        self.consumer.registerProducer(self, True)

        self.direct_chunk = chunk
        self.direct_sent = 0
        return True

    def handleDirectChunkEnd(self, chunk):
        if self.direct_chunk is None:
            return

        print 'finished direct passthrough: ', chunk, ', sent', self.direct_sent, 'bytes'

        if chunk != self.chunk:
            print 'WTF: wrong end of direct chunk?!', chunk, self.direct_chunk

        self.direct_chunk = None
        self.chunk += 1
        self.consumer.unregisterProducer()

        self.sendChunk()

    def handleDirectChunkData(self, data, offset):
        if self.direct_chunk is None:
            print 'WTF: unrequested direct chunk data?!'
            return

        # print 'got direct from offset', offset

        # we might have to throw some of it away, if an offset is requested?
        if self.chunk == self.chunk_first and self.chunk_offset:
            # if the offset is smaller than what has been processed, just send the data
            if self.chunk_offset < offset:
                self.consumer.write(data)
                self.direct_sent += len(data)
            # if the offset is in the current batch of data, send the partial buffer
            elif self.chunk_offset < offset +len(data):
                self.consumer.write(data[self.chunk_offset-offset:])
                self.direct_sent += len(data)-(self.chunk_offset-offset)
            return

        # no offsets, just send the data :)
        self.consumer.write(data)
        self.direct_sent += len(data)

    def stopProducing(self):
        print 'we were asked to stop producing.. very well'
        self.direct_chunk = None

    def pauseProducing(self):
        """
            We are a streaming producer, so we can send data whenever we want.
            However, getting a pauseProducing hint means we are ahead of
            schedule, so we set a switch which will decline direct passthrough
            at a later point (in favor of cached file data)
        """
        self.direct_pause = True
        pass

    def resumeProducing(self):
        """
            On the other hand, if we are asked to resume, we might as well keep
            going with the passthrough :)
        """
        print 'resume request?'
        self.direct_pause = False
        pass

class UncachedRequest(CachedRequest):

    def __init__(self, file, consumer, range_from, range_to):

        self.file = file

        self.range_from = range_from
        self.range_to = range_to

        self.consumer = consumer

        self.d = defer.Deferred()

        self.sendChunk()

    def sendChunk(self, x = None):
        print "initiating uncached request,", self.range_from, '-', self.range_to

        # initiate wait for chunk
        cliFac = UncachedClientFactory(
                self.file.host + ( (':'+str(self.file.port)) if self.file.port != 80 else '' ),
                self.file.rest,
                self.range_from,
                self.range_to,
                self
            )
        reactor.connectTCP(self.file.host, self.file.port, cliFac)

    def handleDirectChunk(self, chunk):
        self.consumer.registerProducer(self, True)

    def handleDirectChunkData(self, data, offset):
        self.consumer.write(data)

    def handleDirectChunkEnd(self, chunk):
        self.consumer.unregisterProducer()
        self.consumer.loseConnection()
        # self.d.callback(None)
