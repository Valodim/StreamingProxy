import os

from twisted.internet import defer, interfaces
from zope.interface import implements

from CacheSender import CacheSender

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

        self.d = defer.Deferred()

        self.sendChunk()

    def sendChunk(self, x = None):

        if self.chunk > self.chunk_last:
            self.consumer.loseConnection()
            self.d.callback(self)
            return

        # are we retrieving directly right now?
        if self.direct_chunk is not None:
            return

        # this is the file this particular chunk work with
        path = self.file.path + os.path.sep + str(self.chunk)

        # see if chunk exists
        if not os.path.isfile(path):
            print "waiting for chunk", self.chunk
            d = self.file.waitForChunk(self.chunk, direct=self)
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

    def handleDirectChunk(self, chunk):
        # debug: don't passthrough chunk 0!
        # if chunk == 0:
            # return False

        if self.chunk > self.chunk_last:
            self.sendChunk()
            return False

        # if this is the correct chunk, and is not yet available
        if self.chunk == chunk and not self.file.isCached(chunk):
            print 'starting direct passthrough:', chunk

            # notify that we'll be producin'
            self.consumer.registerProducer(self, True)

            self.direct_chunk = chunk
            self.direct_sent = 0
            self.direct_skipped = 0
            return True
        else:
            return False

    def handleDirectChunkEnd(self, chunk):
        if self.direct_chunk is None:
            return

        print 'finished direct passthrough: ', chunk, '- skipped', self.direct_skipped, 'bytes, sent', self.direct_sent, 'bytes'

        if chunk != self.chunk:
            print 'WTF: wrong end of direct chunk?!', chunk, self.direct_chunk

        self.direct_chunk = None
        self.chunk += 1
        self.consumer.unregisterProducer()

        self.sendChunk()

    def handleDirectChunkData(self, data):
        if self.direct_chunk is None:
            print 'WTF: unrequested direct chunk data?!'
            return

        # we might have to throw some of it away, if an offset is requested?
        if self.chunk == self.chunk_first and self.chunk_offset:
            # if the offset is smaller than what has been processed, just send the data
            if self.chunk_offset < self.direct_skipped:
                self.consumer.write(data)
                self.direct_sent += len(data)
            # if the offset is in the current batch of data, send the partial buffer
            elif self.chunk_offset < self.direct_skipped +len(data):
                self.consumer.write(data[self.chunk_offset-self.direct_skipped:])
                self.direct_sent += len(data)-(self.chunk_offset-self.direct_skipped)
            # the else case is just throwing away that data :)
            self.direct_skipped += len(data)
            return

        # no offsets, just send the data :)
        self.direct_sent += len(data)
        self.consumer.write(data)

    def stopProducing(self):
        self.direct_chunk = None

    def pauseProducing(self):
        pass
    def resumeProducing(self):
        pass
