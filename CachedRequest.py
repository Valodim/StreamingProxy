import os

from twisted.internet import defer, interfaces, reactor
from zope.interface import implements

from CacheSender import CacheSender
from UncachedClient import UncachedClientFactory
import CacheSettings

class CachedRequest(object):
    """
        This class delivers a specific range of a file to a consumer,
        taking care of all intermediate caching (at some point :) )

        It will mostly work with its associated CachedFile to spawn
        CachedClient, UncachedClient and CacheSender instances to get
        data.

        Data may be passed to the consumer either by delegating producers, ie.
        using a CacheSender. It may also be passed through the CachedRequest
        itself, acting as a streaming PushProducer.

    """

    implements(interfaces.IPushProducer)

    direct_chunk = None
    direct_pause = False

    direct_handled = 0

    def __init__(self, file, consumer, chunk_first, chunk_last, chunk_offset, chunk_last_length):
        self.file = file

        self.chunk_first = chunk_first
        self.chunk_last = chunk_last
        self.chunk_offset = chunk_offset
        self.chunk_last_length = chunk_last_length

        # start with first chunk :)
        self.chunk = self.chunk_first

        self.consumer = consumer

        # at the first chunk, we can already mark the offset as handled
        self.direct_handled = chunk_offset

        self.d = defer.Deferred()

        self.sendChunk()

    def sendChunk(self, producer = None):
        """
            This method runs through a number of ways to retrieve data, either
            from a deferred (ie, finished CacheClient), or as an internal
            loopback if new information came up. It also handles chunk
            transition, and finishes the request when it is finished.

            Attempts to get data, in order of execution:
             - cached file
             - passthrough from passed producer
             - queue with CachedFile.waitForChunk()

            When reading from cache, a call to anticipateChunk will be
            made to enable preloading of at least three chunks.
        """

        if self.chunk > self.chunk_last:
            self.d.callback(self)
            return

        # are we retrieving directly right now?
        if self.direct_chunk is not None:
            return

        # see if the requested chunk exists
        if not self.file.isCached(self.chunk):

            # being offered a producer?
            if producer is not None:
                # not right now... maybe later? just try again some time.
                if self.direct_pause:
                    reactor.callLater(2, self.sendChunk)
                    return

                producer.registerConsumer(self)
                return

            print "waiting for chunk", self.chunk
            # is the offset more than 20% of a chunksize?
            if self.chunk == self.chunk_first and self.direct_handled > CacheSettings.uncachedOffset:
                # work with a partial (uncached!) chunk, then.  if we don't do
                # this, we get a delay of (chunksize-offset)/transferspeed
                # before we get any useful data!
                d = self.file.waitForChunk(self.chunk, offset=self.direct_handled)
            else:
                # passthrough block is off for now, not sure if that is needed anymore
                d = self.file.waitForChunk(self.chunk) #, passthrough=(not self.direct_pause) )
            d.addCallback(self.sendChunk)
            return

        # anticipate the next three chunks
        if self.chunk+3 < self.chunk_last:
            self.file.anticipateChunk(self.chunk+3)
        else:
            # or possibly just anticipate until our last chunk
            self.file.anticipateChunk(self.chunk_last)

        fd = open(self.file.path + os.path.sep + str(self.chunk), 'rb')

        print "sending from cache", self.chunk, 'offset ', self.direct_handled

        # possibly seek to added offset within the first chunk
        if self.direct_handled > 0:
            fd.seek(self.direct_handled)

        # advance chunk by one, reset runtime vars
        self.chunk += 1
        self.direct_handled = 0
        self.direct_pause = False

        # connect the producer
        self.producer = CacheSender()
        if self.chunk_last_length and (self.chunk-1) == self.chunk_last:
            d = self.producer.beginFileTransfer(fd, self.chunk_last_length, self.consumer)
        else:
            d = self.producer.beginFileTransfer(fd, self.file.chunksize, self.consumer)

        d.addCallback(self.sendChunk)
        d.addErrback(self.stopProducing)

    def handleDirectChunk(self, chunk, offset = None):
        """
            Called to register a producer of direct data. The returning
            boolean value indicates whether this was accepted or not,
            and data must only be sent on True.

            Reasons for rejection are wrong chunk, if the file has been cached
            in the meantime, or if the provided offset is bigger than requred.
        """

        if self.chunk != chunk:
            print 'WTF: turning down direct stream, wrong chunk?!'
            return False

        if self.direct_pause:
            print 'turning down direct stream, pause indicates we can wait'
            self.sendChunk()
            return False

        if self.chunk > self.chunk_last:
            print 'turning down direct stream, we are past our chunk'
            self.sendChunk()
            return False

        # if this is the correct chunk, and is not yet available from cache
        if self.file.isCached(chunk):
            print 'turning down direct stream, it\'s already cached (should this happen?)'
            return False

        # is the offset ok for us?
        if offset and self.chunk == self.chunk_first and offset < self.chunk_offset:
            print 'turning down direct stream, no good offset'
            self.sendChunk()
            return False

        print 'starting direct passthrough:', chunk

        # notify that we'll be producin'
        self.consumer.registerProducer(self, True)

        self.direct_chunk = chunk
        return True

    def handleDirectChunkEnd(self, chunk):
        """
            This is called by a producer who registered before with
            handleDirectChunk and didn't get rejected, to indicate the end of
            its passthrough data.

            This may happen in the middle of a chunk, which will then be asked
            for in sendChunk as usual. If the file is cached at that point,
            this will work as a smooth transition.
        """

        if self.direct_chunk is None:
            return

        print 'finished direct passthrough: ', chunk, ', handled', self.direct_handled, 'bytes'

        if chunk != self.chunk:
            print 'WTF: wrong end of direct chunk?!', chunk, self.direct_chunk

        # we no longer produce ourselves
        self.direct_chunk = None
        self.consumer.unregisterProducer()

        # did we get the entire chunk? if so, mark it as done
        if self.direct_handled == self.file.chunksize:
            self.direct_handled = 0
            self.chunk += 1

        # and continue looking for further data, should we need it
        self.sendChunk()

    def handleDirectChunkData(self, data, limit):
        """
            This method expects a File handle (most likely StringIO) to work
            with. It will determine whether or not to send data depending on
            its paused option, and handle offsets accordingly.

            The file handle is expected to handle seek().

            The limit parameter indicates up to what point the buffer is filled,
            and will be reset to if any seek()ing is done.
        """

        if self.direct_chunk is None:
            # this is a TODO, and depends in the handling of aborted requests!
            # print 'WTF: unrequested direct chunk data?!'
            return

        # if we are paused (maybe add an additional condition for this?)
        # note that we cannot turn this down if we are in an offset chunk,
        # because the data is not going to be cached!
        if not (self.chunk == self.chunk_first and self.chunk_offset) and self.direct_pause:
            # we are not supposed to send any data right now, so skip this one

            # this means that the data needs to be sent at a later point, which
            # will happen either in this method if resumeProducing() is called
            # in the meantime, or after we got a handleDirectChunkEnd() and we
            # read from the file, using direct_handled as an added offset.

            return

        # TODO, debug check, gonna this later!
        if self.chunk == self.chunk_first and self.chunk_offset and self.direct_handled < self.chunk_offset:
            print 'WTF: direct_handled < chunk_offset?'
            self.direct_handled = self.chunk_offset

            return

        # any new data?
        if limit > self.direct_handled:
            # seek to the new data offset
            data.seek(self.direct_handled)
            # read it
            buf = data.read()
            # forward it
            self.consumer.write(buf)
            # and take note of the handled range
            self.direct_handled += len(buf)


    def pauseProducing(self):
        """
            We are a streaming producer, so we can send data whenever we want.
            However, getting a pauseProducing hint means we are ahead of
            schedule, so we set a switch which will slow down or even decline
            direct passthrough at a later point (in favor of cached file data)
        """
        if not self.direct_pause:
            print 'pause request?'
            self.direct_pause = True
            # reactor.callLater(5, self.resumeProducing)


    def resumeProducing(self):
        """
            On the other hand, if we are asked to resume, we might as well keep
            going with the passthrough :)
        """
        if self.direct_pause:
            print 'resume request?'
            self.direct_pause = False
            # self.sendChunk()


    def stopProducing(self, err = None):
        print 'we were asked to stop producing.. very well'
        self.direct_chunk = None


class UncachedRequest(CachedRequest):
    """
        An uncached request is very much like a cached one and implements the
        same interfaces. It does however drop the entire caching layer, simply
        streaming from the server.

        This is mostly useful for small pieces of data that do not require
        caching (such as index lookups)

        TODO: Does this need to inherit CachedRequest?
    """

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
        self.d.callback(None)
