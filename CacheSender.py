from twisted.protocols.basic import FileSender

class CacheSender(FileSender):

    def beginFileTransfer(self, fd, length, *args, **kwargs):
        self.length = length
        return FileSender.beginFileTransfer(self, fd, *args, **kwargs)

    def resumeProducing(self):
        chunk = ''

        if self.file and self.length > 0:
            if self.length < self.CHUNK_SIZE:
                readsize = self.length
                self.length = 0
            else:
                readsize = self.CHUNK_SIZE
                self.length -= self.CHUNK_SIZE
            chunk = self.file.read(readsize)

        if not chunk:
            self.file.close()
            self.file = None
            self.consumer.unregisterProducer()
            if self.deferred:
                self.deferred.callback(None)
                self.deferred = None
            return

        self.consumer.write(chunk)

        # if we are at the end of the file, close the stream :)
        if self.length == 0:
            self.resumeProducing()

