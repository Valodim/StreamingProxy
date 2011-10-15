from StringIO import StringIO

from twisted.internet import reactor, defer

from CacheClient import CacheClientFactory

class cachedChunk(object):
    """ """

    def __init__(self, file, chunkno):
        self.file = file
        self.chunkno = chunkno

        self.buffer = StringIO()

    def cached(self):
        return os.file(self.file.path + os.path.sep + self.offset).exists()

    def read(self, me, offset = 0, length = 0):
        """ what this is supposed to do:
         - find if this chunk is buffered
             - if it is, just return the bytes
         - find if this chunk is cached
             - if it is, just read file into buffer
             - if it's not, fetch to buffer and file simultaneously
        """

        self.me = me
        clientFactory = CacheClientFactory(self.file.rest, self.file.headers, self)
        self.nection = reactor.connectTCP(self.file.host, self.file.port, clientFactory)

        pass

    def write(self, buffer):
        self.me.transport.write(buffer)

    def eof(self):
        print "EOF"
