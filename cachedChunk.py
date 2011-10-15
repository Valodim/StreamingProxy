from StringIO import StringIO

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

        # clientFactory = InterceptingProxyClientFactory('GET', self.rest, 'HTTP/1.0', self.headers, { }, self)
        # self.nection = reactor.connectTCP(self.host, self.port, clientFactory)

        pass

