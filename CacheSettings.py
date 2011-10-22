import os

# where files are normally cached. adjust to your own needs.
cachePath = os.path.abspath('.')

# adjust chunk size
chunkSize = 8*1024*1024

# if a request is more than this many bytes off a chunk limit, do not cache.
# this is a tradeoff setting between cache and request delay
# (chunksize-offset)/transferspeed if offset < uncachedOffset else 0
uncachedOffset = 0.2 * chunkSize

# the maximum length of a file that is not cached at all
uncachedLength = 128*1024
