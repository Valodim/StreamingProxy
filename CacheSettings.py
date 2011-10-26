import os

# where files are normally cached. adjust to your own needs.
cachePath = os.path.abspath('cache/') + os.path.sep

# adjust chunk size. the passed parameter is the length of the file
# chunkSize = lambda x: 6*1024*1024
# this splits up in 100 chunks, but max chunksize is 6MiB, min is 500KiB
chunkSize = lambda x: max(512*1024, min(6*1024*1024, x / 100))

# if a request is more than this many bytes off a chunk limit, do not cache.
# this is a tradeoff setting between cache and request delay
# (chunksize-offset)/transferspeed if offset < uncachedOffset else 0
# for me, 512KiB is a good setting, because that takes about half a second
# to load
uncachedOffset = 512*1024

# the maximum length of a file that is not cached at all
uncachedLength = 1024*1024
