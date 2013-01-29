
from exceptions import *

# Calculate the enstore style CRC for a file
# Raises a standard python IOError in case of failures
# Uses the adler32 algorithm from zlib, except with an initial
# value of 0, instead of 1, and adler32 returns a signed int (ie 32 bits)
# while we want an unsigned value
def fileEnstoreChecksum(path):
    """Calculate enstore compatible CRC value"""
    try:
        f =open(path,'rb')
    except (IOError, OSError), ex:
        raise Error(str(ex))
    try:
        return enstoreChecksum(f)
    finally:
        f.close()

def enstoreChecksum(fileobj):
    import zlib
    readblocksize = 1024*1024
    crc = 0
    while 1:
        try:
            s = fileobj.read(readblocksize)
        except (OSError, IOError), ex:
            raise Error(str(ex))
        if not s: break
        crc = zlib.adler32(s,crc)
    crc = long(crc)
    if crc < 0:
        # Return 32 bit unsigned value
        crc  = (crc & 0x7FFFFFFFL) | 0x80000000L
    return { "crc_value" : str(crc), "crc_type" : "adler 32 crc type" }

