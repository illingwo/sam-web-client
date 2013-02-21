"""

Exception classes. The specific exception classes like "FileNotFound" are dynamically generated from the server response
This has the consequence that

from samweb_client.exceptions import *

usually won't import these names because they aren't created until the server has been contacted.

Instead you have to do

catch samweb_client.exceptions.UserNotFound as ex:


The base Error, SAMWebConnectionError, SAMWebHTTPError classes are always available
Some more common Exceptions, like FileNotFound are always available
"""

class Error(Exception):
  pass

class NoMoreFiles(Exception):
  pass

class SAMWebConnectionError(Error):
    """ Connection failure """
    pass

class SAMWebSSLError(SAMWebConnectionError):
    """ SSL connection failure """
    pass

class SAMWebHTTPError(Error):
    def __init__(self, method, url, code, msg):
        self.method = method
        self.url = url
        self.code = code
        self.msg = msg

    def __str__(self):
        if 400 <= self.code < 500:
            return self.msg
        else:
            return "HTTP error: %(code)d %(msg)s\nURL: %(url)s" % self.__dict__

class HTTPBadRequest(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 400, msg)

class InvalidMetadata(HTTPBadRequest): pass
class DimensionError(HTTPBadRequest): pass

class HTTPForbidden(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 403, msg)

class HTTPNotFound(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 404, msg)

class FileNotFound(HTTPNotFound): pass
class DefinitionNotFound(HTTPNotFound): pass
class ProjectNotFound(HTTPNotFound): pass

class HTTPConflict(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 409, msg)

def makeHTTPError(method, url, code, msg, exctype=None):
    ''' Make a new error object. If the exctype is given but there is no existing
    class, create a new one to throw and add it to the module namespace '''
    if exctype and not ' ' in exctype:
        exctype = str(exctype)
        exccls = globals().get(exctype)
        if not exccls:
            basecls = _get_exception_class(code)
            if basecls:
                # backwards compatibilty hack is needed here, because in old versions
                # of python Exception is an old style class, and the subclass needs to be created
                # in a different way
                if type(basecls) == type:
                    classgen = type
                else:
                    import new
                    classgen = new.classobj
                exccls = classgen(exctype, (basecls,), dict())
                globals()[exctype] = exccls
    else:
        exccls = _get_exception_class(code)
    if exccls:
        return exccls(method, url, msg)
    else:
        return SAMWebHTTPError(method, url, code, msg)

def _get_exception_class(code):
    if code == 400:
        return HTTPBadRequest
    elif code == 403:
        return HTTPForbidden
    elif code == 404:
        return HTTPNotFound
    elif code == 409:
        return HTTPConflict

