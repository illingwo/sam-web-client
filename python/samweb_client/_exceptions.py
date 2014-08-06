""" The exception classes live here, along with a class that manages the on-demand generation of new classes """

import threading

class Error(Exception):
  pass

class PlaceHolderError(Error):
    """ Placeholder for generated exceptions where we don't know the real type """
    def __str__(self):
        return 'Placeholder exception for %s' % self.__class__.__name__

class ArgumentError(Error):
    """ For methods called with incorrect arguments """
    pass

class NoMoreFiles(Exception):
  pass

class ArgumentError(Error):
    """ For methods called with incorrect arguments """

class Timeout(Exception): pass

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

class _Exceptions(object):

    # This class is substituted for the exceptions module
    # it looks up exceptions in this module, and if it doesn't
    # find them, creates a placeholder class that can be used in
    # an except clause
    # When an exception is thrown it will be created if it doesn't
    # exist, or if it's a placeholder then the base class is mutated
    # to the correct one for the type that was received from the server
    
    __all__ = ['makeHTTPError']

    def __init__(self):
        self._dynamic_exceptions = {}
        self.lock = threading.Lock()

    def __getattr__(self, attr):
        if attr.startswith('_'): 
            # no underscores
            raise AttributeError()
        self.lock.acquire()
        try:
            try:
                return self._dynamic_exceptions[attr]
            except KeyError:
                o = self._makeClass(attr, PlaceHolderError)
                self._dynamic_exceptions[attr] = o
            return o
        finally:
            self.lock.release()

    def _makeClass(self, exctype, basecls):
        # backwards compatibilty hack is needed here, because in old versions
        # of python Exception is an old style class, and the subclass needs to be created
        # in a different way
        if type(basecls) == type:
            classgen = type
        else:
            import new
            classgen = new.classobj
        return classgen(exctype, (basecls,), dict())

    def makeHTTPError(self, method, url, code, msg, exctype=None):
        ''' Make a new error object. If the exctype is given but there is no existing
        class, create a new one to throw and add it to the module namespace '''
        self.lock.acquire()
        try:
            return self._makeHTTPError(method, url, code, msg, exctype)
        finally:
            self.lock.release()

    def _makeHTTPError(self, method, url, code, msg, exctype=None):
        if exctype and not ' ' in exctype:
            exctype = str(exctype)
            exccls = globals().get(exctype) or self._dynamic_exceptions.get(exctype)
            if not exccls:
                basecls = self._get_exception_class(code)
                if basecls:
                    exccls = self._makeClass(exctype, basecls)
                    self._dynamic_exceptions[exctype] = exccls
            else:
                # the exception already exists - check if it's a placeholder
                if issubclass(exccls, self.PlaceHolderError):
                    # mutate the base class
                    newbase = self._get_exception_class(code)
                    exccls.__bases__ = (newbase, )
        else:
            exccls = self._get_exception_class(code)
        if exccls:
            o = exccls(method, url, msg)
            return o
        else:
            return self.SAMWebHTTPError(method, url, code, msg)

    def _get_exception_class(self, code):
        if code == 400:
            return self.HTTPBadRequest
        elif code == 403:
            return self.HTTPForbidden
        elif code == 404:
            return self.HTTPNotFound
        elif code == 409:
            return self.HTTPConflict
        else:
            return self.SAMWebHTTPError

# Add the exception classes the _Exceptions.__all__ so that "from exceptions import *" will work
import types
for k,v in globals().items():
    if isinstance(v, (type, types.ClassType)) and issubclass(v, Error):
        setattr(_Exceptions, k, v)
        _Exceptions.__all__.append(k)
# These don't inherit from Error
_Exceptions.NoMoreFiles = NoMoreFiles
_Exceptions.Timeout = Timeout
_Exceptions.__all__.extend( ('NoMoreFiles','Timeout') )

