
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
    def __init__(self, method, url, code, msg, exctype):
        self.method = method
        self.url = url
        self.code = code
        self.msg = msg
        self.type = exctype # The type name of the exception, if provided

    def __str__(self):
        if 400 <= self.code < 500:
            return self.msg
        else:
            return "HTTP error: %(code)d %(msg)s\nURL: %(url)s" % self.__dict__

class HTTPNotFound(SAMWebHTTPError):
    def __init__(self, method, url, msg, exctype):
        SAMWebHTTPError.__init__(self, method, url, 404, msg, exctype)

class FileNotFound(HTTPNotFound):
    pass

class HTTPForbidden(SAMWebHTTPError):
    def __init__(self, method, url, msg, exctype):
        SAMWebHTTPError.__init__(self, method, url, 403, msg, exctype)

class HTTPConflict(SAMWebHTTPError):
    def __init__(self, method, url, msg, exctype):
        SAMWebHTTPError.__init__(self, method, url, 409, msg, exctype)

def makeHTTPError(method, url, code, msg, exctype=''):
    # This function exists so we can return specific classes for different error types, if we want
    # for now, just return the generic class
    if code == 404:
        if exctype == 'FileNotFound':
            return FileNotFound(method, url, msg, exctype)
        else:
            return HTTPNotFound(method, url, msg, exctype)
    elif code == 403:
        return HTTPForbidden(method, url, msg, exctype)
    elif code == 409:
        return HTTPConflict(method, url, msg, exctype)
    else:
        return SAMWebHTTPError(method, url, code, msg, exctype)

