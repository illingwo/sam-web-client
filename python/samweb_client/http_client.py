
from samweb_client.exceptions import Error

class SAMWebConnectionError(Error):
    pass

class SAMWebSSLError(SAMWebConnectionError):
    pass

def makeHTTPError(method, url, code, msg, exctype=''):
    # This function exists so we can return specific classes for different error types, if we want
    # for now, just return the generic class
    return SAMWebHTTPError(method, url, code, msg, exctype)

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

class SAMWebHTTPClient(object):
    maxtimeout=60*30 # default max timeout
    maxretryinterval = 60 # default max retry interval
    verboseretries = True # whether to print output when retrying

    def __init__(self, maxtimeout=None, maxretryinterval=None, verboseretries=None, *args, **kwargs):
        if maxtimeout is not None:
            self.maxtimeout = maxtimeout
        if maxretryinterval is not None:
            self.maxretryinterval = maxretryinterval
        if verboseretries is not None:
            self.verboseretries = verboseretries

    def make_ssl_error(self, msg):
        """ Try to make sense of ssl errors and return a suitable exception object """
        if 'error:14094410' in msg:
            if self._cert:
                errmsg = "SSL error: %s: is client certificate valid?" % msg
            else:
                errmsg = "SSL error: %s: no client certificate has been installed" % msg
        else:
            errmsg = "SSL error: %s" % msg
        return SAMWebSSLError(errmsg)

    def get_standard_certificate_path(self):
        import os
        cert = os.environ.get('X509_USER_PROXY')
        if not cert:
            # look in standard place for cert
            proxypath = '/tmp/x509up_u%d' % os.getuid()
            if os.path.exists(proxypath):
                cert = proxypath
        return cert

try:
    from http_client_requests import get_client
except ImportError:
    from http_client_old import get_client

from urllib import quote as escape_url_path

