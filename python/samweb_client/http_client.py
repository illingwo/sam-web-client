
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

def _get_from():
    import os,pwd,socket
    username = os.environ.get('USER')
    if not username:
        try:
            username = pwd.getpwuid(os.getuid()).pw_name
        except:
            username = '<unknown>'
    try:
        return '%s@%s' % (username, socket.getfqdn())
    except:
        return username

class SAMWebHTTPClient(object):
    maxtimeout=60*30 # default max timeout
    maxretryinterval = 60 # default max retry interval
    verbose = False # Full verbose mode
    verboseretries = True # whether to print output when retrying

    _default_headers = { 'Accept' : 'application/json', 'From' : _get_from() }

    def __init__(self, maxtimeout=None, maxretryinterval=None, verbose=None, verboseretries=None, *args, **kwargs):
        if maxtimeout is not None:
            self.maxtimeout = maxtimeout
        if maxretryinterval is not None:
            self.maxretryinterval = maxretryinterval
        if verboseretries is not None:
            self.verboseretries = verboseretries
        if verbose is not None:
            self.verbose = verbose

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

    def get_default_headers(self):
        #return a copy as the user may modify it
        return dict(self._default_headers)

try:
    from http_client_requests import get_client
except ImportError:
    from http_client_old import get_client

from urllib import quote as escape_url_path

