
from samweb_client import Error

class SAMWebSSLError(Error):
    pass

def make_ssl_error(msg, cert):
    """ Try to make sense of ssl errors and return a suitable exception object """
    if 'error:14094410' in msg:
        if cert:
            errmsg = "SSL error: %s: is client certificate valid?" % msg
        else:
            errmsg = "SSL error: %s: no client certificate has been installed" % msg
    else:
        errmsg = "SSL error: %s" % msg
    return SAMWebSSLError(errmsg)

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

def get_standard_certificate_path():
    import os
    cert = os.environ.get('X509_USER_PROXY')
    if not cert:
        # look in standard place for cert
        proxypath = '/tmp/x509up_u%d' % os.getuid()
        if os.path.exists(proxypath):
            cert = proxypath
    return cert

try:
    from http_client_requests import *
except ImportError:
    from http_client_old import *

from urllib import quote as escape_url_path

