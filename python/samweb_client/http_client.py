
import sys, os
from datetime import datetime
from samweb_client.exceptions import *

def get_username():
    username = os.environ.get('GRID_USER', os.environ.get('USER'))
    if not username:
        try:
            import pwd
            username = pwd.getpwuid(os.getuid()).pw_name
        except:
            username = 'unknown'
    return username

def _get_from():
    import socket
    username = get_username()
    try:
        return '%s@%s' % (username, socket.getfqdn())
    except:
        return username

class SAMWebHTTPClient(object):
    max_timeout=6*60*60 # default max timeout
    maxretryinterval = 60 # default max retry interval
    _verbose = False # Full verbose mode
    verboseretries = True # whether to print output when retrying

    _default_headers = { 'Accept' : 'application/json', 'From' : _get_from()}

    def __init__(self, max_timeout=None, maxretryinterval=None, verbose=None, verboseretries=None, socket_timeout=None, *args, **kwargs):
        if max_timeout is not None:
            self.max_timeout = maxtimeout
        if socket_timeout is None:
            socket_timeout = 60*60 # default socket timeout
        self.socket_timeout = socket_timeout


        if maxretryinterval is not None:
            self.maxretryinterval = maxretryinterval
        if verboseretries is not None:
            self.verboseretries = verboseretries
        if verbose is not None:
            self.verbose = verbose
        self._cert = None
        if 'User-Agent' not in self._default_headers:
            self._default_headers['User-Agent'] = self._get_user_agent()

    def get_verbose(self):
        return self._verbose
    def set_verbose(self, verbose):
        self._verbose = verbose
        self._logger(self.__class__.__name__, "initialized")
    verbose = property(get_verbose, set_verbose)

    def get_timezone(self):
        return self._default_headers.get('Timezone')
    def set_timezone(self, tz):
        if not tz:
            return self._default_headers.pop('Timezone', None)
        else:
            self._default_headers['Timezone'] = str(tz)

    timezone = property(get_timezone, set_timezone)

    def get_socket_timeout(self):
        return self._socket_timeout
    def set_socket_timeout(self, timeout):
        # the timeout parameter is only supported in 2.6 onwards
        if sys.version_info >= (2,6):
            self._socket_timeout = timeout
        else:
            self._socket_timeout = None
    socket_timeout = property(get_socket_timeout, set_socket_timeout)

    def make_ssl_error(self, msg):
        """ Try to make sense of ssl errors and return a suitable exception object """
        if 'error:14094410' in msg:
            if self._cert:
                errmsg = "SSL error: %s: is client certificate valid?" % msg
            else:
                errmsg = "SSL error: %s: no client certificate found" % msg
        elif 'SSL_CTX_use_PrivateKey_file' in msg:
            errmsg = "SSL error: unable to open private key file"
        else:
            errmsg = "SSL error: %s" % msg
        return SAMWebSSLError(errmsg)

    @staticmethod
    def get_standard_certificate_path():
        import os
        cert = os.environ.get('X509_USER_PROXY')
        if not cert:
            cert = os.environ.get('X509_USER_CERT')
            key = os.environ.get('X509_USER_KEY')
            if cert and key: cert = (cert, key)
        if not cert:
            # look in standard place for cert
            proxypath = '/tmp/x509up_u%d' % os.getuid()
            if os.path.exists(proxypath):
                cert = proxypath
        return cert

    def use_client_certificate(self, cert=None, key=None):
        """ Use the given certificate and key for client ssl authentication """
        if cert:
            if key:
                self._cert = (cert, key)
            else:
                self._cert = cert
        else:
            self._cert = self.get_standard_certificate_path()

    def get_cert(self):
        return self._cert

    def get_ca_dir(self):
        return os.environ.get('X509_CERT_DIR',"/etc/grid-security/certificates")

    def get_default_headers(self):
        #return a copy as the user may modify it
        return dict(self._default_headers)

    def _logger(self, *args):
        if self._verbose:
            sys.stderr.write('%s %s\n' % (datetime.now().isoformat(), ' '.join(str(a) for a in args)))

    def _logMethod(self, method, url, params=None, data=None):
        if self.verbose:
            msg = [method, url]
            if params:
                msg.append("params=%s" % params)
            if data:
                if isinstance(data, dict):
                    msg.append("data=%s" % data)
                else:
                    msg.append("data=<%d bytes>" % len(data))
            self._logger(*msg)

    def postURL(self, url, data=None, content_type=None, **kwargs):
        return self._doURL(url, method='POST', data=data, content_type=content_type, **kwargs)

    def getURL(self, url, params=None, **kwargs):
        return self._doURL(url, method='GET',params=params, **kwargs)

    def putURL(self, url, data=None, content_type=None, **kwargs):
        return self._doURL(url, method='PUT', data=data, content_type=content_type, **kwargs)

    def deleteURL(self, url, params=None, **kwargs):
        return self._doURL(url, method='DELETE',params=params, **kwargs)

    def _get_user_agent(self):
        import samweb_client.client
        version = samweb_client.client.get_version()
        return 'SAMWebClient/%s (%s) python/%s' % (version, os.path.basename(sys.argv[0] or sys.executable), '%d.%d.%d' % sys.version_info[:3])

try:
    from http_client_requests import get_client
except ImportError:
    from http_client_urllib2 import get_client

from urllib import quote as escape_url_path
def escape_url_component(s):
    return escape_url_path(s, safe='')

