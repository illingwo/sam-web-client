
import sys, os
from datetime import datetime
from samweb_client.exceptions import *

def _get_from():
    import pwd,socket
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

    _default_headers = { 'Accept' : 'application/json', 'From' : _get_from()}

    def __init__(self, maxtimeout=None, maxretryinterval=None, verbose=None, verboseretries=None, *args, **kwargs):
        if maxtimeout is not None:
            self.maxtimeout = maxtimeout
        if maxretryinterval is not None:
            self.maxretryinterval = maxretryinterval
        if verboseretries is not None:
            self.verboseretries = verboseretries
        if verbose is not None:
            self.verbose = verbose
        self._cert = None
        if 'User-Agent' not in self._default_headers:
            self._default_headers['User-Agent'] = self._get_user_agent()

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

    def get_standard_certificate_path(self):
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

    def get_default_headers(self):
        #return a copy as the user may modify it
        return dict(self._default_headers)

    def _logMethod(self, method, url, params=None, data=None):
        if self.verbose:
            sys.stderr.write("%s %s %s" % (datetime.now().isoformat(), method, url))
            if params:
                sys.stderr.write(" params=%s" % params)
            if data:
                if isinstance(data, dict):
                    sys.stderr.write(" data=%s" % data)
                else:
                    sys.stderr.write(" data=<%d bytes>" % len(data))
            sys.stderr.write("\n")

    def postURL(self, url, data=None, content_type=None, **kwargs):
        return self._doURL(url, method='POST', data=data, content_type=content_type, **kwargs)

    def getURL(self, url, params=None, **kwargs):
        return self._doURL(url, method='GET',params=params, **kwargs)

    def putURL(self, url, data=None, content_type=None, **kwargs):
        return self._doURL(url, method='PUT', data=data, content_type=content_type, **kwargs)

    def deleteURL(self, url, params=None, **kwargs):
        return self._doURL(url, method='DELETE',params=params, **kwargs)

    def _get_user_agent(self):
        import sys
        try:
            from _version import version
        except ImportError:
            version = 'unknown'

        # if running from a git checkout, try to obtain the version
        gitdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.git")
        if os.path.exists(gitdir):
            import subprocess
            try:
                p = subprocess.Popen(["git", "--work-tree=%s" % os.path.join(gitdir,".."), "--git-dir=%s" % gitdir, "describe", "--tags", "--dirty"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if p.wait() == 0:
                    version = p.stdout.read().strip()
            except: pass
        return 'SAMWebClient/%s (%s) python/%s' % (version, os.path.basename(sys.argv[0] or sys.executable), '%d.%d.%d' % sys.version_info[:3])

try:
    from http_client_requests import get_client
except ImportError:
    from http_client_urllib2 import get_client

from urllib import quote as escape_url_path
def escape_url_component(s):
    return escape_url_path(s, safe='')

