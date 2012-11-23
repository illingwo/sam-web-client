
from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time, socket, os

from samweb_client import Error, json

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

maxtimeout=60*30
maxretryinterval = 60

class Response(object):
    """ Wrapper for the response object. Provides a text attribue that contains the body of the response.
    If prefetch = True, then the body is read immediately and the connection closed, else the data is not
    read from the server until you try to access it

    The API tries to be similar to that of the requests library, since it'd be nice if we could replace urllib2
    with that. However, it doesn't work with python 2.4, which we need for SL5 support.
    """

    def __init__(self, wrapped, prefetch=True):
        self._wrapped = wrapped
        if prefetch:
            self._load_data()
        else:
            self._data = None

    def _load_data(self):
        self._data = self._wrapped.read()
        self._wrapped.close()

    @property
    def text(self):
        if self._data is not None:
            return self._data
        else:
            self._load_data()
            return self._data
    @property
    def status_code(self):
        return self._wrapped.code
    @property
    def headers(self):
        return self._wrapped.headers
    @property
    def json(self):
        if self._data is None:
            return json.load(self._wrapped)
        else:
            return json.loads(self._data)

    def iter_lines(self):
        if self._data is None:
            for l in self._wrapped:
                yield l
        else:
            for l in self._data.split('\n'):
                yield l

    def __del__(self):
        try:
            self._wrapped.close()
        except: pass

# handler to cope with client certificate auth
# Note that this does not verify the server certificate
# Since the main purpose is for the server to authenticate
# the client. However, you should be cautious about sending
# sensitive infomation (not that SAM deals with that)
# as there's no protection against man-in-the-middle attacks
class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, cert, key):
        urllib2.HTTPSHandler.__init__(self)
        self.cert = cert
        self.key = key

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

def postURL(url, data=None, content_type=None, **kwargs):
    return _doURL(url, action='POST', data=data, content_type=content_type, **kwargs)

def getURL(url, params=None,format=None, **kwargs):
    return _doURL(url,action='GET',params=params,format=format, **kwargs)

def _doURL(url, action='GET', params=None, format=None, data=None, content_type=None, prefetch=True):
    headers = {}
    if format=='json':
        headers['Accept'] = 'application/json'

    if action in ('POST', 'PUT'):
        # these always require body data
        if data is None:
            data = ''
    if isinstance(data, dict):
        data = urlencode(data)
    if params is not None:
        if '?' not in url: url += '?'
        else: url += '&'
        url += urlencode(params)
    tmout = time.time() + maxtimeout
    retryinterval = 1

    request = Request(url, headers=headers)
    if data:
        request.add_data(data)
    if content_type:
        request.add_header('Content-Type', content_type)
    while True:
        try:
            remote = urlopen(request)
        except HTTPError, x:
            #python 2.4 treats 201 and up as errors instead of normal return codes
            if 201 <= x.code <= 299:
                return Response(x)
            errmsg = x.read().strip()
            x.close() # ensure that the socket is closed (otherwise it may hang around in the traceback object)
            # retry server errors (excluding internal errors)
            if x.code > 500 and time.time() < tmout:
                print "Error %s" % errmsg
            else:
                raise SAMWebHTTPError(action, url, x.code, errmsg)
        except URLError, x:
            if isinstance(x.reason, socket.sslerror):
                msg = str(x.reason)
                if 'error:14094410' in msg:
                    if client_cert:
                        raise Error("SSL error: %s: is client certificate valid?" % msg)
                    else:
                        raise Error("SSL error: %s: no client certificate has been installed" % msg)
                else:
                    raise Error("SSL error: %s" % x.reason)
            print 'URL %s not responding' % url
        else:
            return Response(remote, prefetch=prefetch)

        time.sleep(retryinterval)
        retryinterval*=2
        if retryinterval > maxretryinterval:
            retryinterval = maxretryinterval

client_cert = None
def use_client_certificate(cert=None, key=None):
    """ Use the given certificate and key for client ssl authentication """
    if not cert:
        cert = key = os.environ.get('X509_USER_PROXY')
        if not cert:
            # look in standard place for cert
            proxypath = '/tmp/x509up_u%d' % os.getuid()
            if os.path.exists(proxypath):
                cert = key = proxypath
    if cert:
        opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, key) )
        urllib2.install_opener(opener)
        global client_cert
        client_cert = cert

__all__ = ['postURL', 'getURL', 'use_client_certificate', 'quote', 'quote_plus']
