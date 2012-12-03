# HTTP client using standard urllib2 implementation

from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time, socket, os, sys

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, SAMWebSSLError, SAMWebHTTPError

def get_client():
    # There is no local state, so just return the module
    return URLLib2HTTPClient()

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

class URLLib2HTTPClient(SAMWebHTTPClient):
    """ HTTP client using standard urllib2 implementation """

    def __init__(self, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)

    def postURL(self, url, data=None, content_type=None, **kwargs):
        return self._doURL(url, action='POST', data=data, content_type=content_type, **kwargs)

    def getURL(self, url, params=None,format=None, **kwargs):
        return self._doURL(url,action='GET',params=params,format=format, **kwargs)

    def _doURL(self, url, action='GET', params=None, format=None, data=None, content_type=None, prefetch=True):
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
        tmout = time.time() + self.maxtimeout
        retryinterval = 1

        request = Request(url, headers=headers)
        if data is not None:
            request.add_data(data)
        if content_type:
            request.add_header('Content-Type', content_type)
        while True:
            try:
                return Response(urlopen(request), prefetch=prefetch)
            except HTTPError, x:
                #python 2.4 treats 201 and up as errors instead of normal return codes
                if 201 <= x.code <= 299:
                    return Response(x)
                errmsg = x.read().rstrip()
                x.close() # ensure that the socket is closed (otherwise it may hang around in the traceback object)
                # retry server errors (excluding internal errors)
                if x.code > 500 and time.time() < tmout:
                    pass
                else:
                    raise SAMWebHTTPError(action, url, x.code, errmsg)
            except URLError, x:
                if isinstance(x.reason, socket.sslerror):
                    raise self.make_ssl_error(str(x.reason))
                else:
                    errmsg = str(x.reason)
                if time.time() >= tmout:
                    raise ConnectionError(errmsg)

            if self.verboseretries:
                print>>sys.stderr, '%s: %s: retrying in %d s' %( url, errmsg, retryinterval)
            time.sleep(retryinterval)
            retryinterval*=2
            if retryinterval > self.maxretryinterval:
                retryinterval = self.maxretryinterval

    def use_client_certificate(self, cert=None, key=None):
        """ Use the given certificate and key for client ssl authentication """
        if not cert:
            cert = key = self.get_standard_certificate_path()
        if cert:
            opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, key) )
            urllib2.install_opener(opener)
            self._cert = cert

__all__ = [ 'get_client' ]
