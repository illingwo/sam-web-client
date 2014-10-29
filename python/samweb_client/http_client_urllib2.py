# HTTP client using standard urllib2 implementation

from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time, socket, os, sys

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError

def get_client():
    # There is no local state, so just return the module
    return URLLib2HTTPClient()

class Response(object):
    """ Wrapper for the response object. Provides a text attribue that contains the body of the response.
    If stream = False, then the body is read immediately and the connection closed, else the data is not
    read from the server until you try to access it

    The API tries to be similar to that of the requests library, since it'd be nice if we could replace urllib2
    with that. However, it doesn't work with python 2.4, which we need for SL5 support.
    """

    def __init__(self, wrapped, stream=False):
        self._wrapped = wrapped
        if not stream:
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
    def __init__(self, cert):
        urllib2.HTTPSHandler.__init__(self)
        if isinstance(cert, basestring):
            self.cert = self.key = cert
        else:
            self.cert, self.key = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

class HTTP307RedirectHandler(urllib2.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        m = req.get_method()
        if (code in (301, 302, 303, 307) and m in ("GET", "HEAD")
            or code in (301, 302, 303) and m == "POST"):
            # Strictly (according to RFC 2616), 301 or 302 in response
            # to a POST MUST NOT cause a redirection without confirmation
            # from the user (of urllib2, in this case).  In practice,
            # essentially all clients do redirect in this case, so we
            # do the same.
            return Request(newurl,
                           headers=req.headers,
                           origin_req_host=req.get_origin_req_host(),
                           unverifiable=True)
        elif code==307:
            newreq = RequestWithMethod(newurl, method=req.get_method(), headers=req.headers, origin_req_host=req.get_origin_req_host(), unverifiable=True)
            if req.get_data() is not None: newreq.add_data(req.get_data())
            return newreq

        else:
            raise HTTPError(req.get_full_url(), code, msg, headers, fp)

class RequestWithMethod(urllib2.Request):
    def __init__(self, *args, **kwargs):
        self._method = kwargs.pop('method', None)
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method or urllib2.Request.get_method(self)

class URLLib2HTTPClient(SAMWebHTTPClient):
    """ HTTP client using standard urllib2 implementation """

    def __init__(self, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._opener = urllib2.build_opener(HTTP307RedirectHandler())

    def _doURL(self, url, method='GET', params=None, data=None, content_type=None, stream=False, headers=None,role=None):
        request_headers = self.get_default_headers()
        if headers is not None:
            request_headers.update(headers)
        if content_type:
            request_headers['Content-Type'] = content_type

        if role is not None:
            request_headers['SAM-Role'] = str(role)

        if method in ('POST', 'PUT'):
            # these always require body data
            if data is None:
                data = ''

        self._logMethod(method, url, params=params, data=data)

        if data is not None and not isinstance(data, basestring):
            data = urlencode(data, doseq=True)

        if params is not None:
            if '?' not in url: url += '?'
            else: url += '&'
            url += urlencode(params, doseq=True)
        tmout = time.time() + self.maxtimeout
        retryinterval = 1

        request = RequestWithMethod(url, method=method, headers=request_headers)
        if data is not None:
            request.add_data(data)
        while True:
            try:
                return Response(self._opener.open(request), stream=stream)
            except HTTPError, x:
                #python 2.4 treats 201 and up as errors instead of normal return codes
                if 201 <= x.code <= 299:
                    return Response(x)
                if x.headers.get('Content-Type') == 'application/json':
                    err = json.load(x)
                    errmsg = err['message']
                    errtype = err['error']
                else:
                    if x.code >= 500:
                        errmsg = "HTTP error: %d %s" % (x.code, x.msg)
                    else:
                        errmsg = x.read().rstrip()
                    errtype = None
                x.close() # ensure that the socket is closed (otherwise it may hang around in the traceback object)
                # retry server errors
                if x.code >= 500 and time.time() < tmout:
                    pass
                else:
                    raise makeHTTPError(method, url, x.code, errmsg, errtype)
            except URLError, x:
                if isinstance(x.reason, socket.sslerror):
                    raise self.make_ssl_error(str(x.reason))
                else:
                    errmsg = str(x.reason)
                if time.time() >= tmout:
                    raise SAMWebConnectionError(errmsg)
            except httplib.HTTPException, x:
                # I'm not sure exactly what circumstances cause this
                # but assume that it's a retriable error
                try:
                    errmsg = str(x.reason)
                except AttributeError:
                    errmsg = str(x)
                if time.time() >= tmout:
                    raise SAMWebConnectionError(errmsg)

            if self.verboseretries:
                print>>sys.stderr, '%s: retrying in %d s' %( errmsg, retryinterval)
            time.sleep(retryinterval)
            retryinterval*=2
            if retryinterval > self.maxretryinterval:
                retryinterval = self.maxretryinterval

    def use_client_certificate(self, cert=None, key=None):
        """ Use the given certificate and key for client ssl authentication """
        if not cert:
            cert = self.get_standard_certificate_path()
        if cert and not key:
            key = cert
        if cert:
            self._opener = urllib2.build_opener(HTTPSClientAuthHandler((cert,key)), HTTP307RedirectHandler )
            self._cert = cert

__all__ = [ 'get_client' ]
