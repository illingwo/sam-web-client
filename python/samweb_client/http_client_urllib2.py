# HTTP client using standard urllib2 implementation

from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time, socket, os, sys, zlib

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError

def get_client():
    # There is no local state, so just return the module
    return URLLib2HTTPClient()

def _read_response(response, chunk_size=128*1024):
    while True:
        data = response.read(chunk_size)
        if not data: break
        #print>>sys.stderr, 'Read %d bytes' % len(data)
        yield data
    response.close()

def _gzip_decoder(iterator):
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    for chunk in iterator:
        decoded = decoder.decompress(chunk)
        if decoded:
            #print>>sys.stderr, 'Decompressed %d bytes' % len(decoded)
            yield decoded
    else:
        decoded = decoder.flush()
        if decoded:
            #print>>sys.stderr, 'Decompressed %d bytes' % len(decoded)
            yield decoded

class Response(object):
    """ Wrapper for the response object. Provides a text attribue that contains the body of the response.
    If stream = False, then the body is read immediately and the connection closed, else the data is not
    read from the server until you try to access it

    The API tries to be similar to that of the requests library, since it'd be nice if we could replace urllib2
    with that. However, it doesn't work with python 2.4, which we need for SL5 support.
    """

    def __init__(self, wrapped, logger, stream=False):
        self._wrapped = wrapped
        self._data = _read_response(self._wrapped)

        # handle zipped content
        if self.headers.get('Content-Encoding','').lower() == 'gzip':
            self._data = _gzip_decoder(self._data)
            logger("Decompressing gzipped response body")

        # if not streaming, load the whole thing now
        if not stream:
            self._data = tuple(self._data)

    @property
    def text(self):
        try:
            return ''.join(self._data)
        except Exception, ex:
            raise Error("Error reading response body: %s" % str(ex))

    @property
    def status_code(self):
        return self._wrapped.code
    @property
    def headers(self):
        return self._wrapped.headers

    def json(self):
        # json just does a read() on the file object, so we aren't losing anything
        # by reading the whole thing into a string
        return json.loads(self.text)

    def iter_lines(self):
        pending = None
        try:
            for chunk in self.iter_content():
                if pending is not None:
                    chunk = pending + chunk
                lines = chunk.splitlines()
                if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                    pending = lines.pop()
                else:
                    pending = None

                for line in lines:
                    yield line
            if pending is not None:
                yield pending
        except Exception, ex:
            raise Error("Error reading response body: %s" % str(ex))

    def iter_content(self, chunk_size=1):
        # chunk size is currently ignored
        try:
            return iter(self._data)
        except Exception, ex:
            raise Error("Error reading response body: %s" % str(ex))

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
        if cert:
            if isinstance(cert, basestring):
                self.cert = self.key = cert
            else:
                self.cert, self.key = cert
            try:
                # python 2.7.9 support
                from ssl import create_default_context, CERT_NONE
                """ could allow verification with something like
                context = create_default_context(capath="/etc/grid-security/certificates")
                """
                context = create_default_context()
                # disable certificate verification
                context.check_hostname = False
                context.verify_mode = CERT_NONE

                # load the client cert
                context.load_cert_chain(self.cert, self.key)
                self.connargs = { 'context' : context }
            except ImportError:
                # older python
                self.connargs = { "key_file" : self.key, "cert_file" : self.cert }
        else:
            self.connargs = {}

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, **self.connargs)

class HTTP307RedirectHandler(urllib2.HTTPRedirectHandler):

    # We want to keep trying if the redirect provokes an error
    max_repeats = sys.maxint

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        m = req.get_method()
        try:
            logger = req.logger
        except AttributeError:
            pass

        if code in (301, 302, 303):
            # Strictly (according to RFC 2616), 301 or 302 in response
            # to a POST MUST NOT cause a redirection without confirmation
            # from the user (of urllib2, in this case).  In practice,
            # essentially all clients do redirect in this case, so we
            # do the same.
            logger("HTTP", code, "redirect to GET", newurl)
            return LoggingRequest(newurl,
                           headers=req.headers,
                           origin_req_host=req.get_origin_req_host(),
                           unverifiable=True, logger=logger)
        elif code in (307, 308):
            logger("HTTP", code, "redirect to", req.get_method(), newurl)
            newreq = RequestWithMethod(newurl, method=req.get_method(), headers=req.headers, origin_req_host=req.get_origin_req_host(), unverifiable=True, logger=logger)
            if req.get_data() is not None: newreq.add_data(req.get_data())
            return newreq
        else:
            raise HTTPError(req.get_full_url(), code, msg, headers, fp)

    # 308 should be handles the same way as 307
    http_error_308 = urllib2.HTTPRedirectHandler.http_error_307

class LoggingRequest(urllib2.Request):
    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger', self._nulllogger)
        urllib2.Request.__init__(self, *args, **kwargs)
    @staticmethod
    def _nulllogger(*args):
        return

class RequestWithMethod(LoggingRequest):
    def __init__(self, *args, **kwargs):
        self._method = kwargs.pop('method', None)
        LoggingRequest.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method or urllib2.Request.get_method(self)

class URLLib2HTTPClient(SAMWebHTTPClient):
    """ HTTP client using standard urllib2 implementation """

    def __init__(self, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._opener = urllib2.build_opener(HTTP307RedirectHandler())

    def _doURL(self, url, method='GET', params=None, data=None, content_type=None, stream=False, compress=False, headers=None,role=None):
        request_headers = self.get_default_headers()
        if headers is not None:
            request_headers.update(headers)
        if content_type:
            request_headers['Content-Type'] = content_type

        if compress or stream:
            # enable gzipped encoding for streamed data since that might be large
            request_headers['Accept-Encoding'] = 'gzip'

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
        tmout = time.time() + self.max_timeout
        retryinterval = 1

        request = RequestWithMethod(url, method=method, headers=request_headers, logger=self._logger)
        if data is not None:
            request.add_data(data)
        kwargs = {}
        if self.socket_timeout is not None:
            kwargs['timeout'] = self.socket_timeout
        while True:
            try:
                return Response(self._opener.open(request, **kwargs), stream=stream, logger=self._logger)
            except HTTPError, x:
                #python 2.4 treats 201 and up as errors instead of normal return codes
                if 201 <= x.code <= 299:
                    return Response(x, logger=self._logger)
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
        SAMWebHTTPClient.use_client_certificate(self, cert, key)
        self._opener = urllib2.build_opener(HTTPSClientAuthHandler(self._cert), HTTP307RedirectHandler )

__all__ = [ 'get_client' ]
