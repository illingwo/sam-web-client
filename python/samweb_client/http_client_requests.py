# HTTP client using requests library

import requests
import time

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError

def _request_wrapper(func):
    def wrapper(self, url, content_type=None, *args, **kwargs):
        headers = self.get_default_headers()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])

        if content_type is not None:
            headers['Content-Type'] = content_type
        
        kwargs['headers'] = headers

        self._make_session()

        tmout = time.time() + self.maxtimeout
        retryinterval = 1

        while True:
            try:
                response = func(self, url, *args, **kwargs)
                if 200 <= response.status_code < 300:
                    return response
                else:
                    # something went wrong
                    jsonerr = response.json
                    if jsonerr is None:
                        errmsg = response.text.rstrip()
                        errtype =  response.reason
                    else:
                        errmsg = jsonerr['message']
                        errtype = jsonerr['error']
                    exc = makeHTTPError(response.request.method, url, response.status_code, errmsg, errtype)
                    if 400 <= response.status_code <= 500:
                        # For any 400 error + 500 errors, don't bother retrying
                        raise exc
            except requests.exceptions.SSLError, ex:
                msg = ex.message
                raise self.make_ssl_error(msg)
            except requests.exceptions.Timeout, ex:
                exc = SAMWebConnectionError("%s: Timed out waiting for response" % (url,))
            except requests.exceptions.ConnectionError, ex:
                exc = SAMWebConnectionError("%s: %s" % (url, str(ex)))
            
            if time.time() >= tmout:
                raise exc
                
            if self.verboseretries:
                print '%s: retrying in %d s' %( str(exc), retryinterval)
            time.sleep(retryinterval)
            retryinterval*=2
            if retryinterval > self.maxretryinterval:
                retryinterval = self.maxretryinterval

    return wrapper

def get_client(*args, **kwargs):
    return RequestsHTTPClient(*args, **kwargs)

import sys

class RequestsHTTPClient(SAMWebHTTPClient):

    def __init__(self, verbose=False, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._session = None
        self._config = {}
        if verbose: self.verbose = True

    @property
    def verbose(self): return bool(self._config.get('verbose'))
    @verbose.setter
    def verbose(self, verbose):
        if verbose: self._config['verbose'] = sys.stderr
        else:
            try:
                del self._config['verbose']
            except KeyError: pass

    def _make_session(self):
        if self._session is None:
            self._session = requests.Session(verify=False, cert=self._cert, config=self._config)

    def use_client_certificate(self, cert=None, key=None):
        if cert:
            if key:
                self._cert = (cert, key)
            else:
                self._cert = cert
        else:
            self._cert = self.get_standard_certificate_path()
        self._session = None # This will clear any existing session with a different cert

    @_request_wrapper
    def getURL(self, url, params=None, **kwargs):
        return self._session.get(url, params=params, **kwargs)

    @_request_wrapper
    def postURL(self, url, data=None, **kwargs):
        # httplib isn't sending a Content-Length: 0 header for empty bodies
        # even though the latest HTTP revision says this is legal, cherrypy and
        # some other servers don't like it
        # this may be fixed in python 2.7.4
        if not data:
            kwargs.setdefault('headers',{})['Content-Length'] = '0'
        return self._session.post(url, data=data, **kwargs)

__all__ = [ 'get_client' ]
