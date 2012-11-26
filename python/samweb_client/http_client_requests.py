# HTTP client using requests library

import requests
import time

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, SAMWebSSLError, SAMWebHTTPError

def _request_wrapper(func):
    def wrapper(self, url, format=None, content_type=None, *args, **kwargs):
        headers = {}
        if format=='json':
            headers['Accept'] = 'application/json'

        if content_type is not None:
            headers['Content-Type'] = content_type
        
        if headers:
            kwargs['headers'] = headers

        self._make_session()

        tmout = time.time() + self.maxtimeout
        retryinterval = 1

        while True:
            try:
                response = func(self, url, *args, **kwargs)
                return response
            except requests.exceptions.SSLError, ex:
                msg = ex.message
                raise self.make_ssl_error(msg)
            except requests.exceptions.HTTPError, ex:
                exc = SAMWebHTTPError(ex.response.request.method, url, ex.response.status_code, ex.response.text.rstrip())
                if 400 <= ex.response.status_code <= 500:
                    # For any 400 error + 500 errors, don't bother retrying
                    raise exc
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

    def __init__(self, verbosehttprequests=False, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._session = None
        self._config = {}
        if verbosehttprequests:
            self._config['verbose'] = sys.stderr

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
        return self._session.post(url, data=data, **kwargs)

__all__ = [ 'get_client' ]
