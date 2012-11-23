import requests
import time

from samweb_client import Error, json
from http_client import make_ssl_error, SAMWebHTTPError,  get_standard_certificate_path

_cert = None

maxtimeout=60*30
maxretryinterval = 60

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

        tmout = time.time() + maxtimeout
        retryinterval = 1

        while True:
            try:
                response = func(self, url, *args, **kwargs)
                response.raise_for_status() # convert errors into exceptions
                return response
            except requests.exceptions.SSLError, ex:
                msg = ex.message
                if isinstance(self._cert, tuple): cert = self._cert[0]
                else: cert = _cert
                raise make_ssl_error(msg, cert)
            except requests.exceptions.HTTPError, ex:
                raise SAMWebHTTPError(ex.response.request.method, url, ex.response.status_code, ex.response.text.rstrip())
            except requests.exceptions.Timeout, ex:
                errmsg = "Timed out waiting for response from %s" % url
            except requests.exceptions.ConnectionError, ex:
                errmsg = str(ex)
            
            if time.time() > tmout:
                raise Error("%s: %s" % (url, errmsg))
                
            print '%s: %s: retrying in %d s' %( url, errmsg, retryinterval)
            time.sleep(retryinterval)
            retryinterval*=2
            if retryinterval > maxretryinterval:
                retryinterval = maxretryinterval

    return wrapper

def get_client():
    return RequestsHTTPClient()

import sys

class RequestsHTTPClient(object):

    def __init__(self):
        self._session = None
        self._cert =None

    def _make_session(self):
        if self._session is None:
            self._session = requests.Session(verify=False, cert=self._cert, config={'verbose': sys.stderr})

    def use_client_certificate(self, cert=None, key=None):
        if cert:
            if key:
                self._cert = (cert, key)
            else:
                self._cert = cert
        else:
            self._cert = get_standard_certificate_path()
        self._session = None # This will clear any existing session with a different cert

    @_request_wrapper
    def getURL(self, url, params=None, **kwargs):
        return self._session.get(url, params=params, **kwargs)

    @_request_wrapper
    def postURL(self, url, data=None, **kwargs):
        return self._session.post(url, data=data, **kwargs)

