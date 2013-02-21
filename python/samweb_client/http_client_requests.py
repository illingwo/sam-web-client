# HTTP client using requests library

import requests
import time

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError

def get_client(*args, **kwargs):
    return RequestsHTTPClient(*args, **kwargs)

import sys

class RequestsHTTPClient(SAMWebHTTPClient):

    def __init__(self, verbose=False, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._session = None
        self.verbose = verbose

    def _make_session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.verify = False
            self._session.cert = self._cert

    def use_client_certificate(self, cert=None, key=None):
        if cert:
            if key:
                self._cert = (cert, key)
            else:
                self._cert = cert
        else:
            self._cert = self.get_standard_certificate_path()
        self._session = None # This will clear any existing session with a different cert

    def _doURL(self, url, method="GET", content_type=None, *args, **kwargs):
        headers = self.get_default_headers()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])

        if content_type is not None:
            headers['Content-Type'] = content_type
        
        kwargs['headers'] = headers

        self._logMethod( method, url, params=kwargs.get("params"), data=kwargs.get("data"))

        self._make_session()

        tmout = time.time() + self.maxtimeout
        retryinterval = 1

        while True:
            try:
                response = self._session.request(method, url, *args, **kwargs)
                if 200 <= response.status_code < 300:
                    return response
                else:
                    # something went wrong
                    if 'application/json' in response.headers['Content-Type']:
                        jsonerr = response.json()
                        errmsg = jsonerr['message']
                        errtype = jsonerr['error']
                    else:
                        errmsg = response.text.rstrip()
                        errtype = None
                    exc = makeHTTPError(response.request.method, url, response.status_code, errmsg, errtype)
                    if 400 <= response.status_code <= 500:
                        # For any 400 error + 500 errors, don't bother retrying
                        raise exc
            except requests.exceptions.SSLError, ex:
                msg = str(ex.message)
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

    def postURL(self, url, data=None, **kwargs):
        # httplib isn't sending a Content-Length: 0 header for empty bodies
        # even though the latest HTTP revision says this is legal, cherrypy and
        # some other servers don't like it
        # this may be fixed in python 2.7.4
        if not data:
            kwargs.setdefault('headers',{})['Content-Length'] = '0'
        return SAMWebHTTPClient.postURL(self, url, data, **kwargs)

    def putURL(self, url, data=None, **kwargs):
        # httplib isn't sending a Content-Length: 0 header for empty bodies
        # even though the latest HTTP revision says this is legal, cherrypy and
        # some other servers don't like it
        # this may be fixed in python 2.7.4
        if not data:
            kwargs.setdefault('headers',{})['Content-Length'] = '0'
        return SAMWebHTTPClient.putURL(self, url, data, **kwargs)

    def _get_user_agent(self):
        ua = SAMWebHTTPClient._get_user_agent(self)
        ua += " requests/%s" % requests.__version__
        return ua

__all__ = [ 'get_client' ]
