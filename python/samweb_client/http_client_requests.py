# HTTP client using requests library

import requests
import time

from samweb_client import Error, json
from http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError

def get_client(*args, **kwargs):
    return RequestsHTTPClient(*args, **kwargs)

import sys

# it might be a good idea to verify the server, but at the moment we never send sensitive data. So shut it up.
from requests.packages import urllib3
urllib3.disable_warnings()

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

    def _doURL(self, url, method="GET", content_type=None, role=None, *args, **kwargs):
        headers = self.get_default_headers()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])

        if content_type is not None:
            headers['Content-Type'] = content_type

        if role is not None:
            headers['SAM-Role'] = str(role)
        
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
                        if response.status_code >= 500:
                            errmsg = "HTTP error: %d %s" % (response.status_code, response.reason)
                        else:
                            errmsg = response.text.rstrip()
                        errtype = None
                    exc = makeHTTPError(response.request.method, url, response.status_code, errmsg, errtype)
                    if 400 <= response.status_code < 500:
                        # For any 400 error, don't bother retrying
                        raise exc
            except requests.exceptions.SSLError, ex:
                errmsg = str(ex.message)
                raise self.make_ssl_error(errmsg)
            except requests.exceptions.Timeout, ex:
                errmsg = "%s: Timed out waiting for response" % (url,)
                exc = SAMWebConnectionError(errmsg)
            except requests.exceptions.ConnectionError, ex:
                errmsg = "%s: %s" % (url, str(ex))
                exc = SAMWebConnectionError(errmsg)
            
            if time.time() >= tmout:
                raise exc
                
            if self.verboseretries:
                print '%s: retrying in %d s' %( errmsg, retryinterval)
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
