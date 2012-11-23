import requests
import time

from samweb_client import Error, json
from http_client import make_ssl_error, SAMWebHTTPError,  get_standard_certificate_path

_cert = None
def use_client_certificate(cert=None, key=None):
    global _cert
    if cert:
        if key:
            _cert = (cert, key)
        else:
            _cert = cert
    else:
        _cert = get_standard_certificate_path()

maxtimeout=60*30
maxretryinterval = 60

def _request_wrapper(func):
    def wrapper(url, format=None, content_type=None, *args, **kwargs):
        headers = {}
        if format=='json':
            headers['Accept'] = 'application/json'

        if content_type is not None:
            headers['Content-Type'] = content_type
        
        if headers:
            kwargs['headers'] = headers

        if url.startswith('https:'):
            kwargs.update({'verify':False, 'cert':_cert})

        tmout = time.time() + maxtimeout
        retryinterval = 1

        while True:
            try:
                response = func(url, *args, **kwargs)
                response.raise_for_status() # convert errors into exceptions
                return response
            except requests.exceptions.SSLError, ex:
                msg = ex.message
                if isinstance(_cert, tuple): cert = cert[0]
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

@_request_wrapper
def getURL(url, params=None, **kwargs):
    return requests.get(url, params=params, **kwargs)

@_request_wrapper
def postURL(url, data=None, **kwargs):
    return requests.post(url, data=data, **kwargs)

