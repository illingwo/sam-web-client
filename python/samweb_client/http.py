
from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time, socket

from samweb_client import Error, samweb_connect

class SAMWebHTTPError(Error):
    pass

maxtimeout=60*30
maxretryinterval = 60

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

def postURL(url, args):
    return _doURL(url, action='POST', args=args)

def getURL(url, args=None,format=None):
    return _doURL(url,action='GET',args=args,format=format)

def _doURL(url, action='GET', args=None, format=None):
    # if provided with a relative url, add the baseurl
    if '://' not in url:
        url = samweb_connect.baseurl + url
    headers = {}
    if format=='json':
        headers['Accept'] = 'application/json'
    if action =='POST':
        if args is None: args = {}
        params = urlencode(args)
    else:
        params = None
        if args is not None:
            if '?' not in url: url += '?'
            else: url += '&'
            url += urlencode(args)
    tmout = time.time() + maxtimeout
    retryinterval = 1

    request = Request(url, data=params, headers=headers)
    while True:
        try:
            remote = urlopen(request)
        except HTTPError, x:
            #python 2.4 treats 201 and up as errors instead of normal return codes
            if 201 <= x.code <= 299:
                return x
            errmsg = x.read().strip()
            # retry server errors (excluding internal errors)
            if x.code > 500 and time.time() < tmout:
                print "Error %s" % errmsg
            else:
                if action == 'POST':
                    msg = "POST to %s, args = %s" % ( url, args)
                else:
                    msg = "GET of %s" % (url, )
                raise SAMWebHTTPError("%s, failed with %s: %s" % (msg, str(x), errmsg))
        except URLError, x:
            if isinstance(x.reason, socket.sslerror):
                raise SAMWebHTTPError("SSL error: %s" % x.reason)
            print 'URL %s not responding' % url
        else:
            return remote

        time.sleep(retryinterval)
        retryinterval*=2
        if retryinterval > maxretryinterval:
            retryinterval = maxretryinterval

def use_client_certificate(cert, key):
    """ Use the given certificate and key for client ssl authentication """
    opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, key) )
    urllib2.install_opener(opener)

__all__ = ['postURL', 'getURL', 'use_client_certificate']
