
from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time, socket

from samweb_client import Error

class SAMWebHTTPError(Error):
    def __init__(self, method, url, args, code, msg):
        self.method = method
        self.url = url
        self.args = args
        self.code = code
        self.msg = msg

    def __str__(self):
        if 400 <= self.code < 500:
            return self.msg
        else:
            return "HTTP error: %(code)d %(msg)s\nURL: %(url)s" % self.__dict__

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

def postURL(url, args=None, body=None, content_type=None):
    return _doURL(url, action='POST', args=args, body=body, content_type=content_type)

def getURL(url, args=None,format=None):
    return _doURL(url,action='GET',args=args,format=format)

def _doURL(url, action='GET', args=None, format=None, body=None, content_type=None):
    headers = {}
    if format=='json':
        headers['Accept'] = 'application/json'

    params=None
    if action =='POST':
        if body is None:
            if args is None: args = {}
            params = urlencode(args)
    else:
        if args is not None:
            if '?' not in url: url += '?'
            else: url += '&'
            url += urlencode(args)
    tmout = time.time() + maxtimeout
    retryinterval = 1

    request = Request(url, data=params, headers=headers)
    if body:
        request.add_data(body)
    if content_type:
        request.add_header('Content-Type', content_type)
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
                raise SAMWebHTTPError(action, url, args, x.code, errmsg)
        except URLError, x:
            if isinstance(x.reason, socket.sslerror):
                raise Error("SSL error: %s" % x.reason)
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

__all__ = ['postURL', 'getURL', 'use_client_certificate', 'quote', 'quote_plus']
