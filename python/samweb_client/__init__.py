
import os, pwd

# Get a json library if available. Try the standard library if available;
# simplejson if that's available, else fall back to old (py2.4 compatible simplejson)
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import simplejson_209 as json

def convert_from_unicode(input):
    """ convert an object structure (specifically those returned via json
    to use plain strings insead of unicode """
    if isinstance(input, dict):
        return type(input)( (convert_from_unicode(key), convert_from_unicode(value)) for key, value in input.iteritems())
    elif isinstance(input, list):
        return [convert_from_unicode(element) for element in input]
    elif isinstance(input, unicode):
        try:
            return input.encode('ascii')
        except UnicodeEncodeError:
            # can't be represented as ascii; leave it as unicode
            return input
    else:
        return input

from exceptions import *

import http_client
from client import SAMWebClient 
import projects
import files
import admin
import misc
