
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


class Error(Exception):
  pass

from client import SAMWebClient 
import projects
import files
