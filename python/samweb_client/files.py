
from samweb_client import *
from samweb_client.http import *

def listFiles(dimensions=None, defname=None):
    if defname is not None:
        result = getURL(samweb_connect.baseurl + '/definitions/name/%s/files/list' % defname)
    else:
        if len(dimensions) > 1024:
            method = postURL
        else:
            method = getURL
        result = method(samweb_connect.baseurl + '/files/list', {'dims':dimensions})
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def parseDims(dimensions):
    """ For debugging only """
    if len(dimensions) > 1024:
        method = postURL
    else:
        method = getURL
    result = method(samweb_connect.baseurl + '/files/list', {'dims':dimensions, "parse_only": "1"})
    return result.read().strip()

def countFiles(dimensions=None, defname=None):
    if defname is not None:
        result = getURL(samweb_connect.baseurl + '/definitions/name/%s/files/count' % defname)
    else:
        result = getURL(samweb_connect.baseurl + '/files/count', {'dims':dimensions})
    return long(result.read().strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % quote(filenameorid)
    return path

def locateFile(filenameorid):
    url = samweb_connect.baseurl + _make_file_path(filenameorid) + '/locations'
    result = getURL(url)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def getMetadata(filenameorid, format=None):
    url = samweb_connect.baseurl + _make_file_path(filenameorid) + '/metadata'
    result = getURL(url,format=format)
    return result.read().strip()

def listDefinitions(**queryCriteria):
    result = getURL(samweb_connect.baseurl + '/definitions/list', queryCriteria)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def descDefinition(defname):
    result = getURL(samweb_connect.baseurl + '/definitions/name/' + defname + '/describe')
    return result.read().strip()

def createDefinition(defname, dims, user=None, group=None, description=None):

    params = { "defname": defname,
             "dims": dims,
             "user": user or samweb_connect.user,
             "group": group or samweb_connect.group,
             }
    if description:
        params["description"] = description

    result = postURL(samweb_connect.baseurl + '/definitions/create', params)
    return result.read().strip()

def deleteDefinition(defname):
    result = postURL(samweb_connect.baseurl + '/definitions/name/%s/delete' % defname, {})
    return result.read().strip()

