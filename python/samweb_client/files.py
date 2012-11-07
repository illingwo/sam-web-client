
from samweb_client import *
from samweb_client.http import *

def listFiles(dimensions=None, defname=None):
    if defname is not None:
        result = getURL('/definitions/name/%s/files/list' % defname)
    else:
        if len(dimensions) > 1024:
            method = postURL
        else:
            method = getURL
        result = method('/files/list', {'dims':dimensions})
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def parseDims(dimensions):
    """ For debugging only """
    if len(dimensions) > 1024:
        method = postURL
    else:
        method = getURL
    result = method('/files/list', {'dims':dimensions, "parse_only": "1"})
    return result.read().strip()

def countFiles(dimensions=None, defname=None):
    if defname is not None:
        result = getURL('/definitions/name/%s/files/count' % defname)
    else:
        result = getURL('/files/count', {'dims':dimensions})
    return long(result.read().strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % quote(filenameorid)
    return path

def locateFile(filenameorid):
    url = _make_file_path(filenameorid) + '/locations'
    result = getURL(url)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def _getMetadata(filenameorid, format=None):
    url = _make_file_path(filenameorid) + '/metadata'
    return getURL(url,format=format)

def getMetadataDict(filenameorid):
    """ Return metadata as a dictionary """
    response = _getMetadata(filenameorid, format='json')
    return json.load()

def getMetadata(filenameorid, format=None):
    """ Return metadata as a string"""
    result = _getMetadata(filenameorid, format=format)
    return result.read().strip()

def declareFile(md=None, mdfile=None):
    """ Declare a new file """
    if md:
        body = json.dumps(md)
    else:
        body = mdfile.read()
    postURL('/files', body=body, content_type='application/json')

def listDefinitions(**queryCriteria):
    result = getURL('/definitions/list', queryCriteria)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def descDefinition(defname):
    result = getURL('/definitions/name/' + defname + '/describe')
    return result.read().strip()

def createDefinition(defname, dims, user=None, group=None, description=None):

    params = { "defname": defname,
             "dims": dims,
             "user": user or samweb_connect.user,
             "group": group or samweb_connect.group,
             }
    if description:
        params["description"] = description

    result = postURL('/definitions/create', params)
    return result.read().strip()

def deleteDefinition(defname):
    result = postURL('/definitions/name/%s/delete' % defname, {})
    return result.read().strip()

