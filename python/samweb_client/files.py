
from samweb_client import json
from samweb_client.client import samweb_method
from samweb_client.http import quote

@samweb_method
def listFiles(samweb, dimensions=None, defname=None):
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/list' % defname)
    else:
        if len(dimensions) > 1024:
            method = samweb.postURL
        else:
            method = samweb.getURL
        result = method('/files/list', {'dims':dimensions})
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

@samweb_method
def parseDims(samweb, dimensions):
    """ For debugging only """
    if len(dimensions) > 1024:
        method = samweb.postURL
    else:
        method = samweb.getURL
    result = method('/files/list', {'dims':dimensions, "parse_only": "1"})
    return result.read().strip()

@samweb_method
def countFiles(samweb, dimensions=None, defname=None):
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/count' % defname)
    else:
        result = samweb.getURL('/files/count', {'dims':dimensions})
    return long(result.read().strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % quote(filenameorid)
    return path

@samweb_method
def locateFile(samweb, filenameorid):
    url = _make_file_path(filenameorid) + '/locations'
    result = samweb.getURL(url)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

@samweb_method
def _getMetadata(samweb, filenameorid, format=None):
    url = _make_file_path(filenameorid) + '/metadata'
    return samweb.getURL(url,format=format)

@samweb_method
def getMetadataDict(samweb, filenameorid):
    """ Return metadata as a dictionary """
    response = samweb._getMetadata(filenameorid, format='json')
    return json.load()

@samweb_method
def getMetadata(samweb, filenameorid, format=None):
    """ Return metadata as a string"""
    result = samweb._getMetadata(filenameorid, format=format)
    return result.read().strip()

@samweb_method
def declareFile(samweb, md=None, mdfile=None):
    """ Declare a new file """
    if md:
        body = json.dumps(md)
    else:
        body = mdfile.read()
    samweb.postURL('/files', body=body, content_type='application/json', secure=True)

@samweb_method
def listDefinitions(samweb, **queryCriteria):
    result = samweb.getURL('/definitions/list', queryCriteria)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

@samweb_method
def descDefinition(samweb, defname):
    result = samweb.getURL('/definitions/name/' + defname + '/describe')
    return result.read().strip()

@samweb_method
def createDefinition(samweb, defname, dims, user=None, group=None, description=None):

    params = { "defname": defname,
             "dims": dims,
             "user": user or samweb_connect.user,
             "group": group or samweb_connect.group,
             }
    if description:
        params["description"] = description

    result = samweb.postURL('/definitions/create', params)
    return result.read().strip()

@samweb_method
def deleteDefinition(samweb, defname):
    result = samweb.postURL('/definitions/name/%s/delete' % defname, {})
    return result.read().strip()

