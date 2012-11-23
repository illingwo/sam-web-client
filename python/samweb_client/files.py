
from samweb_client import json
from samweb_client.client import samweb_method
from samweb_client.http import quote

from itertools import ifilter

@samweb_method
def listFiles(samweb, dimensions=None, defname=None):
    """ list files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)
    
    returns:
      a generator producing file names (note that the network connection may not be closed
        until you have read the entire list)
    """

    # This can return a potentially long list, so don't preload the result
    # instead return a generator which reads it progressively
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/list' % defname, prefetch=False)
    else:
        if len(dimensions) > 1024:
            method = samweb.postURL
        else:
            method = samweb.getURL
        result = method('/files/list', {'dims':dimensions}, prefetch=False)

    return ifilter( None, (l.strip() for l in result.iter_lines()) )

@samweb_method
def parseDims(samweb, dimensions):
    """ For debugging only """
    if len(dimensions) > 1024:
        method = samweb.postURL
    else:
        method = samweb.getURL
    result = method('/files/list', {'dims':dimensions, "parse_only": "1"})
    return result.text.rstrip()

@samweb_method
def countFiles(samweb, dimensions=None, defname=None):
    """ return count of files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)"""
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/count' % defname)
    else:
        result = samweb.getURL('/files/count', {'dims':dimensions})
    return long(result.text.strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % quote(filenameorid)
    return path

@samweb_method
def locateFile(samweb, filenameorid):
    """ return locations for this file
    arguments:
        name or id of file
    """
    url = _make_file_path(filenameorid) + '/locations'
    result = samweb.getURL(url, format='json')
    return result.json

@samweb_method
def getMetadata(samweb, filenameorid):
    """ Return metadata as a dictionary 
    arguments:
        name or id of file
    """
    response = samweb.getURL(_make_file_path(filenameorid) + '/metadata', format='json')
    return response.json

@samweb_method
def getMetadataText(samweb, filenameorid, format=None):
    """ Return metadata as a string
    arguments:
        name or id of file
    """
    result = samweb.getURL(_make_file_path(filenameorid) + '/metadata', format=format)
    return result.text.rstrip()

@samweb_method
def declareFile(samweb, md=None, mdfile=None):
    """ Declare a new file
    arguments:
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    else:
        data = mdfile.read()
    return samweb.postURL('/files', data=data, content_type='application/json', secure=True).text

@samweb_method
def retireFile(samweb, filename):
    """ Retire a file:
    arguments:
        filename
    """
    url = _make_file_path(filename) + '/retired_date'
    return samweb.postURL(url, secure=True).text

@samweb_method
def listDefinitions(samweb, **queryCriteria):
    """ List definitions matching given query parameters:
    arguments:
        one or more key=string value pairs to pass to server
    """
    result = samweb.getURL('/definitions/list', queryCriteria, prefetch=False)
    return ifilter( None, (l.strip() for l in result.iter_lines()) )

def _descDefinitionURL(defname):
    return '/definitions/name/' + defname + '/describe'

@samweb_method
def descDefinitionDict(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = self.getURL(_descDefinitionURL(defname), format='json')
    return result.text

@samweb_method
def descDefinition(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = samweb.getURL(_descDefinitionURL(defname))
    return result.text.rstrip()

@samweb_method
def createDefinition(samweb, defname, dims, user=None, group=None, description=None):
    """ Create a new dataset definition
    arguments:
        definition name
        dimensions string
        user: username (default None)
        group: group name (default None)
        description: description of new definition (default None)
    """

    params = { "defname": defname,
             "dims": dims,
             "user": user or samweb.user,
             "group": group or samweb.group,
             }
    if description:
        params["description"] = description

    result = samweb.postURL('/definitions/create', params)
    return result.text.rstrip()

@samweb_method
def deleteDefinition(samweb, defname):
    """ Delete a dataset definition
    arguments:
        definition name

    (Definitions that have already been used cannot be deleted)
    """
    result = samweb.postURL('/definitions/name/%s/delete' % defname, {})
    return result.text.rstrip()

