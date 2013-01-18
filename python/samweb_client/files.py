
from samweb_client import json, convert_from_unicode
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path
from samweb_client.exceptions import *

from itertools import ifilter

try:
    from collections import namedtuple
except ImportError:
    def fileinfo(*args): return tuple(args)
else:
    fileinfo = namedtuple("fileinfo", ["file_name", "file_id", "file_size", "event_count"])

def _make_file_info(lines):
    for l in lines:
        values = l.split()
        if values:
            try:
                yield fileinfo( values[0], long(values[1]), long(values[2]), int(values[3])  )
            except Exception:
                raise Error("Error while decoding file list output from server")

@samweb_method
def listFiles(samweb, dimensions=None, defname=None, fileinfo=False):
    """ list files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)
      fileinfo: boolean; if True, return file_id, file_size, event_count 
    
    returns:
      a generator producing file names (note that the network connection may not be closed
        until you have read the entire list). If fileinfo is true, it will produce
        (file_name, file_id, file_size, event_count) tuples
    """

    # This can return a potentially long list, so don't preload the result
    # instead return a generator which reads it progressively
    params = {'format':'plain'}
    if fileinfo:
        params['fileinfo'] = 1
    kwargs = { 'params' : params, 'stream':True }
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/list' % escape_url_path(defname), **kwargs)
    else:
        if len(dimensions) > 1024:
            kwargs['data'] = {'dims':dimensions}
            method = samweb.postURL
        else:
            params['dims'] = dimensions
            method = samweb.getURL
        result = method('/files/list', **kwargs)
    if fileinfo:
        return _make_file_info(result.iter_lines())
    else:
        return ifilter( None, (l.strip() for l in result.iter_lines()) )

@samweb_method
def listFilesSummary(samweb, dimensions=None, defname=None):
    """ return summary of files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)"""
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/summary' % escape_url_path(defname))
    else:
        params = {}
        kwargs = {'params' : params }
        if len(dimensions) > 1024:
            kwargs['data'] = {'dims':dimensions}
            method = samweb.postURL
        else:
            params.update({'dims':dimensions})
            method = samweb.getURL
        result = samweb.getURL('/files/summary', **kwargs)
    return result.json()

@samweb_method
def parseDims(samweb, dimensions):
    """ For debugging only """
    params = { "parse_only": "1"}
    kwargs = {'params' : params }
    if len(dimensions) > 1024:
        kwargs['data'] = {'dims':dimensions}
        method = samweb.postURL
    else:
        params.update({'dims':dimensions})
        method = samweb.getURL
    result = method('/files/list', **kwargs)
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
        params = {}
        kwargs = {'params' : params }
        if len(dimensions) > 1024:
            kwargs['data'] = {'dims':dimensions}
            method = samweb.postURL
        else:
            params.update({'dims':dimensions})
            method = samweb.getURL
        result = samweb.getURL('/files/count', **kwargs)
    return long(result.text.strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % escape_url_path(filenameorid)
    return path

@samweb_method
def locateFile(samweb, filenameorid):
    """ return locations for this file
    arguments:
        name or id of file
    """
    url = _make_file_path(filenameorid) + '/locations'
    result = samweb.getURL(url)
    return convert_from_unicode(result.json())

@samweb_method
def addFileLocation(samweb, filenameorid, location):
    """ Add a location for a file
    arguments:
        name or id of file
        location
    """
    url = _make_file_path(filenameorid) + '/locations'
    data = { "add" : location }
    return samweb.postURL(url, data=data, secure=True)

@samweb_method
def removeFileLocation(samweb, filenameorid, location):
    """ Remove a location for a file
    arguments:
        name or id of file
        location
    """
    url = _make_file_path(filenameorid) + '/locations'
    data = { "remove" : location }
    return samweb.postURL(url, data=data, secure=True)

@samweb_method
def getMetadata(samweb, filenameorid):
    """ Return metadata as a dictionary 
    arguments:
        name or id of file
    """
    response = samweb.getURL(_make_file_path(filenameorid) + '/metadata')
    return convert_from_unicode(response.json())

@samweb_method
def getMetadataText(samweb, filenameorid, format=None):
    """ Return metadata as a string
    arguments:
        name or id of file
    """
    if format is None: format='plain'
    result = samweb.getURL(_make_file_path(filenameorid) + '/metadata', params={'format':format})
    return result.text.rstrip()

@samweb_method
def getFileLineage(samweb, lineagetype, filenameorid):
    """ Return lineage information for a file
    arguments:
        lineagetype (ie "parents", "children")
        name or id of file
    """
    result = samweb.getURL(_make_file_path(filenameorid) + '/lineage/' + escape_url_path(lineagetype))
    return convert_from_unicode(result.json())

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
def modifyFileMetadata(samweb, filename, md=None, mdfile=None):
    """ Modify file metadata
    arguments:
        filename
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    else:
        data = mdfile.read()
    url = _make_file_path(filename)
    return samweb.putURL(url + "/metadata", data=data, content_type='application/json', secure=True).text

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
    params = dict(queryCriteria)
    params['format'] = 'plain'
    result = samweb.getURL('/definitions/list', params, stream=True)
    return ifilter( None, (l.strip() for l in result.iter_lines()) )

def _descDefinitionURL(defname):
    return '/definitions/name/' + defname + '/describe'

@samweb_method
def descDefinitionDict(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = self.getURL(_descDefinitionURL(defname))
    return convert_from_unicode(result.json())

@samweb_method
def descDefinition(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = samweb.getURL(_descDefinitionURL(defname), {'format':'plain'})
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
    return result.json()

@samweb_method
def deleteDefinition(samweb, defname):
    """ Delete a dataset definition
    arguments:
        definition name

    (Definitions that have already been used cannot be deleted)
    """
    result = samweb.postURL('/definitions/name/%s/delete' % defname, {})
    return result.text.rstrip()

