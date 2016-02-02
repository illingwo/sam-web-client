
from samweb_client import json, convert_from_unicode
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path, escape_url_component
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
def getAvailableDimensions(samweb):
    """ List the available dimensions """
    result = samweb.getURL('/files/list/dimensions?format=json&descriptions=1')
    return convert_from_unicode(result.json())

@samweb_method
def _callDimensions(samweb, url, dimensions, params=None,stream=False):
    """ Call the requested method with a dimensions string, 
    automatically switching from GET to POST as needed """
    if params is None: params = {}
    else: params = params.copy()
    kwargs = {'params':params, 'stream':stream}
    if len(dimensions) > 1024:
        kwargs['data'] = {'dims':dimensions}
        method = samweb.postURL
    else:
        params['dims'] = dimensions
        method = samweb.getURL
    return method(url, **kwargs)

@samweb_method
def listFiles(samweb, dimensions=None, defname=None, fileinfo=False, stream=False):
    """ list files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)
      fileinfo: boolean; if True, return file_id, file_size, event_count 
      stream: boolean: if True the return value will be a generator and the results will
                       be progressively returned to the client. Note that this keeps the network connection open until
                       all the response has been read. (default False)
    
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
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/list' % escape_url_component(defname), params=params,stream=True)
    else:
        result = samweb._callDimensions('/files/list', dimensions, params, stream=True)
    if fileinfo:
        output = _make_file_info(result.iter_lines())
    else:
        output = ifilter( None, (l.strip() for l in result.iter_lines()) )
    if stream: return output
    else: return list(output)

@samweb_method
def listFilesSummary(samweb, dimensions=None, defname=None):
    """ return summary of files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)"""
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/summary' % escape_url_component(defname))
    else:
        result = samweb._callDimensions('/files/summary', dimensions)
    return convert_from_unicode(result.json())

@samweb_method
def parseDims(samweb, dimensions, mode=False):
    """ For debugging only """
    if not mode:
        params = { "parse_only": "1"}
        result = samweb._callDimensions('/files/list', dimensions, params)
    else:
        params = { "diagnostics" : "1" }
        if mode=='count':
            path = '/files/count'
        elif mode=='summary':
            path = '/files/summary'
        elif mode=='fileinfo':
            params['fileinfo']="1"
            path = '/files/list'
        else:
            path = '/files/list'
        result = samweb._callDimensions(path, dimensions, params)
    return result.text.rstrip()

@samweb_method
def countFiles(samweb, dimensions=None, defname=None):
    """ return count of files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)"""
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/count' % escape_url_component(defname))
    else:
        result = samweb._callDimensions('/files/count', dimensions)
    return long(result.text.strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % escape_url_component(filenameorid)
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
def locateFiles(samweb, filenameorids):
    """ return the locations of multiple files
    The return value is a dictionary of { file_name_or_id : location } pairs
    """

    file_names = []
    file_ids = []
    for filenameorid in filenameorids:
        try:
            file_ids.append(long(filenameorid))
        except ValueError:
            file_names.append(filenameorid)

    params = {}
    if file_names: params["file_name"] = file_names
    if file_ids: params["file_id"] = file_ids
    response = samweb.getURL("/files/locations", params=params)
    return convert_from_unicode(response.json())

@samweb_method
def addFileLocation(samweb, filenameorid, location):
    """ Add a location for a file
    arguments:
        name or id of file
        location
    """
    url = _make_file_path(filenameorid) + '/locations'
    data = { "add" : location }
    return samweb.postURL(url, data=data, secure=True, role='*')

@samweb_method
def removeFileLocation(samweb, filenameorid, location):
    """ Remove a location for a file
    arguments:
        name or id of file
        location
    """
    url = _make_file_path(filenameorid) + '/locations'
    data = { "remove" : location }
    return samweb.postURL(url, data=data, secure=True, role='*')

@samweb_method
def getFileAccessUrls(samweb, filenameorid, schema, locationfilter=None):
    """ return urls by which this file may be accessed
    arguments:
        name or id of file
        schema
        locationfilter (default None)
    """
    params = { "schema": schema }
    if locationfilter:
        params["location"] = locationfilter
    response = samweb.getURL(_make_file_path(filenameorid) + '/locations/url', params=params)
    return convert_from_unicode(response.json())

@samweb_method
def getMetadata(samweb, filenameorid, locations=False):
    """ Return metadata as a dictionary 
    arguments:
        name or id of file
        locations: if True, also return file locations
    """
    params = {}
    if locations: params['locations'] = True
    response = samweb.getURL(_make_file_path(filenameorid) + '/metadata', params=params)
    return convert_from_unicode(response.json())

@samweb_method
def getMultipleMetadata(samweb, filenameorids, locations=False, asJSON=False):
    """ Return a list of metadata dictionaries
    (This method does not return an error if a
    file does not exist; instead it returns no
    result for that file)
    arguments:
        list of file names or ids
        locations: if True include location information
        asJSON: return the undecoded JSON string instead of python objects
    """
    file_names = []
    file_ids = []
    for filenameorid in filenameorids:
        try:
            file_ids.append(long(filenameorid))
        except ValueError:
            file_names.append(filenameorid)
    
    params = {}
    if file_names: params["file_name"] = file_names
    if file_ids: params["file_id"] = file_ids
    if locations: params["locations"] = 1
    # use post because the lists cab be large
    response = samweb.postURL("/files/metadata", data=params)
    if asJSON:
        return response.text.rstrip()
    else:
        return convert_from_unicode(response.json())

@samweb_method
def getMetadataText(samweb, filenameorid, format=None, locations=False):
    """ Return metadata as a string
    arguments:
        name or id of file
    """
    if format is None: format='plain'
    params = {'format':format}
    if locations: params['locations'] = 1
    result = samweb.getURL(_make_file_path(filenameorid) + '/metadata', params=params)
    return result.text.rstrip()

@samweb_method
def getFileLineage(samweb, lineagetype, filenameorid):
    """ Return lineage information for a file
    arguments:
        lineagetype (ie "parents", "children")
        name or id of file
    """
    result = samweb.getURL(_make_file_path(filenameorid) + '/lineage/' + escape_url_component(lineagetype))
    return convert_from_unicode(result.json())

@samweb_method
def validateFileMetadata(samweb, md=None, mdfile=None):
    """ Check the metadata for validity
    arguments:
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    elif mdfile:
        data = mdfile.read()
    else:
        raise ArgumentError('Must specify metadata dictionary or file object')
    return samweb.postURL('/files/validate_metadata', data=data, content_type='application/json').text

@samweb_method
def declareFile(samweb, md=None, mdfile=None):
    """ Declare a new file
    arguments:
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    elif mdfile:
        data = mdfile.read()
    else:
        raise ArgumentError('Must specify metadata dictionary or file object')
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
    return samweb.putURL(url + "/metadata", data=data, content_type='application/json', secure=True, role='*').text

@samweb_method
def retireFile(samweb, filename):
    """ Retire a file:
    arguments:
        filename
    """
    url = _make_file_path(filename) + '/retired_date'
    return samweb.postURL(url, secure=True, role='*').text

