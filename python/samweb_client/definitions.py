""" Client methods relating to definitions """

from samweb_client import json, convert_from_unicode
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path, escape_url_component
from samweb_client.exceptions import *

from itertools import ifilter

@samweb_method
def listDefinitions(samweb, stream=False, **queryCriteria):
    """ List definitions matching given query parameters:
    arguments:
        one or more key=string value pairs to pass to server
        stream: boolean: if True, the results will be returned progressively
    """
    params = dict(queryCriteria)
    params['format'] = 'plain'
    result = samweb.getURL('/definitions/list', params, stream=True)
    output = ifilter( None, (l.strip() for l in result.iter_lines()) )
    if stream: return output
    else: return list(output)

def _descDefinitionURL(defname):
    return '/definitions/name/' + defname + '/describe'

@samweb_method
def descDefinitionDict(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = samweb.getURL(_descDefinitionURL(defname))
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

    result = samweb.postURL('/definitions/create', params, secure=True)
    return result.json()

@samweb_method
def renameDefinition(samweb, defname, newname):
    """ Rename a dataset definition
        definition name
        new name for definition

    """
    samweb.putURL('/definitions/name/%s' % escape_url_component(defname), {'defname':newname}, secure=True)

@samweb_method
def deleteDefinition(samweb, defname):
    """ Delete a dataset definition
    arguments:
        definition name

    (Definitions that have already been used cannot be deleted)
    """
    result = samweb.postURL('/definitions/name/%s/delete?format=plain' % escape_url_component(defname), {}, secure=True)
    return result.text.rstrip()

@samweb_method
def takeSnapshot(samweb, defname, group=None):
    """ Create a snapshot for a existing definition
    arguments:
        definition name
        group: group (default experiment name)
    """
    if not group: group = samweb.group
    result = samweb.postURL('/definitions/name/%s/snapshot?format=plain' % escape_url_component(defname), {"group":group}, secure=True)
    return int(result.text.rstrip())

