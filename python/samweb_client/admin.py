
from samweb_client import json, convert_from_unicode, Error
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path, SAMWebHTTPError

@samweb_method
def listApplications(samweb, **queryCriteria):
    result = samweb.getURL('/values/applications', queryCriteria)
    return convert_from_unicode(result.json())

@samweb_method
def addApplication(samweb, family, name, version):
    return samweb.postURL('/values/applications', {"family":family, "name":name, "version":version}, secure=True).text.rstrip()

@samweb_method
def listUsers(samweb):
    result = samweb.getURL('/users')
    return convert_from_unicode(result.json())

@samweb_method
def _describeUser(samweb, username, format=None):
    params = {}
    if format:
        params['format'] = format
    return samweb.getURL('/users/name/%s' % escape_url_path(username), params)

@samweb_method
def describeUser(samweb, username):
    result = samweb._describeUser(username)
    return convert_from_unicode(result.json())

@samweb_method
def describeUserText(samweb, username):
    result = samweb._describeUser(username, format='plain')
    return result.text.rstrip()

@samweb_method
def addUser(samweb, username, firstname=None, lastname=None, email=None, uid=None, groups=None):

    userdata = { 'username' : username }
    if firstname: userdata['first_name'] = firstname
    if lastname: userdata['last_name'] = lastname
    if email: userdata['email'] = email
    if uid is not None:
        try:
            uid = int(uid)
        except ValueError:
            raise Error("Invalid value for uid: %s" % uid)
        else:
            userdata['uid'] = uid
    if groups:
        userdata["groups"] = groups
    return samweb.postURL('/users', data=json.dumps(userdata), content_type='application/json', secure=True).text.rstrip()

@samweb_method
def modifyUser(samweb, username, **args):
    return samweb.postURL('/users/name/%s' % escape_url_path(username), data=json.dumps(args), content_type='application/json', secure=True).text.rstrip()

@samweb_method
def listValues(samweb, vtype):
    """ list values from database. This method tries to be generic, so vtype is
    passed directly to the web server
    arguments:
        vtype: string with values to return (ie data_tiers, groups)
    """
    try:
        return samweb.getURL('/values/%s' % escape_url_path(vtype)).json()
    except SAMWebHTTPError, ex:
        if ex.code == 404:
            raise Error("Unknown value type '%s'" % vtype)
        else: raise

