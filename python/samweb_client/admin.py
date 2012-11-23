
from samweb_client import json, Error
from samweb_client.client import samweb_method
from samweb_client.http import quote, quote_plus

@samweb_method
def listApplications(samweb, **queryCriteria):
    result = samweb.getURL('/values/applications', queryCriteria, format='json')
    return json.load(result)

@samweb_method
def addApplication(samweb, family, name, version):
    return samweb.postURL('/values/applications', {"family":family, "name":name, "version":version}, secure=True).text.rstrip()

@samweb_method
def listUsers(samweb):
    result = samweb.getURL('/users', format='json')
    return result.json

@samweb_method
def _describeUser(samweb, username, format=None):
    return samweb.getURL('/users/name/%s' % quote(username), format=format)

@samweb_method
def describeUser(samweb, username):
    result = samweb._describeUser(username, format='json')
    return result.json

@samweb_method
def describeUserText(samweb, username):
    result = samweb._describeUser(username)
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
    return samweb.postURL('/users', body=json.dumps(userdata), content_type='application/json', secure=True).text.rstrip()

@samweb_method
def modifyUser(samweb, username, **args):
    return samweb.postURL('/users/name/%s' % quote(username), body=json.dumps(args), content_type='application/json', secure=True).text.rstrip()
