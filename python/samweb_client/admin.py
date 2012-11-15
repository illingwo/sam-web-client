
from samweb_client import json
from samweb_client.client import samweb_method
from samweb_client.http import quote

@samweb_method
def listApplications(samweb, **queryCriteria):
    result = samweb.getURL('/values/applications', queryCriteria, format='json')
    return json.load(result)

@samweb_method
def addApplication(samweb, family, name, version):
    samweb.postURL('/values/applications', {"family":family, "name":name, "version":version}, secure=True)
