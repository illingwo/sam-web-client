
import time, os
from samweb_client import json, convert_from_unicode
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path
from exceptions import *

@samweb_method
def makeProjectName(samweb, description):
    """ Make a suitable project name from the provided string """
    description = description.replace(' ','_')
    import time
    now = time.strftime("%Y%m%d%H%M%S")
    return "%s_%s_%s" % ( samweb.user ,description, now)

@samweb_method
def startProject(samweb, project, defname, station=None, group=None, user=None):
    """ Start a project on a station
    arguments:
        project: project name
        defname: definition name
        station: station name (defaults to experiment name)
        group: group name (defaults to experiment name)
        user: user name (default is username from certificate)
    """

    if not station: station = samweb.get_station()
    if not group: group = samweb.group
    args = {'name':project,'station':station,"defname":defname,"group":group}
    if user: args["username"] = user
    result = samweb.postURL('/startProject', args, secure=True)
    projecturl = result.text.strip()
    if projecturl.startswith('https'):
        # prefer to use unencrypted project urls
        projecturl = samweb.findProject(project, station)
    return {'project':project,'dataset':defname,'projectURL':projecturl}

@samweb_method
def findProject(samweb, project, station=None):
    args = {'name':project}
    if station: args['station'] = station
    result = samweb.getURL('/findProject', args)
    return result.text.strip()

@samweb_method
def startProcess(samweb, projecturl, appfamily, appname, appversion, deliveryLocation=None, node=None,
        user=None, maxFiles=None, description=None, schemas=None):
    if not node:
        # default for the node is the local hostname
        import socket
        node = socket.getfqdn()
    if not user:
        user = samweb.user

    args = { "appname":appname, "appversion":appversion, "node" : node, "username":user }
    if appfamily:
        args["appfamily"] = appfamily
    if maxFiles:
        args["filelimit"] = maxFiles
    if deliveryLocation:
        args["deliverylocation"] = deliveryLocation
    if description:
        args["description"] = description
    if schemas:
        args["schemas"] = schemas
    result = samweb.postURL(projecturl + '/establishProcess', args)
    return result.text.strip()

@samweb_method
def makeProcessUrl(samweb, projecturl, processid):
    """ Make the process url from a project url and process id """
    if not '://' in projecturl:
        projecturl = samweb.findProject(projecturl)
    return projecturl + '/process/' + str(processid)

@samweb_method
def getNextFile(samweb, processurl):
    url = processurl + '/getNextFile'
    while True:
        result= samweb.postURL(url, data={})
        code = result.status_code
        data = result.text.strip()
        if code == 202:
            retry_interval = 10
            retry_after = result.headers.get('Retry-After')
            if retry_after:
                try:
                    retry_interval = int(retry_after)
                except ValueError: pass
            time.sleep(retry_interval)
        elif code == 204:
            raise NoMoreFiles()
        else:
            if 'application/json' in result.headers['Content-Type']:
                return result.json()
            lines = data.split('\n')
            output = { "url" : lines[0] }
            if len(lines) > 1: output["filename"] = lines[1]
            else:
                output["filename"] = os.path.basename(output["url"])
            return output

@samweb_method
def releaseFile(samweb, processurl, filename, status="ok"):
    args = { 'filename' : filename, 'status':status }
    return samweb.postURL(processurl + '/releaseFile', args).text.rstrip()

@samweb_method
def stopProject(samweb, projecturl):
    if not '://' in projecturl:
        projecturl = samweb.findProject(projecturl)
    args = { "force" : 1 }
    return samweb.postURL(projecturl + "/endProject", args).text.rstrip()

@samweb_method
def projectSummary(samweb, projecturl):
    if not '://' in projecturl:
        projecturl = '/projects/name/%s' % escape_url_path(projecturl)
    return convert_from_unicode(samweb.getURL(projecturl + "/summary").json())

@samweb_method
def projectSummaryText(samweb, projecturl):
    if not '://' in projecturl:
        projecturl = '/projects/name/%s' % escape_url_path(projecturl)
    return samweb.getURL(projecturl + "/summary", params=dict(format='plain')).text.rstrip()

@samweb_method
def projectRecoveryDimension(samweb, projectnameorurl, useFileStatus=None, useProcessStatus=None):
    """Get the dimensions to create a recovery dataset
    arguments:
        projectnameorurl : name or url of the project
        useFileStatus : use the status of the last file seen by a process (default unset)
        useProcessStatus : use the status of the process (default unset)
    """
    if not '://' in projectnameorurl:
        projectnameorurl = "/projects/name/%s" % escape_url_path(projectnameorurl)
    params = { "format" : "plain" }
    if useFileStatus is not None: params['useFiles'] = useFileStatus
    if useProcessStatus is not None: params['useProcess'] = useProcessStatus
    return convert_from_unicode(samweb.getURL(projectnameorurl + "/recovery_dimensions", params=params).text.rstrip())

@samweb_method
def setProcessStatus(samweb, status, projectnameorurl, processid=None, process_desc=None):
    """ Mark the final status of a process

    Either the processid or the process description must be provided. If the description is
    used it must be unique within the project

    arguments:
        status: completed or bad
        projectnameorurl: project name or url
        processid: process identifier
        process_desc: process description
    """
    if '://' not in projectnameorurl:
        url = '/projects/name/%s' % escape_url_path(projectnameorurl)
    else: url = projectnameorurl
    args = { "status" : status }
    if processid is not None:
        url += '/processes/%s' % processid
    elif process_desc is not None:
        url += '/process_description/%s' % escape_url_path(process_desc)
    else:
        # assume direct process url
        pass

    return samweb.putURL(url + "/status", args, secure=True).text.rstrip()

