
from samweb_client import *
from samweb_client.http import *

class NoMoreFiles(Exception):
  pass

def makeProject(defname, project, station=None, user=None, group=None):
    if not station: station = getstation()
    if not user: user = samweb_connect.user
    if not group: group = samweb_connect.group
    args = {'name':project,'station':station,"defname":defname,"username":user,"group":group}
    result = postURL('/startProject', args)
    return {'project':project,'dataset':defname,'projectURL':result.read().strip()}

def findProject(project, station=None):
    args = {'name':project}
    if station: args['station'] = station
    else: args['station'] = samweb_connect.station
    result = getURL('/findProject', args)
    return result.read().strip()

def makeProcess(projecturl, appfamily, appname, appversion, deliveryLocation=None, user=None, maxFiles=None):
    if not deliveryLocation:
        deliveryLocation = socket.getfqdn()
    if not user:
        user = samweb_connect.user

    args = { "appname":appname, "appversion":appversion, "deliverylocation" : deliveryLocation, "username":user }
    if appfamily:
        args["appfamily"] = appfamily
    if maxFiles:
        args["filelimit"] = maxFiles
    result = postURL(projecturl + '/establishProcess', args)
    return result.read().strip()

def getNextFile(processurl):
    url = processurl + '/getNextFile'
    while True:
        result= postURL(url, {})
        code = result.code
        if code == 202:
            retry_interval = 10
            retry_after = result.info().getheader('Retry-After')
            if retry_after:
                try:
                    retry_interval = int(retry_after)
                except ValueError: pass
            time.sleep(retry_interval)
        elif code == 204:
            raise NoMoreFiles()
        else:
            return result.read().strip()

def releaseFile(processurl, filename, status="ok"):
    args = { 'filename' : filename, 'status':status }
    postURL(processurl + '/releaseFile', args)

def stopProject(projecturl):
    args = { "force" : 1 }
    postURL(projecturl + "/endProject", args)

def projectSummary(projecturl):
    return getURL(projecturl + "/summary").read().strip()

"""

Example of running a project

def runProject():
  projectinfo =  makeProject("test2056")
  projecturl = projectinfo["projectURL"]
  print "Project name is %s" % projectinfo["project"]
  print "Project URL is %s" % projecturl

  cpid = makeProcess(projecturl)
  print "Consumer process id %s" %cpid
  processurl = projecturl + '/process/%s' % cpid

  while True:

    try:
      newfile = getNextFile(processurl)
      print "Got file %s" % newfile
    except NoMoreFiles:
      print "No more files available"
      break

    releaseFile(processurl, newfile)
    print "Released file %s" % newfile

  stopProject(projecturl)
  print "Project ended"
"""
