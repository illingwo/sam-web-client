
from samweb_client.client import samweb_method
from exceptions import *

@samweb_method
def makeProject(samweb, defname, project, station=None, user=None, group=None):
    if not station: station = samweb.get_station()
    if not user: user = samweb.user
    if not group: group = samweb.group
    args = {'name':project,'station':station,"defname":defname,"username":user,"group":group}
    result = samweb.postURL('/startProject', args)
    return {'project':project,'dataset':defname,'projectURL':result.text.strip()}

@samweb_method
def findProject(samweb, project, station=None):
    args = {'name':project}
    if station: args['station'] = station
    else: args['station'] = samweb.get_station()
    result = samweb.getURL('/findProject', args)
    return result.text.strip()

@samweb_method
def makeProcess(samweb, projecturl, appfamily, appname, appversion, deliveryLocation=None, user=None, maxFiles=None):
    if not deliveryLocation:
        import socket
        deliveryLocation = socket.getfqdn()
    if not user:
        user = samweb.user

    args = { "appname":appname, "appversion":appversion, "deliverylocation" : deliveryLocation, "username":user }
    if appfamily:
        args["appfamily"] = appfamily
    if maxFiles:
        args["filelimit"] = maxFiles
    result = samweb.postURL(projecturl + '/establishProcess', args)
    return result.text.strip()

@samweb_method
def getNextFile(samweb, processurl):
    url = processurl + '/getNextFile'
    while True:
        result= samweb.postURL(url, data={})
        code = result.status_code
        data = result.text.strip()
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
            return data

@samweb_method
def releaseFile(samweb, processurl, filename, status="ok"):
    args = { 'filename' : filename, 'status':status }
    return samweb.postURL(processurl + '/releaseFile', args).text.rstrip()

@samweb_method
def stopProject(samweb, projecturl):
    args = { "force" : 1 }
    return samweb.postURL(projecturl + "/endProject", args).text.rstrip()

@samweb_method
def projectSummary(samweb, projecturl):
    return samweb.getURL(projecturl + "/summary").text.rstrip()

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
