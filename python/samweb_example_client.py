#! /usr/bin/python

from urllib import urlencode
from urllib2 import urlopen, URLError, HTTPError

import time,os, socket, sys, optparse, user, pwd

baseurl = "http://samweb.fnal.gov:20004/sam/minerva/api"
default_group = 'minerva'
default_station = 'minerva'

class Error(Exception):
  pass

class NoMoreFiles(Exception):
  pass

maxtimeout=60*30
maxretryinterval = 60

def postURL(url, args):
    return _doURL(url, action='POST', args=args)

def getURL(url, args=None):
    return _doURL(url,action='GET',args=args)

def _doURL(url, action='GET', args=None):
    if action =='POST':
        if args is None: args = {}
        params = urlencode(args)
    else:
        params = None
        if args is not None:
            if '?' not in url: url += '?'
            url += urlencode(args)
    tmout = time.time() + maxtimeout
    retryinterval = 1
    while True:
        try:
            if params is not None:
                remote = urlopen(url, params)
            else:
                remote = urlopen(url)
        except HTTPError, x:
            #python 2.4 treats 201 and up as errors instead of normal return codes
            if 201 <= x.code <= 299:
                return (x.read(), x.code)
            errmsg = x.read().strip()
            # retry server errors (excluding internal errors)
            if x.code > 500 and time.time() < tmout:
                print "Error %s" % errmsg
            else:
                if action == 'POST':
                    msg = "POST to %s, args = %s" % ( url, args)
                else:
                    msg = "GET of %s" % (url, )
                raise Error("%s, failed with %s: %s" % (msg, str(x), errmsg))
        except URLError, x:
            print 'URL %s not responding' % url
        else:
            return (remote.read(), remote.code)

        time.sleep(retryinterval)
        retryinterval*=2
        if retryinterval > maxretryinterval:
            retryinterval = maxretryinterval

def getuser():
    return pwd.getpwuid(os.getuid()).pw_name

def getgroup():
    return default_group

def getstation():
    return default_station

def makeProject(defname, project, station=None, user=None, group=None):
    if not station: station = getstation()
    if not user: user = getuser()
    if not group: group = getgroup()
    args = {'name':project,'station':station,"defname":defname,"username":user,"group":group}
    result, _ = postURL(baseurl + '/startProject', args)
    return {'project':project,'dataset':defname,'projectURL':result.strip()}

def findProject(project, station=None):
    if not station: station = getstation()
    args = {'name':project,'station':station}
    result, _ = getURL(baseurl + '/findProject', args)
    return result.strip()

def makeProcess(projecturl, appfamily, appname, appversion, deliveryLocation=None, user=None, maxFiles=None):
    if not deliveryLocation:
        deliveryLocation = socket.getfqdn()
    if not user:
        user = getuser()

    args = { "appname":appname, "appversion":appversion, "deliverylocation" : deliveryLocation, "username":user }
    if appfamily:
        args["appfamily"] = appfamily
    if maxFiles:
        args["filelimit"] = maxFiles
    cid, _ = postURL(projecturl + '/establishProcess', args)
    return cid.strip()

def getNextFile(processurl):
  url = processurl + '/getNextFile'
  while True:
      remote, code = postURL(url, {})
      if code == 202:
        time.sleep(10)
      elif code == 204:
        raise NoMoreFiles()
      else:
        return remote.strip()

def releaseFile(processurl, filename, status="ok"):
  args = { 'filename' : filename, 'status':status }
  postURL(processurl + '/releaseFile', args)

def stopProject(projecturl):
  args = { "force" : 1 }
  postURL(projecturl + "/endProject", args)

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

class CmdError(Error): pass

class startProjectCmd(object):
    def addOptions(self, parser):
        parser.add_option("--project", dest="project")
        parser.add_option("--defname", dest="defname")
        parser.add_option("--group", dest="group")
        parser.add_option("--url", action="store_true", dest="url")

    def run(self, options, args):
        if not options.defname:
            raise CmdError("Definition name not specified")
        defname = options.defname
        project = options.project
        if not options.project:
            now = time.strftime("%Y%m%d%H%M%S")
            project = "%s_%s_%s" % ( os.environ["USER"],defname, now)
        rval = makeProject(defname, project, group=options.group)
        if options.url:
            print rval["projectURL"]
        else:
            print rval["project"]

class findProjectCmd(object):
    def addOptions(self, parser):
        parser.add_option("--project", dest="project")

    def run(self, options, args):
        if not options.project:
            raise CmdError("Project name must be specified")

        rval = findProject(options.project)
        print rval

class stopProjectCmd(object):
    def addOptions(self, parser):

        parser.add_option("--project", dest="project")
        parser.add_option("--projecturl", dest="projecturl")

    def run(self, options, args):
        
        projecturl = None
        if options.projecturl:
            projecturl = options.projecturl
        if not projecturl:
            raise CmdError("Must specify project url")

        stopProject(projecturl)

class startProcessCmd(object):
    def addOptions(self, parser):
        parser.add_option("--project", dest="project")
        parser.add_option("--projecturl", dest="projecturl")
        parser.add_option("--appfamily", dest="appfamily")
        parser.add_option("--appname", dest="appname")
        parser.add_option("--appversion", dest="appversion")
        parser.add_option("--delivery-location", dest="deliverylocation")
        parser.add_option("--url", action="store_true", dest="url")
        parser.add_option("--max-files", dest="maxfiles")

    def run(self, options, args):
        if not options.appname or not options.appversion:
            raise CmdError("Application name and version must be specified")
        projecturl = None
        if options.projecturl:
            projecturl = options.projecturl
        if not projecturl:
            raise CmdError("Must specify project url")

        kwargs = { "deliveryLocation":options.deliverylocation }
        if options.maxfiles:
            kwargs['maxFiles'] = options.maxfiles

        rval = makeProcess(projecturl, options.appfamily, options.appname, 
                options.appversion, **kwargs)
        if options.url:
            print '%s/process/%s' % (projecturl, rval)
        else:
            print rval

class ProcessCmd(object):

    def addOptions(self, parser):
        parser.add_option("--project", dest="project")
        parser.add_option("--projecturl", dest="projecturl")
        parser.add_option("--processid", dest="processid")
        parser.add_option("--processurl", dest="processurl")

    def makeProcessUrl(self, options):

        processurl = None
        if options.processurl:
            processurl = options.processurl
        elif options.projecturl and options.processid:
            processurl = options.projecturl + '/process/%s' % options.processid
        if not processurl:
            raise CmdError("Must specify either process url or project url and process id")
        return processurl
    
class getNextFileCmd(ProcessCmd):
    
    def run(self, options, args):
        processurl = self.makeProcessUrl(options)
        try:
            rval = getNextFile(processurl)
            print rval
        except NoMoreFiles:
            return 10

class releaseFileCmd(ProcessCmd):
    def addOptions(self, parser):
        ProcessCmd.addOptions(self, parser)
        parser.add_option("--file", dest="filename")
        parser.add_option("--status", dest="status")

    def run(self, options, args):
        processurl = self.makeProcessUrl(options)
        if not options.filename:
            raise CmdError("Must specify filename")
        status = options.status
        if not status: status = 'ok'
        releaseFile(processurl, options.filename)

commands = { "start-project": startProjectCmd,
             "find-project": findProjectCmd,
         "stop-project": stopProjectCmd,
         "start-process": startProcessCmd,
         "get-next-file": getNextFileCmd,
         "release-file": releaseFileCmd,
       }

def coreusage():
    print "Available commands: "
    for c in commands:
        print "    %s" % c
    return 1

def main():


    if len(sys.argv) < 2:
        print>>sys.stderr, "No command specified"
        return coreusage()

    try:
        cmd = commands[sys.argv[1]]
    except KeyError:
        print>>sys.stderr, "Unknown command %s" % sys.argv[1]
        return coreusage()

    command = cmd()
    usage = "usage: %%prog %s [options] arg" % sys.argv[1]
    parser = optparse.OptionParser(usage)
    command.addOptions(parser)

    (options, args) = parser.parse_args(sys.argv[2:])

    try:
        return command.run(options, args)
    except CmdError, ex:
        print>>sys.stderr, str(ex)
        parser.print_help()
        return 2
    except Error, ex:
        print>>sys.stderr, str(ex)
        return 1

if __name__ == '__main__':

    sys.exit(main())

