#! /usr/bin/python

from urllib import urlencode
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError

import time,os, socket, sys, optparse, user, pwd

# handler to cope with client certificate auth
class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, cert, key):
        urllib2.HTTPSHandler.__init__(self)
        self.cert = cert
        self.key = key

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

experiment = os.environ.get('SAM_EXPERIMENT')
baseurl = os.environ.get('SAM_WEB_BASE_URL')
default_group = os.environ.get('SAM_GROUP')
default_station = os.environ.get('SAM_STATION')

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
            if isinstance(x.reason, socket.sslerror):
                raise Error("SSL error: %s" % x.reason)
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

def listFiles(dimensions=None, defname=None):
    if defname is not None:
        result, _ = getURL(baseurl + '/definitions/name/%s/files/list' % defname)
    else:
        if len(dimensions) > 1024:
            method = postURL
        else:
            method = getURL
        result, _ = method(baseurl + '/files/list', {'dims':dimensions})
    return [ l.strip() for l in result.split('\n') if l ]

def parseDims(dimensions):
    """ For debugging only """
    if len(dimensions) > 1024:
        method = postURL
    else:
        method = getURL
    result, _ = method(baseurl + '/files/list', {'dims':dimensions, "parse_only": "1"})
    return result.strip()

def countFiles(dimensions=None, defname=None):
    if defname is not None:
        result, _ = getURL(baseurl + '/definitions/name/%s/files/count' % defname)
    else:
        result, _ = getURL(baseurl + '/files/count', {'dims':dimensions})
    return long(result.strip())

def listDefinitions(**queryCriteria):
    result, _ = getURL(baseurl + '/definitions/list', queryCriteria)
    return [ l.strip() for l in result.split('\n') if l ]

def descDefinition(defname):
    result, _ = getURL(baseurl + '/definitions/name/' + defname + '/describe')
    return result.strip()

def createDefinition(defname, dims, user=None, group=None, description=None):

    params = { "defname": defname,
             "dims": dims,
             "user": user or getuser(),
             "group": group or getgroup(),
             }
    if description:
        params["description"] = description

    result, _ = postURL(baseurl + '/definitions/create', params)
    return result.strip()

def deleteDefinition(defname):
    result, _ = postURL(baseurl + '/definitions/name/%s/delete' % defname, {})
    return result.strip()

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

class CmdBase(object):
    def addOptions(self, parser):
        pass

class listFilesCmd(CmdBase):

    def addOptions(self, parser):
        parser.add_option("--parse-only", action="store_true", dest="parse_only", default=False)

    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        if options.parse_only:
            print parseDims(dims)
        else:
            for filename in listFiles(dims):
                print filename

class countFilesCmd(CmdBase):
    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        print countFiles(dims)

class listDefinitionsCmd(CmdBase):
    def addOptions(self, parser):
        parser.add_option("--defname", dest="defname")
        parser.add_option("--user", dest="user")
        parser.add_option("--group", dest="group")
        parser.add_option("--after", dest="after")
        parser.add_option("--before", dest="before")

    def run(self, options, args):
        args = {}
        if options.defname:
            args['defname'] = options.defname
        if options.user:
            args['user'] = options.user
        if options.group:
            args['group'] = options.group
        if options.after:
            args['after'] = options.after
        if options.before:
            args['before'] = options.before
        for l in listDefinitions(**args):
            print l

class descDefinitionCmd(CmdBase):

    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print descDefinition(args[0])

class listDefinitionFilesCmd(CmdBase):
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        for filename in listFiles(defname=args[0]):
            print filename

class countDefinitionFilesCmd(CmdBase):
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print countFiles(defname=args[0])

class createDefinitionCmd(CmdBase):
    def addOptions(self, parser):
        parser.add_option("--defname", dest="defname")
        parser.add_option("--user", dest="user")
        parser.add_option("--group", dest="group")
        parser.add_option("--description", dest="description")

    def run(self, options, args):
        dims = ' '.join(args)
        if not dims:
            raise CmdError("No dimensions specified")
        if not options.defname:
            raise CmdError("Must specify defname")
        return createDefinition(options.defname, dims, options.user, options.group, options.description)

class deleteDefinitionCmd(CmdBase):
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        return deleteDefinition(args[0])

class startProjectCmd(CmdBase):
    def addOptions(self, parser):
        parser.add_option("--project", dest="project")
        parser.add_option("--defname", dest="defname")
        parser.add_option("--group", dest="group")
        #parser.add_option("--url", action="store_true", dest="url")

    def run(self, options, args):
        if not options.defname:
            raise CmdError("Definition name not specified")
        defname = options.defname
        project = options.project
        if not options.project:
            now = time.strftime("%Y%m%d%H%M%S")
            project = "%s_%s_%s" % ( os.environ["USER"],defname, now)
        rval = makeProject(defname, project, group=options.group)
        print rval["projectURL"]

class findProjectCmd(CmdBase):
    def addOptions(self, parser):
        parser.add_option("--project", dest="project")

    def run(self, options, args):
        if not options.project:
            raise CmdError("Project name must be specified")

        rval = findProject(options.project)
        print rval

class stopProjectCmd(CmdBase):
    def addOptions(self, parser):

        parser.add_option("--project", dest="project")
        parser.add_option("--projecturl", dest="projecturl")

    def run(self, options, args):
        
        projecturl = None
        if options.projecturl:
            projecturl = options.projecturl
        elif options.project:
            projecturl = findProject(options.project)
        else:
            raise CmdError("Must specify project name or url")

        stopProject(projecturl)

class startProcessCmd(CmdBase):
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

class ProcessCmd(CmdBase):

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

commands = {
        "list-files": listFilesCmd,
        "count-files": countFilesCmd,
        "list-definitions": listDefinitionsCmd,
        "describe-definition": descDefinitionCmd,
        "list-definition-files": listDefinitionFilesCmd,
        "count-definition-files": countDefinitionFilesCmd,
        "create-definition": createDefinitionCmd,
        "delete-definition": deleteDefinitionCmd,
        "start-project": startProjectCmd,
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
    parser.add_option('-e','--experiment',dest='experiment')
    parser.add_option('-d','--devel', action="store_true", dest='devel', default=False)
    parser.add_option('-s','--secure', action="store_true", dest='secure', default=True)
    parser.add_option('--cert', dest='cert')
    parser.add_option('--key', dest='key')

    command.addOptions(parser)

    (options, args) = parser.parse_args(sys.argv[2:])

    global experiment, baseurl, default_group, default_station

    # configure https settings
    if options.secure or baseurl and baseurl.startswith('https'):
        cert = options.cert
        key = options.key or options.cert
        if not cert:
            cert = key = os.environ.get('X509_USER_PROXY')
            if not cert:
                # look in standard place for cert
                proxypath = '/tmp/x509up_u%d' % os.getuid()
                if os.path.exists(proxypath):
                    cert = key = proxypath
        if cert and key:
            opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, key) )
            urllib2.install_opener(opener)
        else:
            print>>sys.stderr, ("In secure mode certificate and key must be available, either from the --cert and --key\n"
                "options, the X509_USER_PROXY envvar, or in /tmp/x509up_u%d" % os.getuid())

    # configure the url
    experiment = experiment or options.experiment
    if experiment is not None:
        if baseurl is None:
            if options.devel:
                path = "/sam/%s/dev/api" % experiment
            else:
                path = "/sam/%s/api" % experiment
            if options.secure:
                baseurl = "https://samweb.fnal.gov:8483%s" % path
            else:
                baseurl = "http://samweb.fnal.gov:8480%s" % path
        if default_group is None:
            default_group = experiment
        if default_station is None:
            default_station = experiment
    elif not baseurl:
        print>>sys.stderr, "Either the experiment must be specified in environment or command line, or the base url must be set"
        parser.print_help()
        return 2

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
