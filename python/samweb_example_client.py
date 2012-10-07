#! /usr/bin/python

from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time,os, socket, sys, optparse, user, pwd

# handler to cope with client certificate auth
# Note that this does not verify the server certificate
# Since the main purpose is for the server to authenticate
# the client. However, you should be cautious about sending
# sensitive infomation (not that SAM deals with that)
# as there's no protection against man-in-the-middle attacks
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

def getURL(url, args=None,format=None):
    return _doURL(url,action='GET',args=args,format=format)

def _doURL(url, action='GET', args=None, format=None):
    headers = {}
    if format=='json':
        headers['Accept'] = 'application/json'
    if action =='POST':
        if args is None: args = {}
        params = urlencode(args)
    else:
        params = None
        if args is not None:
            if '?' not in url: url += '?'
            else: url += '&'
            url += urlencode(args)
    tmout = time.time() + maxtimeout
    retryinterval = 1

    request = Request(url, data=params, headers=headers)
    while True:
        try:
            remote = urlopen(request)
        except HTTPError, x:
            #python 2.4 treats 201 and up as errors instead of normal return codes
            if 201 <= x.code <= 299:
                return x
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
            return remote

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
        result = getURL(baseurl + '/definitions/name/%s/files/list' % defname)
    else:
        if len(dimensions) > 1024:
            method = postURL
        else:
            method = getURL
        result = method(baseurl + '/files/list', {'dims':dimensions})
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def parseDims(dimensions):
    """ For debugging only """
    if len(dimensions) > 1024:
        method = postURL
    else:
        method = getURL
    result = method(baseurl + '/files/list', {'dims':dimensions, "parse_only": "1"})
    return result.read().strip()

def countFiles(dimensions=None, defname=None):
    if defname is not None:
        result = getURL(baseurl + '/definitions/name/%s/files/count' % defname)
    else:
        result = getURL(baseurl + '/files/count', {'dims':dimensions})
    return long(result.read().strip())

def _make_file_path(filenameorid):
    try:
        fileid = long(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % quote(filenameorid)
    return path

def locateFile(filenameorid):
    url = baseurl + _make_file_path(filenameorid) + '/locations'
    result = getURL(url)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def getMetadata(filenameorid, format=None):
    url = baseurl + _make_file_path(filenameorid) + '/metadata'
    result = getURL(url,format=format)
    return result.read().strip()

def listDefinitions(**queryCriteria):
    result = getURL(baseurl + '/definitions/list', queryCriteria)
    return filter( lambda l: l, (l.strip() for l in result.readlines()) )

def descDefinition(defname):
    result = getURL(baseurl + '/definitions/name/' + defname + '/describe')
    return result.read().strip()

def createDefinition(defname, dims, user=None, group=None, description=None):

    params = { "defname": defname,
             "dims": dims,
             "user": user or getuser(),
             "group": group or getgroup(),
             }
    if description:
        params["description"] = description

    result = postURL(baseurl + '/definitions/create', params)
    return result.read().strip()

def deleteDefinition(defname):
    result = postURL(baseurl + '/definitions/name/%s/delete' % defname, {})
    return result.read().strip()

def makeProject(defname, project, station=None, user=None, group=None):
    if not station: station = getstation()
    if not user: user = getuser()
    if not group: group = getgroup()
    args = {'name':project,'station':station,"defname":defname,"username":user,"group":group}
    result = postURL(baseurl + '/startProject', args)
    return {'project':project,'dataset':defname,'projectURL':result.read().strip()}

def findProject(project, station=None):
    if not station: station = getstation()
    args = {'name':project,'station':station}
    result = getURL(baseurl + '/findProject', args)
    return result.read().strip()

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
    name = "list-files"

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
    name = "count-files"
    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        print countFiles(dims)

class locateFileCmd(CmdBase):
    name = "locate-file"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("No filename specified")
        filename = args[0]
        print '\n'.join(locateFile(filename))

class getMetadataCmd(CmdBase):
    name = 'get-metadata'

    def addOptions(self, parser):
        parser.add_option("--json", action="store_const", const="json", dest="format")

    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Invalid or no argument specified")
        print getMetadata(args[0],format=options.format)

class listDefinitionsCmd(CmdBase):
    name = "list-definitions"
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

    name = "describe-definition"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print descDefinition(args[0])

class listDefinitionFilesCmd(CmdBase):
    name = "list-definition-files"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        for filename in listFiles(defname=args[0]):
            print filename

class countDefinitionFilesCmd(CmdBase):
    name = "count-definition-files"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print countFiles(defname=args[0])

class createDefinitionCmd(CmdBase):
    name = "create-definition"
    def addOptions(self, parser):
        parser.add_option("--user", dest="user")
        parser.add_option("--group", dest="group")
        parser.add_option("--description", dest="description")

    def run(self, options, args):
        try:
            defname = args[0]
        except IndexError:
            raise CmdError("No definition name specified")
        dims = ' '.join(args[1:])
        if not dims:
            raise CmdError("No dimensions specified")
        return createDefinition(defname, dims, options.user, options.group, options.description)

class deleteDefinitionCmd(CmdBase):
    name = "delete-definition"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        return deleteDefinition(args[0])

class startProjectCmd(CmdBase):
    name = "start-project"
    def addOptions(self, parser):
        parser.add_option("--defname", dest="defname")
        parser.add_option("--group", dest="group")
        #parser.add_option("--url", action="store_true", dest="url")

    def run(self, options, args):
        if not options.defname:
            raise CmdError("Definition name not specified")
        defname = options.defname
        try:
            project = args[0]
        except IndexError:
            now = time.strftime("%Y%m%d%H%M%S")
            project = "%s_%s_%s" % ( os.environ["USER"],defname, now)
        rval = makeProject(defname, project, group=options.group)
        print rval["projectURL"]

class findProjectCmd(CmdBase):
    name = "find-project"
    def addOptions(self, parser):
        parser.add_option("--project", dest="project")

    def run(self, options, args):
        if not options.project:
            raise CmdError("Project name must be specified")

        rval = findProject(options.project)
        print rval

class stopProjectCmd(CmdBase):
    name = "stop-project"

    def run(self, options, args):
        
        try:
            projecturl = args[0]
        except IndexError:
            raise CmdError("Must specify project name or url")

        if not '://' in projecturl:
            projecturl = findProject(projecturl)

        stopProject(projecturl)

class startProcessCmd(CmdBase):
    name = "start-process"
    def addOptions(self, parser):
        parser.add_option("--appfamily", dest="appfamily")
        parser.add_option("--appname", dest="appname")
        parser.add_option("--appversion", dest="appversion")
        parser.add_option("--delivery-location", dest="deliverylocation")
        parser.add_option("--url", action="store_true", dest="url")
        parser.add_option("--max-files", dest="maxfiles")

    def run(self, options, args):
        if not options.appname or not options.appversion:
            raise CmdError("Application name and version must be specified")
        try:
            projecturl = args[0]
        except IndexError:
            raise CmdError("Must specify project url")
        if not '://' in projecturl:
            projecturl = findProject(projecturl)

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

    def makeProcessUrl(self, args):
        # note that this modifies args
        if len(args) == 1:
            processurl = args.pop(0)
        elif len(args) >= 2:
            projecturl = args.pop(0)
            if not '://' in projecturl:
                projecturl = findProject(projecturl)
            processurl = projecturl + '/process/%s' % args.pop(0)
        if not processurl:
            raise CmdError("Must specify either process url or project url and process id")
        return processurl
    
class getNextFileCmd(ProcessCmd):
    name = "get-next-file"
    
    def run(self, options, args):
        processurl = self.makeProcessUrl(args)
        try:
            rval = getNextFile(processurl)
            print rval
        except NoMoreFiles:
            return 0

class releaseFileCmd(ProcessCmd):
    name = "release-file"
    def addOptions(self, parser):
        ProcessCmd.addOptions(self, parser)
        parser.add_option("--status", dest="status")

    def run(self, options, args):
        processurl = self.makeProcessUrl(args)
        if len(args) != 1:
            raise CmdError("Must specify filename")
        filename = args[0]
        status = options.status
        if not status: status = 'ok'
        releaseFile(processurl, filename)

commands = {
       }

# add all commands that define a name attribute to the list
for o in locals().values():
    try:
        if issubclass(o, CmdBase) and hasattr(o, 'name') and o.name not in commands:
            commands[o.name] = o
    except TypeError: pass

def coreusage():
    print "Available commands: "
    for c in commands:
        print "    %s" % c
    return 1

def main():

    usage = "usage: %prog [base options] <command> [command options] ..."
    parser = optparse.OptionParser(usage=usage)
    parser.disable_interspersed_args()
    base_options = optparse.OptionGroup(parser, "Base options")
    base_options.add_option('-e','--experiment',dest='experiment')
    base_options.add_option('--dev', action="store_true", dest='devel', default=False)
    base_options.add_option('-s','--secure', action="store_true", dest='secure', default=False)
    base_options.add_option('--cert', dest='cert')
    base_options.add_option('--key', dest='key')
    parser.add_option_group(base_options)

    (options, args) = parser.parse_args(sys.argv[1:])
    if not args:
        print>>sys.stderr, "No command specified"
        return coreusage()

    try:
        cmd = commands[args[0]]
    except KeyError:
        print>>sys.stderr, "Unknown command %s" % args[0]
        return coreusage()
    command = cmd()
    usage = "usage: %%prog [base options] %s [command options] ..." % args[0]
    parser.usage = usage
    parser.enable_interspersed_args()
    cmd_options = optparse.OptionGroup(parser, "%s options" % args[0])

    command.addOptions(cmd_options)
    parser.add_option_group(cmd_options)

    (cmdoptions, args) = parser.parse_args(args[1:])

    global experiment, baseurl, default_group, default_station

    # configure https settings
    if options.secure or cmdoptions.secure or baseurl and baseurl.startswith('https'):
        cert = options.cert or cmdoptions.cert
        key = options.key or cmdoptions.key or cert
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
    experiment = experiment or options.experiment or cmdoptions.experiment
    if experiment is not None:
        if baseurl is None:
            if options.devel or cmdoptions.devel and not experiment.endswith('/dev'):
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
        return command.run(cmdoptions, args)
    except CmdError, ex:
        print>>sys.stderr, str(ex)
        parser.print_help()
        return 2
    except Error, ex:
        print>>sys.stderr, str(ex)
        return 1

if __name__ == '__main__':

    sys.exit(main())

