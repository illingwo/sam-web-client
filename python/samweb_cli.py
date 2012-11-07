
from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time,os, socket, sys, optparse, user, pwd


from samweb_client.http import *
from samweb_client import *

from samweb_client.files import *
from samweb_client.projects import *

class CmdError(Error): pass

class CmdBase(object):
    secure = False # mark commands that require authentication
    description = None
    args = None
    options = ()
    def addOptions(self, parser):
        pass

class listFilesCmd(CmdBase):
    name = "list-files"
    options = [ "parse-only" ]
    description = "List files by dimensions query"
    args = "<dimensions query>"

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
    description = "Count files by dimensions query"
    args = "<dimensions query>"
    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        print countFiles(dims)

class locateFileCmd(CmdBase):
    name = "locate-file"
    description = "List file locations"
    args = "<file name>"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("No filename specified")
        filename = args[0]
        print '\n'.join(locateFile(filename))

class getMetadataCmd(CmdBase):
    name = 'get-metadata'
    description = "Get metadata for a file"
    args = "<file name>"

    def addOptions(self, parser):
        parser.add_option("--json", action="store_const", const="json", dest="format")

    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Invalid or no argument specified")
        print getMetadata(args[0],format=options.format)

class declareFileCmd(CmdBase):
    name = 'declare-file'
    description = "Declare a new file into the database"
    args = "<name of metadata file>"

    secure = True

    def run(self, options, args):
        if not args:
            raise CmdError("No metadata files provided")
        for name in args:
            try:
                f = open(name)
            except IOError, ex:
                raise CmdError("Failed to open file: %s: " % (name, str(ex)))
            declareFile(mdfile=f)

class listDefinitionsCmd(CmdBase):
    name = "list-definitions"
    options = [ "defname=", "user=", "group=", "after=", "before=" ]
    description = "List existing dataset definitions"

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
    description = "Describe an existing dataset definition"
    args = "<dataset definition>"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print descDefinition(args[0])

class listDefinitionFilesCmd(CmdBase):
    name = "list-definition-files"
    description = "List files in a dataset definition"
    args = "<dataset definition>"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        for filename in listFiles(defname=args[0]):
            print filename

class countDefinitionFilesCmd(CmdBase):
    name = "count-definition-files"
    description = "Count number of files in a dataset definition"
    args = "<dataset definition>"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print countFiles(defname=args[0])

class createDefinitionCmd(CmdBase):
    name = "create-definition"
    description = "Create a new dataset definition"
    args = "<new definition name> <dimensions>"
    options = [ "user=", "group=", "description=" ]

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
    description = "Delete an existing dataset definition"
    args = "<dataset definition>"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        return deleteDefinition(args[0])

class startProjectCmd(CmdBase):
    name = "start-project"
    description = "Start a new project"
    options = [ "defname=", "group=", "station=" ]
    args = "[project name]"

    def run(self, options, args):
        if not options.defname:
            raise CmdError("Definition name not specified")
        defname = options.defname
        try:
            project = args[0]
        except IndexError:
            now = time.strftime("%Y%m%d%H%M%S")
            project = "%s_%s_%s" % ( os.environ["USER"],defname, now)
        rval = makeProject(defname, project, station=options.station, group=options.group)
        print rval["projectURL"]

class ProjectCmdBase(CmdBase):

    options = [ "station=" ]

    def _getProjectUrl(self, options, args):
        try:
            projecturl = args.pop(0)
        except IndexError:
            raise CmdError("Must specify project name or url")

        if not '://' in projecturl:
            projecturl = findProject(projecturl, options.station)
        return projecturl

class findProjectCmd(ProjectCmdBase):
    name = "find-project"
    description = "Return the URL for a running project"
    args = "<project name>"

    def run(self, options, args):
        rval = self._getProjectUrl(options, args)
        print rval

class stopProjectCmd(ProjectCmdBase):
    name = "stop-project"
    description = "Stop a running project"
    args = "<project name>"

    def run(self, options, args):
        
        projecturl = self._getProjectUrl(options, args)
        stopProject(projecturl)

class projectSummaryCmd(ProjectCmdBase):
    name = "project-summary"
    description = "Display the summary information for a project"
    args = "<project name>"

    def run(self, options, args):
        projecturl = self._getProjectUrl(options, args)
        print projectSummary(projecturl)

class startProcessCmd(CmdBase):
    name = "start-process"
    description = "Start a consumer process within a project"
    options = [ "appfamily=", "appname=", "appversion=", "delivery-location=", "url", "max-files=" ]
    args = "<project name or url>"

    def run(self, options, args):
        if not options.appname or not options.appversion:
            raise CmdError("Application name and version must be specified")
        try:
            projecturl = args[0]
        except IndexError:
            raise CmdError("Must specify project url")
        if not '://' in projecturl:
            projecturl = findProject(projecturl)

        kwargs = { "deliveryLocation":options.delivery_location }
        if options.maxfiles:
            kwargs['maxFiles'] = options.max_files

        rval = makeProcess(projecturl, options.appfamily, options.appname, 
                options.appversion, **kwargs)
        if options.url:
            print '%s/process/%s' % (projecturl, rval)
        else:
            print rval

class ProcessCmd(CmdBase):
    args = "(<process url> | <project url> <process id>)"

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
    description = "Get the next file from a process"
    
    def run(self, options, args):
        processurl = self.makeProcessUrl(args)
        try:
            rval = getNextFile(processurl)
            print rval
        except NoMoreFiles:
            return 0

class releaseFileCmd(ProcessCmd):
    name = "release-file"
    description = "Release a file from a process"
    options = [ "status=" ]

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

    usage = "%prog [base options] <command> [command options] ..."
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
    usage = "%%prog [base options] %s [command options]" % (args[0])
    if command.args: usage += ' ' + command.args
    parser.usage = usage
    if command.description: parser.description = command.description
    parser.enable_interspersed_args()
    cmd_options = optparse.OptionGroup(parser, "%s options" % args[0])

    for opt in command.options:
        attribs = {}
        if opt.endswith('='):
            # value
            opt = opt[:-1]
        else:
            # flag
            attribs.update({"action":"store_true", "default":False})
        attribs["dest"] = opt.replace('-','_')

        cmd_options.add_option('--%s' % opt, **attribs)

    command.addOptions(cmd_options)
    parser.add_option_group(cmd_options)

    (cmdoptions, args) = parser.parse_args(args[1:])

    # configure https settings
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
        use_client_certificate(cert, key)

    if command.secure or options.secure or cmdoptions.secure:
        if not (cert and key):
            print>>sys.stderr, ("In secure mode certificate and key must be available, either from the --cert and --key\n"
                "options, the X509_USER_PROXY envvar, or in /tmp/x509up_u%d" % os.getuid())
        samweb_connect.secure = True

    else:
        samweb_connect.secure = False

    # configure the url
    experiment = options.experiment or cmdoptions.experiment
    if experiment is not None:
        samweb_connect.experiment = experiment

    if options.devel or cmdoptions.devel:
        samweb_connect.devel = True

    try:
        return command.run(cmdoptions, args)
    except CmdError, ex:
        print>>sys.stderr, str(ex)
        parser.print_help()
        return 2
    except Error, ex:
        print>>sys.stderr, str(ex)
        return 1

