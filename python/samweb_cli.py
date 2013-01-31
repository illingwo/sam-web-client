
from urllib import urlencode, quote, quote_plus
import urllib2,httplib
from urllib2 import urlopen, URLError, HTTPError, Request

import time,os, socket, sys, optparse, user, pwd

from samweb_client import *

class CmdError(Error): pass

class CmdBase(object):
    cmdgroup = None
    description = None
    args = None
    options = ()

    def __init__(self, samweb):
        self.samweb = samweb

    def addOptions(self, parser):
        pass

def _file_list_summary_str(summary):
    return "File count:\t%(file_count)s\nTotal size:\t%(total_file_size)s\nEvent count:\t%(total_event_count)s" % summary

def _file_list_str_gen(g, fileinfo):
    if fileinfo:
        for result in g:
            yield '\t'.join(str(e) for e in result)
    else:
        for result in g:
            yield result

class listFilesCmd(CmdBase):
    name = "list-files"
    options = [ ("parse-only", "Return parser output for these dimensions instead of evaluating them"),
                ("fileinfo", "Return additional information for each file"),
                ("summary", "Return a summary of the results instead of the full list"),
                ]

    description = "List files by dimensions query"
    cmdgroup = 'datafiles'
    args = "<dimensions query>"

    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        if options.parse_only:
            print self.samweb.parseDims(dims)
        elif options.summary:
            summary = self.samweb.listFilesSummary(dims)
            print _file_list_summary_str(summary)
        else:
            fileinfo = options.fileinfo
            for l in _file_list_str_gen(self.samweb.listFiles(dims,fileinfo=fileinfo), fileinfo):
                print l

class countFilesCmd(CmdBase):
    name = "count-files"
    description = "Count files by dimensions query"
    args = "<dimensions query>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        print self.samweb.countFiles(dims)

class locateFileCmd(CmdBase):
    name = "locate-file"
    description = "List file locations"
    args = "<file name>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("No filename specified")
        filename = args[0]
        for loc in ( l.get('location') or l['full_path'] for l in self.samweb.locateFile(filename)):
            print loc

class addFileLocationCmd(CmdBase):
    name='add-file-location'
    description="add a location for a file"
    args = "<file_name> <location>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Incorrect arguments")
        self.samweb.addFileLocation(args[0],args[1])

class removeFileLocationCmd(CmdBase):
    name='remove-file-location'
    description="remove a location for a file"
    args = "<file_name> <location>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Incorrect arguments")
        self.samweb.removeFileLocation(args[0],args[1])

class getMetadataCmd(CmdBase):
    name = 'get-metadata'
    description = "Get metadata for a file"
    args = "<file name>"
    cmdgroup = 'datafiles'

    def addOptions(self, parser):
        parser.add_option("--json", action="store_const", const="json", dest="format")

    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Invalid or no argument specified")
        print self.samweb.getMetadataText(args[0],format=options.format)

class fileLineage(CmdBase):
    name = 'file-lineage'
    description = 'Get lineage for a file'
    args = '<parents|children|ancestors|descendants|rawancestors> <file name>'
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Invalid or no argument specified")
        def _printLineage(results, indent=0):
            for r in results:
                print '%s%s' % (' '*indent, r['file_name'])
                for k in ('children','parents'):
                    if k in r:
                        _printLineage(r[k], indent+4)
                        break
        _printLineage(self.samweb.getFileLineage(args[0], args[1]))

class calculateChecksumCmd(CmdBase):
    name = 'file-checksum'
    description = 'Calculate a checksum for a file using the enstore algorithm (sometimes inaccurately described as a "CRC")'
    args = "<path to file> [<path to file> [...]]"
    cmdgroup = 'utility'

    def run(self, options, args):
        if not args:
            raise CmdError("No file paths provided")
        from samweb_client.utility import fileEnstoreChecksum
        if len(args) == 1:
            print json.dumps(fileEnstoreChecksum(args[0]))
        else:
            for a in args:
                try:
                    print "%s: %s" % (a, json.dumps(fileEnstoreChecksum(a)))
                except Error, ex:
                    print "%s: %s" % (a, ex)

class declareFileCmd(CmdBase):
    name = 'declare-file'
    description = "Declare a new file into the database"
    args = "<name of metadata file (json format)>"
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if not args:
            raise CmdError("No metadata files provided")
        for name in args:
            try:
                f = open(name)
            except IOError, ex:
                raise CmdError("Failed to open file: %s: %s" % (name, str(ex)))
            try:
                self.samweb.declareFile(mdfile=f)
            finally:
                f.close()

class modifyMetadataCmd(CmdBase):
    name = 'modify-metadata'
    description = "Modify metadata for an existing file"
    args = "<file name> <name of file containing metadata parameters to modify (json format)>"
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Invalid arguments provided")
        try:
            f = open(args[1])
        except IOError, ex:
            raise CmdError("Failed to open file: %s: %s" % (args[1], str(ex)))
        try:
            self.samweb.modifyFileMetadata(args[0], mdfile=f)
        finally:
            f.close()
        print "Metadata has been updated for file '%s'" % args[0]

class retireFileCmd(CmdBase):
    name = 'retire-file'
    description = "Mark a file as retired"
    args = "<file name> [file name] ..."
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if not args:
            raise CmdError("No file names provided")
        for filename in args:
            rval = 0
            try:
                self.samweb.retireFile(filename)
            except Error, ex:
                rval = 1
                print ex
            else:
                print "%s has been retired" % filename
        return rval

class listDefinitionsCmd(CmdBase):
    name = "list-definitions"
    options = [ "defname=", "user=", "group=", "after=", "before=" ]
    description = "List existing dataset definitions"
    cmdgroup = 'definitions'

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
        for l in self.samweb.listDefinitions(**args):
            print l

class descDefinitionCmd(CmdBase):
    name = "describe-definition"
    description = "Describe an existing dataset definition"
    args = "<dataset definition>"
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print self.samweb.descDefinition(args[0])

class listDefinitionFilesCmd(CmdBase):
    name = "list-definition-files"
    description = "List files in a dataset definition"
    args = "<dataset definition>"
    options = [ ("fileinfo", "Return additional information for each file"),
                ("summary", "Return a summary of the results instead of the full list"),
                ]
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        if options.summary:
            print _file_list_summary_str(self.samweb.listFilesSummary(defname=args[0]))
        else:
            fileinfo = options.fileinfo
            for l in _file_list_str_gen(self.samweb.listFiles(defname=args[0],fileinfo=fileinfo), fileinfo):
                print l

class countDefinitionFilesCmd(CmdBase):
    name = "count-definition-files"
    description = "Count number of files in a dataset definition"
    args = "<dataset definition>"
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print self.samweb.countFiles(defname=args[0])

class createDefinitionCmd(CmdBase):
    name = "create-definition"
    description = "Create a new dataset definition"
    args = "<new definition name> <dimensions>"
    options = [ "user=", "group=", "description=" ]
    cmdgroup = 'definitions'

    def run(self, options, args):
        try:
            defname = args[0]
        except IndexError:
            raise CmdError("No definition name specified")
        dims = ' '.join(args[1:])
        if not dims:
            raise CmdError("No dimensions specified")
        result = self.samweb.createDefinition(defname, dims, options.user, options.group, options.description)
        print "Dataset definition '%s' has been created with id %s" % (result["defname"], result["defid"])

class deleteDefinitionCmd(CmdBase):
    name = "delete-definition"
    description = "Delete an existing dataset definition"
    args = "<dataset definition>"
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        return self.samweb.deleteDefinition(args[0])

class startProjectCmd(CmdBase):
    name = "start-project"
    description = "Start a new project"
    options = [ "defname=", "group=", "station=" ]
    args = "[project name]"
    cmdgroup = 'projects'

    def run(self, options, args):
        if not options.defname:
            raise CmdError("Definition name not specified")
        defname = options.defname
        try:
            project = args[0]
        except IndexError:
            project = self.samweb.makeProjectName(defname)
        rval = self.samweb.startProject(project, defname=defname, station=options.station, group=options.group)
        print rval["projectURL"]

class ProjectCmdBase(CmdBase):

    options = [ "station=" ]
    cmdgroup = 'projects'

    def _getProjectUrl(self, options, args):
        try:
            projecturl = args.pop(0)
        except IndexError:
            raise CmdError("Must specify project name or url")

        if not '://' in projecturl:
            projecturl = self.samweb.findProject(projecturl, options.station)
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
        self.samweb.stopProject(projecturl)

class projectSummaryCmd(ProjectCmdBase):
    name = "project-summary"
    description = "Display the summary information for a project"
    args = "<project name>"

    def run(self, options, args):
        projecturl = self._getProjectUrl(options, args)
        print self.samweb.projectSummaryText(projecturl)

class startProcessCmd(CmdBase):
    name = "start-process"
    description = "Start a consumer process within a project"
    options = [ "appfamily=", "appname=", "appversion=", "delivery-location=", "url", "max-files=", "schemas=" ]
    args = "<project name or url>"
    cmdgroup = 'projects'

    def run(self, options, args):
        if not options.appname or not options.appversion:
            raise CmdError("Application name and version must be specified")
        try:
            projecturl = args[0]
        except IndexError:
            raise CmdError("Must specify project url")
        if not '://' in projecturl:
            projecturl = self.samweb.findProject(projecturl)

        kwargs = { "deliveryLocation":options.delivery_location }
        if options.max_files:
            kwargs['maxFiles'] = options.max_files
        if options.schemas:
            kwargs['schemas'] = options.schemas

        rval = self.samweb.startProcess(projecturl, options.appfamily, options.appname,
                options.appversion, **kwargs)
        if options.url:
            print self.samweb.makeProcessUrl(projecturl, rval)
        else:
            print rval

class ProcessCmd(CmdBase):
    args = "(<process url> | <project url> <process id>)"
    cmdgroup = 'projects'

    def makeProcessUrl(self, args):
        # note that this modifies args
        try:
            url = args.pop(0)
            if args:
                processid = args.pop(0)
                url = self.samweb.makeProcessUrl(url, processid)
        except IndexError: url = None
        if not url:
            raise CmdError("Must specify either process url or project url and process id")
        return url
    
class getNextFileCmd(ProcessCmd):
    name = "get-next-file"
    description = "Get the next file from a process"
    
    def run(self, options, args):
        processurl = self.makeProcessUrl(args)
        try:
            rval = self.samweb.getNextFile(processurl)
            if not rval["url"].endswith(rval["filename"]):
                print "%s\t%s" % (rval["url"], rval["filename"])
            else:
                print rval["url"]
        except NoMoreFiles:
            return 0

class releaseFileCmd(ProcessCmd):
    name = "release-file"
    description = "Release a file from a process"
    args = "(<process url> | <project url> <process id>) <file name>"
    options = [ "status=" ]

    def run(self, options, args):
        try:
            filename = args.pop()
        except IndexError:
            raise CmdError("No project and file name arguments")
        processurl = self.makeProcessUrl(args)
        #if len(args) != 1:
        #    raise CmdError("Must specify filename")
        status = options.status
        if not status: status = 'ok'
        self.samweb.releaseFile(processurl, filename)

class listValuesCmd(CmdBase):

    name = 'list-values'
    description = "List values from the database"
    cmdgroup = 'admin'
    args = "<data_tiers|file_types|file_formats|groups|...>"
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Invalid arguments")

        vtype = args[0]
        for i in self.samweb.listValues(vtype):
            if isinstance(i, basestring): line = i
            elif isinstance(i, dict):
                # if the dict contains a key which is a prefix of the vtype
                # then print that value (ie for data_tiers print the value of the data_tier key)
                # else join the values with tabs
                for k in i:
                    if vtype.startswith(k): line = i[k]
                    break
                else:
                    line = '\t'.join(str(i[k]) for k in sorted(i.iterkeys()))
            else: line = '\t'.join(str(v) for v in i)
            print line

class addValueCmd(CmdBase):
    name = 'add-value'
    description = "Add value to the database"
    cmdgroup = 'admin'
    args = "<data_tiers|file_types|file_formats|groups|...> <value> [<value> [<value> [...]]]"
    def run(self, options, args):
        if not args:
            raise CmdError("Invalid arguments")
        vtype = args.pop(0)
        self.samweb.addValue(vtype, *args)
        print "Added value to %s" % vtype

class listApplicationsCmd(CmdBase):
    name = 'list-applications'
    description = "List defined applications"
    options = [ "family=", "name=", "version=" ]
    cmdgroup = 'admin'
    def run(self, options, args):
        queryparams = {}
        if options.family: queryparams["family"] = options.family
        if options.name: queryparams["name"] = options.name
        if options.version: queryparams["version"] = options.version
        for app in self.samweb.listApplications(**queryparams):
            print "%(family)s\t%(name)s\t%(version)s" % app

class addApplicationCmd(CmdBase):
    name = 'add-application'
    description = "Add a new application to the database"
    args = "<family> <name> <version>"
    cmdgroup = 'admin'
    def run(self, options, args):
    
        try:
            family, name, version = args
        except ValueError:
            raise CmdError("Invalid arguments: must specify family, name, and version")
        self.samweb.addApplication(family, name, version)

class listUsersCmd(CmdBase):
    name = 'list-users'
    description = "List registered users"
    cmdgroup = 'admin'
    def run(self, options, args):
        for user in self.samweb.listUsers():
            print user

class describeUserCmd(CmdBase):
    name = 'describe-user'
    description = 'List user information'
    cmdgroup = 'admin'
    args = "<username>"
    def run(self, options, args):
        try:
            username, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify username")

        print self.samweb.describeUserText(username)

class addUserCmd(CmdBase):
    name = 'add-user'
    description = "Add new user"
    cmdgroup = 'admin'
    options = ( 'first-name=', 'last-name=', 'email=', 'uid=', 'groups=' )
    args = "<username>"
    def run(self, options, args):
        try:
            username, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify username")

        if options.groups:
            groups = options.groups.split(',')
        else:
            groups = None

        self.samweb.addUser(username, firstname=options.first_name, lastname=options.last_name, email=options.email, uid=options.uid, groups=groups)

class modifyUserCmd(CmdBase):
    name = 'modify-user'
    description = "Modify user"
    cmdgroup = 'admin'
    options = ( 'email=', 'groups=', 'addgroups=', 'status=' )
    args = "<username>"
    def run(self, options, args):
        try:
            username, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify username")

        args = {}
        if options.email: args['email'] = options.email
        if options.status: args['status'] = options.status
        if options.groups:
            args['groups'] = options.groups.split(',')
        if options.addgroups:
            args['addgroups'] = options.addgroups.split(',')
        self.samweb.modifyUser(username, **args)

commands = {
       }
command_groups = {}

group_descriptions = {
        "datafiles": "Data file commands",
        "definitions" : "Definition commands",
        "projects" : "Project commands",
        "admin": "Admin commands",
        }

# add all commands that define a name attribute to the list
for o in locals().values():
    try:
        if issubclass(o, CmdBase) and hasattr(o, 'name') and o.name not in commands:
            commands[o.name] = o
            command_groups.setdefault(o.cmdgroup,[]).append(o.name)
    except TypeError: pass

def command_list():
    s = ["Available commands:",]
    for g in command_groups:
        if g is None: group_desc = 'Uncategorized'
        else: group_desc = group_descriptions.get(g, g)
        s.append("  %s:" % group_desc)
        for c in sorted(command_groups[g]):
            s.append("    %s" % c)
        s.append('')
    return '\n'.join(s)

def _list_commands(option, opt, value, parser):
    print command_list()
    parser.exit()

def main():

    usage = "%prog [base options] <command> [command options] ..."
    parser = optparse.OptionParser(usage=usage)
    parser.disable_interspersed_args()
    parser.add_option('--help-commands', action="callback", callback=_list_commands, help="list available commands")
    base_options = optparse.OptionGroup(parser, "Base options")
    base_options.add_option('-e','--experiment',dest='experiment', help='use this experiment server. If not set, defaults to $SAM_EXPERIMENT.')
    base_options.add_option('--dev', action="store_true", dest='devel', default=False, help='use development server')
    base_options.add_option('-s','--secure', action="store_true", dest='secure', default=False, help='always use secure (SSL) mode')
    base_options.add_option('--cert', dest='cert', help='x509 certificate for authentication. If not specified, use $X509_USER_PROXY, $X509_USER_CERT/$X509_USER_KEY or standard grid proxy location')
    base_options.add_option('--key', dest='key', help='x509 key for authentication (defaults to same as certificate)')
    base_options.add_option('-v','--verbose', action="store_true", dest='verbose', default=False, help="Verbose mode")
    parser.add_option_group(base_options)

    (options, args) = parser.parse_args(sys.argv[1:])
    if not args:
        print>>sys.stderr, "No command specified"
        parser.print_help(sys.stderr)
        print>>sys.stderr, '\n',command_list()
        return 1

    try:
        cmd = commands[args[0]]
    except KeyError:
        print>>sys.stderr, "Unknown command %s" % args[0]
        parser.print_help(sys.stderr)
        print>>sys.stderr, '\n',command_list()
        return 1

    # set up client
    samweb = SAMWebClient()
    command = cmd(samweb)

    usage = "%%prog [base options] %s [command options]" % (args[0])
    if command.args: usage += ' ' + command.args
    parser.usage = usage
    if command.description: parser.description = command.description
    parser.enable_interspersed_args()
    cmd_options = optparse.OptionGroup(parser, "%s options" % args[0])

    for opt in command.options:
        attribs = {}
        if isinstance(opt, (tuple, list)):
            optname, description = opt
        else:
            optname, description = opt, None
        if optname.endswith('='):
            # value
            optname = optname[:-1]
        else:
            # flag
            attribs.update({"action":"store_true", "default":False})
        if description:
            attribs['help'] = description
        attribs["dest"] = optname.replace('-','_')

        cmd_options.add_option('--%s' % optname, **attribs)

    command.addOptions(cmd_options)
    parser.add_option_group(cmd_options)

    (cmdoptions, args) = parser.parse_args(args[1:])


    # configure https settings
    cert = options.cert or cmdoptions.cert
    key = options.key or cmdoptions.key or cert
    if cert: samweb.set_certificate(cert, key)

    if options.secure or cmdoptions.secure:
        samweb.secure = True

    # configure the url
    experiment = options.experiment or cmdoptions.experiment
    if experiment is not None:
        samweb.experiment = experiment

    if options.devel or cmdoptions.devel:
        samweb.devel = True

    # verbose mode
    samweb.verbose = (options.verbose or cmdoptions.verbose)

    try:
        return command.run(cmdoptions, args)
    except CmdError, ex:
        print>>sys.stderr, str(ex)
        parser.print_help()
        return 2
    except Error, ex:
        print>>sys.stderr, str(ex)
        return 1

