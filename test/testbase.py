import unittest
import sys,os
import cStringIO

# drop some things from the environment
for e in ('SAM_GROUP', 'SAM_EXPERIMENT', 'SAM_WEB_BASE_URL', 'SAM_WEB_SSL_BASE_URL'):
    try: del os.environ[e]
    except KeyError: pass

# fix up path to find the code
ourpath = sys.path[0]
sys.path.insert(0, os.path.normpath(os.path.join(ourpath, '../python')))
import samweb_client

class SAMWebTest(unittest.TestCase):

    def setUp(self):
        self.samweb = samweb_client.SAMWebClient(experiment=self.experiment)

class SamdevTest(SAMWebTest):
    experiment = 'samdev'

class MinervaDevTest(SAMWebTest):
    experiment = 'minerva/dev'

class DZeroDevTest(SAMWebTest):
    experiment = 'dzero/dev'

class SAMWebCmdTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self._stdoutio = None
        self._stderrio = None
        self.stdout = None
        self.stderr = None
        self.originalstdout = sys.stdout
        self.originalstderr = sys.stderr

    def trap_output(self):
        self._stdoutio = cStringIO.StringIO()
        self._stderrio = cStringIO.StringIO()
        sys.stdout = self._stdoutio
        sys.stderr = self._stderrio

    def restore_output(self):
        if self._stdoutio is not None:
            sys.stdout = self.originalstdout
            sys.stderr = self.originalstderr
            self.stdout = self._stdoutio.getvalue() or ""
            self.stderr = self._stderrio.getvalue() or ""
            self._stdoutio = None
            self._stderrio = None
        
    def run_cmd(self, cmdline):
        import samweb_cli
        if isinstance(cmdline, str):
            cmdline = cmdline.split()
        self.trap_output()
        try:
            return samweb_cli.main(cmdline)
        finally:
            self.restore_output()

    def check_cmd_return(self, cmdline):
        try:
            rval = self.run_cmd(cmdline)
        except SystemExit as ex:
            print self.stdout
            print self.stderr
            rval = ex
        assert rval is None or rval == 0

