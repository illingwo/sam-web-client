import unittest
import sys,os
import cStringIO

# fix up path to find the code
ourpath = sys.path[0]
sys.path.insert(0, os.path.normpath(os.path.join(ourpath, '../python')))
import samweb_client

class SAMWebTest(unittest.TestCase):

    def setUp(self):
        self.samweb = samweb_client.SAMWebClient(experiment=self.experiment)

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
            self.stdout = self._stdoutio.getvalue()
            self.stderr = self._stderrio.getvalue()
            self._stdoutio = None
            self._stderrio = None
        

