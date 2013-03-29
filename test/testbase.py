import unittest
import sys,os
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

