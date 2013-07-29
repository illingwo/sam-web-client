#! /usr/bin/env python
import testbase
import unittest
import samweb_client
import samweb_cli
import time

defname = 'one_enstore_file_test'

class TestDefinitionMinerva(testbase.MinervaDevTest):

    def test_descDefinition_DefNotFound(self):
        fake_def_name = 'doesnotexist_%d' % time.time()
        self.assertRaises(samweb_client.exceptions.DefinitionNotFound, self.samweb.descDefinition, fake_def_name)
        self.assertRaises(samweb_client.exceptions.DefinitionNotFound, self.samweb.descDefinitionDict, fake_def_name)

    def test_descDefinition(self):
        output = self.samweb.descDefinition(defname)
        assert defname in output
        d = self.samweb.descDefinitionDict(defname)
        assert d['defname'] == defname

if __name__ == '__main__':
    unittest.main()
