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

    def test_snapshot(self):
        output = self.samweb.takeSnapshot(defname)
        self.assertEquals(int(output),8130)

class TestDefinitionCommands(testbase.SAMWebCmdTest):

    def test_takeSnapshot(self):

        cmdline = '-e minerva/dev take-snapshot %s' % defname
        self.check_cmd_return(cmdline.split())
        assert "8130" in self.stdout


if __name__ == '__main__':
    unittest.main()
