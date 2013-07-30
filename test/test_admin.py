#! /usr/bin/env python

import unittest
import testbase
import samweb_client

class AdminTest(testbase.SamdevTest):
    def test_list_parameters(self):
        self.samweb.listParameters() 

    def test_listValues(self):
        values = self.samweb.listValues('data_tiers')
        assert 'raw' in (v['data_tier'] for v in values)

class AdminTest(testbase.MinervaDevTest):

    def test_add_parameter(self):
        self.assertRaises(samweb_client.exceptions.HTTPConflict, self.samweb.addParameter, 'Offline.tag','string')

    def test_listDataDisks(self):
        disks = self.samweb.listDataDisks()
        assert 'minerva_bluearc:/minerva/data' in ( d['mount_point'] for d in disks )

    def test_addDataDisk(self):
        self.assertRaises(samweb_client.exceptions.HTTPConflict, self.samweb.addDataDisk, 'minerva_bluearc:/minerva/data')

class TestAdminCommands(testbase.SAMWebCmdTest):

    def test_listParametersCmd(self):

        cmdline = '-e samdev list-parameters'
        self.check_cmd_return(cmdline)
        assert 'Offline.tag (string)' in self.stdout

    def test_addParameterCmd(self):
        cmdline = '-e minerva/dev add-parameter Offline.tag string'
        self.run_cmd(cmdline)
        assert "Parameter Offline.tag already exists" in self.stderr

    def test_listDataDisksCmd(self):
        cmdline = '-e minerva/dev list-data-disks'
        self.check_cmd_return(cmdline)
        assert 'minerva_bluearc:/minerva/data\n' in self.stdout

    def test_addDataDiskCmd(self):
        cmdline = '-e minerva/dev add-data-disk minerva_bluearc:/minerva/data'
        self.run_cmd(cmdline)
        assert "Disk for node 'minerva_bluearc', directory '/minerva/data' already exists" in self.stderr

    def test_listValuesCmd(self):
        cmdline = '-e samdev list-values data_tiers'
        self.run_cmd(cmdline)
        assert 'raw\n' in self.stdout

    def test_listValuesHelp(self):
        cmdline = '-e samdev list-values --help-categories'
        self.check_cmd_return(cmdline)

if __name__ == '__main__':
    unittest.main()
