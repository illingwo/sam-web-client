#! /usr/bin/env python

import unittest
import testbase
import samweb_client
import samweb_cli

class AdminTest(testbase.MinervaDevTest):
    def test_list_parameters(self):
        self.samweb.listParameters() 

    def test_add_parameter(self):
        self.assertRaises(samweb_client.exceptions.HTTPConflict, self.samweb.addParameter, 'Offline.tag','string')

    def test_listDataDisks(self):
        disks = self.samweb.listDataDisks()
        assert 'minerva_bluearc:/minerva/data' in ( d['mount_point'] for d in disks )

    def test_addDataDisk(self):
        self.assertRaises(samweb_client.exceptions.HTTPConflict, self.samweb.addDataDisk, 'minerva_bluearc:/minerva/data')

class TestAdminCommands(testbase.SAMWebCmdTest):

    def test_listParametersCmd(self):

        cmdline = '-e minerva/dev list-parameters'
        self.trap_output()
        try:
            rval = samweb_cli.main(cmdline.split())
        finally:
            self.restore_output()
        assert 'Offline.tag (string)' in self.stdout
        assert rval is None

    def test_addParameterCmd(self):
        cmdline = '-e minerva/dev add-parameter Offline.tag string'
        self.trap_output()
        try:
            rval = samweb_cli.main(cmdline.split())
        finally:
            self.restore_output()
        assert "Parameter Offline.tag already exists" in self.stderr

    def test_listDataDisksCmd(self):
        cmdline = '-e minerva/dev list-data-disks'
        self.trap_output()
        try:
            rval = samweb_cli.main(cmdline.split())
        finally:
            self.restore_output()

        assert rval is None
        assert 'minerva_bluearc:/minerva/data\n' in self.stdout

    def test_addDataDiskCmd(self):
        cmdline = '-e minerva/dev add-data-disk minerva_bluearc:/minerva/data'
        self.trap_output()
        try:
            rval = samweb_cli.main(cmdline.split())
        finally:
            self.restore_output()
        assert "Disk for node 'minerva_bluearc', directory '/minerva/data' already exists" in self.stderr

if __name__ == '__main__':
    unittest.main()
