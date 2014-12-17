#! /usr/bin/env python

import unittest
import testbase
import samweb_client

class AdminTest(testbase.SamdevTest):
    def test_list_parameters(self):
        self.samweb.listParameters() 

    def test_list_param_values(self):
        vals = self.samweb.listParameterValues('Quality.MINERvA')
        vals = self.samweb.listParameterValues('GLOBAL.true_floatval')
        vals = self.samweb.listParameterValues('GLOBAL.true_intval')
        self.assertRaises(samweb_client.exceptions.HTTPNotFound,
                self.samweb.listParameterValues,'this_parameter.does_not_exist')

    def test_listValues(self):
        values = self.samweb.listValues('data_tiers')
        assert 'raw' in (v['data_tier'] for v in values)

    def test_add_parameter(self):
        self.assertRaises(samweb_client.exceptions.HTTPConflict, self.samweb.addParameter, 'Offline.tag','string')

    def test_listDataDisks(self):
        disks = self.samweb.listDataDisks()
        assert 'samdevdata:/grid' in ( d['mount_point'] for d in disks )

    def test_addDataDisk(self):
        self.assertRaises(samweb_client.exceptions.HTTPConflict, self.samweb.addDataDisk, 'samdevdata:/grid')

    def test_listUsers(self):
        users = self.samweb.listUsers()
        assert 'sam' in users

    def test_userInfo(self):
        info = self.samweb.describeUser('sam')
        assert 'sam' == info["username"]

class TestAdminCommands(testbase.SAMWebCmdTest):

    def test_listParametersCmd(self):

        cmdline = '-e samdev list-parameters'
        self.check_cmd_return(cmdline)
        assert 'Offline.tag (string)' in self.stdout

        cmdline = '-e samdev list-parameters GLOBAL.true_floatval'
        self.check_cmd_return(cmdline)

    def test_addParameterCmd(self):
        cmdline = '-e samdev add-parameter Offline.tag string'
        self.run_cmd(cmdline)
        assert "Parameter Offline.tag already exists" in self.stderr

    def test_listDataDisksCmd(self):
        cmdline = '-e samdev list-data-disks'
        self.check_cmd_return(cmdline)
        assert 'samdevdata:/grid\n' in self.stdout

    def test_addDataDiskCmd(self):
        cmdline = '-e samdev add-data-disk samdevdata:/grid'
        self.run_cmd(cmdline)
        assert "Disk for node 'samdevdata', directory '/grid' already exists" in self.stderr

    def test_listValuesCmd(self):
        cmdline = '-e samdev list-values data_tiers'
        self.run_cmd(cmdline)
        assert 'raw\n' in self.stdout

    def test_listValuesHelp(self):
        cmdline = '-e samdev list-values --help-categories'
        self.check_cmd_return(cmdline)

if __name__ == '__main__':
    unittest.main()
