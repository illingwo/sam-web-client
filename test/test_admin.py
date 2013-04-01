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


if __name__ == '__main__':
    unittest.main()
