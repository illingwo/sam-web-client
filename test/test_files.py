#! /usr/bin/env python
import testbase
import unittest
import samweb_client
import samweb_cli
import time

class TestDefinitionMinerva(testbase.SamdevTest):

    def test_listDimensions(self):
        dims = self.samweb.getAvailableDimensions()
        assert 'file_name' in [ dim for dim, desc in dims ]


class TestDefinitionCommands(testbase.SAMWebCmdTest):

    def test_listDimensions(self):

        cmdline = '-e samdev list-files --help-dimensions'
        self.check_cmd_return(cmdline.split())
        assert "file_name" in self.stdout

if __name__ == '__main__':
    unittest.main()
