#! /usr/bin/env python
import testbase
import unittest
import samweb_client
import samweb_cli
import time
import types

class TestDefinition(testbase.SamdevTest):

    def test_listDimensions(self):
        dims = self.samweb.getAvailableDimensions()
        assert 'file_name' in [ dim for dim, desc in dims ]

    def test_listFiles(self):
        files = self.samweb.listFiles("file_name MN_00000798_0004_numib_v04_0911090239_RawEvents.root")
        assert isinstance(files, list)
        assert len(files)==1 and files[0]=="MN_00000798_0004_numib_v04_0911090239_RawEvents.root"

        files = self.samweb.listFiles("file_name MN_00000798_0004_numib_v04_0911090239_RawEvents.root",stream=True)
        assert hasattr(files, 'next')
        files = list(files)
        assert len(files)==1 and files[0]=="MN_00000798_0004_numib_v04_0911090239_RawEvents.root"

class TestDefinitionCommands(testbase.SAMWebCmdTest):

    def test_listDimensions(self):

        cmdline = '-e samdev list-files --help-dimensions'
        self.check_cmd_return(cmdline.split())
        assert "file_name" in self.stdout

    def test_listFiles(self):
        cmdline = ['-e', 'samdev', 'list-files', 'file_name MN_00000798_0004_numib_v04_0911090239_RawEvents.root']
        self.check_cmd_return(cmdline)
        files = self.stdout.rstrip().split('\n')
        assert len(files)==1 and files[0]=="MN_00000798_0004_numib_v04_0911090239_RawEvents.root"

if __name__ == '__main__':
    unittest.main()
