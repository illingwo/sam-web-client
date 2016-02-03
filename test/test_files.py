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

class TestLocation(testbase.SamdevTest):

    def test_locateFile(self):
        locations = self.samweb.locateFile("MN_00000798_0004_numib_v04_0911090239_RawEvents.root")

        assert set([l["full_path"] for l in locations]) == set(['enstore:/pnfs/samdev/rawdata/raw/numib/00/00/07/98', 'samdevdata:/grid/data/samdev/data01'])

    def test_locateFiles(self):
        locations = self.samweb.locateFiles(["MN_00000798_0004_numib_v04_0911090239_RawEvents.root", 'MN_00000798_0005_numib_v04_0911090240_RawEvents.root'])
        assert "MN_00000798_0004_numib_v04_0911090239_RawEvents.root" in locations and 'MN_00000798_0005_numib_v04_0911090240_RawEvents.root' in locations
        assert set([l["full_path"] for l in locations['MN_00000798_0004_numib_v04_0911090239_RawEvents.root']]) == set(['enstore:/pnfs/samdev/rawdata/raw/numib/00/00/07/98', 'samdevdata:/grid/data/samdev/data01'])

    def test_locateFilesIterator(self):
        filenames = ["MN_00000798_0004_numib_v04_0911090239_RawEvents.root", 'MN_00000798_0005_numib_v04_0911090240_RawEvents.root']
        locations = list(self.samweb.locateFilesIterator(filenames))
        assert set(filenames) == set( l[0] for l in locations)

    def test_fileUrl(self):
        urls = self.samweb.getFileAccessUrls("MN_00000798_0004_numib_v04_0911090239_RawEvents.root", schema="gsiftp")
        assert set(urls) == set(['gsiftp://fg-bestman1.fnal.gov:2811/grid/data/samdev/data01/MN_00000798_0004_numib_v04_0911090239_RawEvents.root',
            'gsiftp://fndca1.fnal.gov:2811/pnfs/fnal.gov/usr/samdev/rawdata/raw/numib/00/00/07/98/MN_00000798_0004_numib_v04_0911090239_RawEvents.root'])

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

class TestLocateCommands(testbase.SAMWebCmdTest):
    def test_getFileAccessUrl(self):
        cmdline = ['-e', 'samdev', 'get-file-access-url', '--schema=gsiftp', 'MN_00000798_0004_numib_v04_0911090239_RawEvents.root']
        self.check_cmd_return(cmdline)
        urls = self.stdout.rstrip().split('\n')
        assert set(urls) == set(['gsiftp://fg-bestman1.fnal.gov:2811/grid/data/samdev/data01/MN_00000798_0004_numib_v04_0911090239_RawEvents.root',
            'gsiftp://fndca1.fnal.gov:2811/pnfs/fnal.gov/usr/samdev/rawdata/raw/numib/00/00/07/98/MN_00000798_0004_numib_v04_0911090239_RawEvents.root'])

if __name__ == '__main__':
    unittest.main()
