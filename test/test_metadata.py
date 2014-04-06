#! /usr/bin/env python
import testbase
import unittest
import samweb_client
import samweb_cli
import time

test_file_name = 'test_file_name' +  str(long(time.time()))

class TestMetadataSamdev(testbase.SamdevTest):

    def test_getMetadata_FileNotFound(self):
        self.assertRaises(samweb_client.exceptions.FileNotFound, self.samweb.getMetadata, 'foo')

    def test_getMetadata(self):
        md = self.samweb.getMetadata('MN_00000798_0004_numib_v04_0911090239_RawEvents.root')
        self.assertEqual(md['file_name'], 'MN_00000798_0004_numib_v04_0911090239_RawEvents.root')

    def test_getMetadataAndLocations(self):
        md = self.samweb.getMetadata('MN_00000798_0004_numib_v04_0911090239_RawEvents.root', locations=True)
        assert 'locations' in md
        assert len('locations') > 0

    def test_getMultipleMetadata(self):
        def _check_results(mds):
            self.assertEqual(len(mds),2)
            for md in mds:
                if not ( (md['file_name'] == 'MN_00000798_0004_numib_v04_0911090239_RawEvents.root' and md['file_id'] == 1322)
                        or (md['file_name'] == 'MN_00000798_0005_numib_v04_0911090240_RawEvents.root' and md['file_id'] == 2552)):
                    assert False, "Expected metadata not found"

        mds = self.samweb.getMultipleMetadata(['MN_00000798_0004_numib_v04_0911090239_RawEvents.root',
            'MN_00000798_0005_numib_v04_0911090240_RawEvents.root'])
        _check_results(mds)

        mds = self.samweb.getMultipleMetadata(['MN_00000798_0004_numib_v04_0911090239_RawEvents.root', 2552])
        _check_results(mds)

        mds = self.samweb.getMultipleMetadata([1322, 2552])
        _check_results(mds)
    
        mds = self.samweb.getMultipleMetadata([1322], locations=True)
        md = mds[0]
        assert 'locations' in md
        assert len(md['locations']) > 0

    def test_validateMetadata(self):
        md = {'file_name' : test_file_name, 'file_type' : 'nonphysicsgeneric', 'file_size' : 1024,
            'data_tier':'raw',}
        self.samweb.validateFileMetadata(md=md)

    def test_validateMetadataFile(self):
        md = {'file_name' : test_file_name, 'file_type' : 'nonphysicsgeneric', 'file_size' : 1024,
            'data_tier':'raw',}
        import tempfile
        tmp = tempfile.TemporaryFile()
        samweb_client.json.dump(md, tmp)
        tmp.seek(0)

        self.samweb.validateFileMetadata(mdfile=tmp)

    def test_validateMetadataBad(self):
        md = {'file_name' : test_file_name}
        self.assertRaises(samweb_client.exceptions.InvalidMetadata, self.samweb.validateFileMetadata, md=md)

class TestMetadataCommands(testbase.SAMWebCmdTest):

    def test_validateMetadataCmd(self):
        md = {'file_name' : test_file_name, 'file_type' : 'nonphysicsgeneric', 'file_size' : 1024,
            'data_tier':'raw',}
        import tempfile
        tmp = tempfile.NamedTemporaryFile()
        samweb_client.json.dump(md, tmp)
        tmp.flush()

        cmdline = '-e samdev validate-metadata %s' % tmp.name
        self.check_cmd_return(cmdline.split())
        assert "Metadata is valid" in self.stdout

    def test_getMetadataCmd(self):
        cmdline = '-e samdev get-metadata MN_00000798_0004_numib_v04_0911090239_RawEvents.root'
        self.check_cmd_return(cmdline.split())

        assert "File Name: MN_00000798_0004_numib_v04_0911090239_RawEvents.root" in self.stdout

    def test_getMetadataWithLocations(self):
        cmdline = '-e samdev get-metadata --json --locations MN_00000798_0004_numib_v04_0911090239_RawEvents.root'
        self.check_cmd_return(cmdline.split())
        md = samweb_client.json.loads(self.stdout)
        assert md["file_name"] == "MN_00000798_0004_numib_v04_0911090239_RawEvents.root"
        assert len(md["locations"]) > 0

    def test_getMultipleMetadataWithLocations(self):
        cmdline = '-e samdev get-metadata --json --locations MN_00000798_0004_numib_v04_0911090239_RawEvents.root MN_00000798_0005_numib_v04_0911090240_RawEvents.root'
        self.check_cmd_return(cmdline.split())
        mds = samweb_client.json.loads(self.stdout)
        for md in mds:
            assert md["file_name"] in ("MN_00000798_0004_numib_v04_0911090239_RawEvents.root", "MN_00000798_0005_numib_v04_0911090240_RawEvents.root")
            assert len(md["locations"]) > 0

if __name__ == '__main__':
    unittest.main()
