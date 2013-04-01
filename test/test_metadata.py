#! /usr/bin/env python
import testbase
import unittest
import samweb_client
import samweb_cli

class TestMetadataMinerva(testbase.MinervaDevTest):

    def test_getMetadata_FileNotFound(self):
        self.assertRaises(samweb_client.exceptions.FileNotFound, self.samweb.getMetadata, 'foo')

    def test_getMetadata(self):
        md = self.samweb.getMetadata('MN_00000798_0004_numib_v04_0911090239_RawEvents.root')
        self.assertEqual(md['file_name'], 'MN_00000798_0004_numib_v04_0911090239_RawEvents.root')

    def test_validateMetadata(self):
        md = {'file_name' : 'test_file_name', 'file_type' : 'nonPhysicsGeneric', 'file_size' : 1024,
            'data_tier':'raw',}
        self.samweb.validateFileMetadata(md=md)

    def test_validateMetadataFile(self):
        md = {'file_name' : 'test_file_name', 'file_type' : 'nonPhysicsGeneric', 'file_size' : 1024,
            'data_tier':'raw',}
        import tempfile
        tmp = tempfile.TemporaryFile()
        samweb_client.json.dump(md, tmp)
        tmp.seek(0)

        self.samweb.validateFileMetadata(mdfile=tmp)


    def test_validateMetadataBad(self):
        md = {'file_name' : 'test_file_name'}
        self.assertRaises(samweb_client.exceptions.InvalidMetadata, self.samweb.validateFileMetadata, md=md)

class TestMetadataCommands(testbase.SAMWebCmdTest):

    def test_validateMetadataCmd(self):
        md = {'file_name' : 'test_file_name', 'file_type' : 'nonPhysicsGeneric', 'file_size' : 1024,
            'data_tier':'raw',}
        import tempfile
        tmp = tempfile.NamedTemporaryFile()
        samweb_client.json.dump(md, tmp)
        tmp.flush()

        cmdline = '-e minerva/dev validate-metadata %s' % tmp.name
        self.trap_output()
        try:
            self.check_cmd_return(samweb_cli.main(cmdline.split()))
        finally:
            self.restore_output()
        assert "Metadata is valid" in self.stdout

    def test_getMetadataCmd(self):
        cmdline = '-e minerva/dev get-metadata MN_00000798_0004_numib_v04_0911090239_RawEvents.root'
        self.trap_output()
        try:
            self.check_cmd_return(samweb_cli.main(cmdline.split()))
        finally:
            self.restore_output()

        assert "File Name: MN_00000798_0004_numib_v04_0911090239_RawEvents.root" in self.stdout

if __name__ == '__main__':
    unittest.main()
