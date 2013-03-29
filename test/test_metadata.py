import testbase
import unittest
import samweb_client.exceptions

class TestMetadataMinerva(testbase.MinervaDevTest):

    def test_getMetadata_FileNotFound(self):
        self.assertRaises(samweb_client.exceptions.FileNotFound, self.samweb.getMetadata, 'foo')

    def test_getMetadata(self):
        md = self.samweb.getMetadata('MN_00000798_0004_numib_v04_0911090239_RawEvents.root')
        self.assertEqual(md['file_name'], 'MN_00000798_0004_numib_v04_0911090239_RawEvents.root')

if __name__ == '__main__':
    unittest.main()
