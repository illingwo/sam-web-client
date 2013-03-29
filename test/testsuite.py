#! /usr/bin/env python

import unittest

if __name__ == '__main__':
    suite = unittest.TestSuite()
    for testmodule in ['test_metadata']:
        mod = __import__(testmodule)
        suite.addTest(unittest.defaultTestLoader.loadTestsFromModule(mod))

    unittest.TextTestRunner(verbosity=2).run(suite)
