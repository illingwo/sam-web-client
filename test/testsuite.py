#! /usr/bin/env python

import unittest
import optparse

testmodules = ['test_files', 'test_metadata', 'test_admin', 'test_definition', 'test_project']

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-v","--verbose", action="store_true", dest="verbose", default=False,
            help="Increase verbosity of output")
    (options, args) = parser.parse_args()

    suite = unittest.TestSuite()
    for testmodule in testmodules:
        mod = __import__(testmodule)
        suite.addTest(unittest.defaultTestLoader.loadTestsFromModule(mod))

    if options.verbose:
        verbosity=2
    else:
        verbosity=1

    unittest.TextTestRunner(verbosity=verbosity).run(suite)
