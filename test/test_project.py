#! /usr/bin/env python
import testbase
import unittest
import samweb_client
import samweb_cli
import time, socket, os

defname = 'one_enstore_file_test'

class TestStartProject(testbase.SamdevTest):

    def test_startProject_badargs(self):
        projectname = 'test-project-%s-%s-%s' % (socket.getfqdn(), os.getpid(), time.time())
        self.assertRaises(samweb_client.exceptions.Error, self.samweb.startProject, projectname)
        self.assertRaises(samweb_client.exceptions.Error, self.samweb.startProject, projectname, defname=defname, snapshot_id=10)

class TestMinervaProject(testbase.MinervaDevTest):
    def test_listProjects(self):
        projects = self.samweb.listProjects(user='sam')
        assert (len(projects) > 1)

class TestProjectCommands(testbase.SAMWebCmdTest):

    def test_listProjects(self):
        cmdline = '-e minerva/dev list-projects --user=sam --defname=%'
        self.check_cmd_return(cmdline.split())
        assert self.stdout

if __name__ == '__main__':
    unittest.main()
