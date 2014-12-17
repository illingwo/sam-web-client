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

class TestSamdevProject(testbase.SamdevTest):
    def test_runProject(self):
        self.samweb.runProject(defname='test-project')

    def test_listProjects(self):
        projects = self.samweb.listProjects(user='sam')
        assert (len(projects) > 1)

class TestProjectCommands(testbase.SAMWebCmdTest):

    def test_listProjects(self):
        cmdline = '-e samdev list-projects --user=sam --defname=%'
        self.check_cmd_return(cmdline.split())
        assert self.stdout

    def test_project(self):
        import time, socket, os
        defname = "test-project"
        projectname = '%s_samweb_test_%s_%s_%s' % (os.getlogin(), socket.gethostname(), os.getpid(), time.time())
        cmdline = '-e samdev start-project --defname=%s %s' % (defname, projectname)
        self.check_cmd_return(cmdline.split())
        projecturl = self.stdout.strip()
        assert projecturl.startswith('http')

        cmdline = '-e samdev start-process --appname=demo --appversion=1 --user=%s %s' % (os.getlogin(), projecturl)
        self.check_cmd_return(cmdline.split())
        processid = int(self.stdout.strip())

        cmdline = '-e samdev get-next-file %s %s' % (projecturl, processid)
        self.check_cmd_return(cmdline.split())
        fileurl = self.stdout.strip()
        assert fileurl

        cmdline = '-e samdev release-file %s %s %s' % (projecturl, processid, fileurl)
        self.check_cmd_return(cmdline.split())

        cmdline = '-e samdev stop-process %s %s' % (projecturl, processid)
        self.check_cmd_return(cmdline.split())

        cmdline = '-e samdev stop-project %s' % (projectname)
        self.check_cmd_return(cmdline.split())

        cmdline = '-e samdev project-summary %s' % (projectname)
        self.check_cmd_return(cmdline.split())
        assert self.stdout

    def test_project_with_snapshot(self):
        import time, socket, os
        snapshot_id = 1
        projectname = '%s_samweb_test_%s_%s_%s' % (os.getlogin(), socket.gethostname(), os.getpid(), time.time())
        cmdline = '-e samdev start-project --snapshot_id=%s %s' % (snapshot_id, projectname)
        self.check_cmd_return(cmdline.split())
        assert self.stdout.startswith('http')
        cmdline = '-e samdev stop-project %s' % (projectname)
        self.check_cmd_return(cmdline.split())


if __name__ == '__main__':
    unittest.main()
