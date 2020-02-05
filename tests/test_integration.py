from unittest import TestCase
try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os
import os.path as osp
import qondor

TESTDIR = osp.abspath(osp.dirname(__file__))


class TestHelloWorld(TestCase):

    def setUp(self):
        self._backdir = os.getcwd()
        os.chdir(TESTDIR)
        # qondor.Submitter.ignore_uncommitted_changes_in_python_modules = True
        self.python_file = osp.join(TESTDIR, 'job_helloworld.py')

    def tearDown(self):
        os.chdir(self._backdir)

    def test_helloworld(self):
        submitter = qondor.Submitter(self.python_file)
        cluster_id, ads = submitter.submit()
        qondor.logger.info('Submitted job %s:\n%s', cluster_id, ads)
        qondor.wait(cluster_id)
        out = osp.join(ads[0]['QondorRundir'], ads[0]['Out'])
        err = osp.join(ads[0]['QondorRundir'], ads[0]['Err'])
        with open(out, 'r') as f:
            out_contents = f.read()
        with open(err, 'r') as f:
            err_contents = f.read()
        qondor.logger.info('Contents of %s:\n%s', out, out_contents)
        qondor.logger.info('Contents of %s:\n%s', err, err_contents)
        self.assertEquals(
            [l.strip() for l in out_contents.split('\n') if len(l.strip) != 0][-1]
            'Hello world!'
            )
