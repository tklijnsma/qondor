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

    def tearDown(self):
        os.chdir(self._backdir)

    def test_helloworld(self):
        python_file = osp.join(TESTDIR, 'job_helloworld.py')
        submitter = qondor.Submitter(python_file)
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
            [l.strip() for l in out_contents.split('\n') if len(l.strip) != 0][-1],
            'Hello world!'
            )

class TestSplitTransactions(TestCase):

    def setUp(self):
        self._backdir = os.getcwd()
        os.chdir(TESTDIR)
        self.cluster_ids_to_remove = []

    def tearDown(self):
        for cluster_id in self.cluster_ids_to_remove:
            qondor.remove_jobs(cluster_id)
        os.chdir(self._backdir)

    def test_splittransactions(self):
        python_file = osp.join(TESTDIR, 'job_split_transactions.py')
        submitter = qondor.Submitter(python_file)
        qondor.logger.info(submitter.preprocessing.split_transactions)
        cluster_ids, ads = submitter.submit()
        self.cluster_ids_to_remove.extend(cluster_ids)
        self.assertTrue('this_is_item_1' in ads[0][0]['Environment'])


class TestCMSSW(TestCase):

    def setUp(self):
        self._backdir = os.getcwd()
        self.tarball = osp.join(TESTDIR, 'CMSSW_10_2_18.tar.gz')
        self.scram_arch = 'slc7_amd64_gcc820'
        self.rundir = '/tmp/qondortesting'

    def tearDown(self):
        os.chdir(self._backdir)

    def test_extract(self):
        cmssw = qondor.CMSSW.from_tarball(self.tarball, self.scram_arch, outdir=self.rundir)
        self.assertEquals(
            cmssw.cmssw_src,
            osp.join(self.rundir, 'CMSSW_10_2_18/src')
            )
