from unittest import TestCase

try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os, glob
import os.path as osp
import qondor

logger = qondor.logger

TESTDIR = osp.abspath(osp.dirname(__file__))


class CaseJobWithCleanup(TestCase):
    def setUp(self):
        self._backdir = os.getcwd()
        os.chdir(TESTDIR)
        self.cluster_ids_to_remove = []
        self.dirs_to_remove = []
        self.files_to_remove = []

    def tearDown(self):
        for cluster_id in self.cluster_ids_to_remove:
            qondor.remove_jobs(cluster_id)
        for directory in self.dirs_to_remove:
            if not osp.isdir(directory):
                continue
            logger.info("Removing %s", directory)
            shutil.rmtree(directory)
        for file in self.files_to_remove:
            if not osp.isfile(file):
                continue
            logger.info("Removing %s", file)
            os.remove(file)
        os.chdir(self._backdir)


class TestHelloWorld(CaseJobWithCleanup):
    def test_helloworld(self):
        python_file = osp.join(TESTDIR, "job_helloworld.py")
        submitter = qondor.Submitter(python_file)
        cluster_id, ads = submitter.submit()
        self.cluster_ids_to_remove.append(cluster_id)
        qondor.logger.info("Submitted job %s:\n%s", cluster_id, ads)
        qondor.wait(cluster_id)
        out = osp.join(ads[0]["QondorRundir"], ads[0]["Out"])
        err = osp.join(ads[0]["QondorRundir"], ads[0]["Err"])
        with open(out, "r") as f:
            out_contents = f.read()
        with open(err, "r") as f:
            err_contents = f.read()
        qondor.logger.info("Contents of %s:\n%s", out, out_contents)
        qondor.logger.info("Contents of %s:\n%s", err, err_contents)
        self.assertEquals(
            [l.strip() for l in err_contents.split("\n") if len(l.strip()) != 0][-1],
            "Hello world!",
        )

    def test_helloworld_nofile(self):
        submitter = qondor.CodeSubmitter("print 'Hello world!'")
        cluster_id, ads = submitter.submit()
        self.cluster_ids_to_remove.append(cluster_id)
        qondor.logger.info("Submitted job %s:\n%s", cluster_id, ads)
        qondor.wait(cluster_id)
        out = osp.join(ads[0]["QondorRundir"], ads[0]["Out"])
        err = osp.join(ads[0]["QondorRundir"], ads[0]["Err"])
        with open(out, "r") as f:
            out_contents = f.read()
        with open(err, "r") as f:
            err_contents = f.read()
        qondor.logger.info("Contents of %s:\n%s", out, out_contents)
        qondor.logger.info("Contents of %s:\n%s", err, err_contents)
        self.assertEquals(
            [l.strip() for l in err_contents.split("\n") if len(l.strip()) != 0][-1],
            "Hello world!",
        )

    def test_helloworld_nofile_local(self):
        submitter = qondor.CodeSubmitter("print('Hello world!')", "testvar testvalue")
        output = submitter.run_local()
        self.assertEquals(
            [l.strip() for l in output if len(l.strip()) != 0][-1], "Hello world!"
        )


class TestSplitTransactions(CaseJobWithCleanup):
    def test_splittransactions(self):
        python_file = osp.join(TESTDIR, "job_split_transactions.py")
        submitter = qondor.Submitter(python_file)
        logger.info(submitter.preprocessing.split_transactions)
        cluster_ids, ads = submitter.submit()
        self.cluster_ids_to_remove.extend(cluster_ids)
        self.assertTrue("this_is_item_1" in ads[0][0]["Environment"])


class TestCMSSW(CaseJobWithCleanup):
    def setUp(self):
        super(TestCMSSW, self).setUp()
        self.tarball = osp.join(
            TESTDIR, "CMSSW_11_0_0_pre10_HGCALHistoryWithCaloPositions.tar.gz"
        )

    def test_extract_and_run_locally(self):
        cmssw = qondor.CMSSW.from_tarball(self.tarball)
        self.assertEquals(
            cmssw.cmssw_src,
            osp.join(qondor.CMSSW.default_local_rundir, "CMSSW_11_0_0_pre10/src"),
        )
        passed_outfile = osp.join(TESTDIR, "test_root_file.root")
        actual_outfile = passed_outfile.replace(".root", "_numEvent5.root")
        self.files_to_remove.append(actual_outfile)
        output = cmssw.run_command(
            [
                "cmsRun",
                "HGCALDev/PCaloHitWithPostionProducer/python/SingleMuPt_pythia8_cfi_GEN_SIM_PCaloHitWithPosition.py",
                "outputFile={0}".format(passed_outfile),
                "maxEvents=5",
            ]
        )
        self.assertTrue(len(output) > 0)
        logger.info("output[0]: %s", output[0])
        logger.info("output[1]: %s", output[1])
        self.assertTrue(osp.isfile(actual_outfile))

    def test_extract_and_run_condor(self):
        python_file = osp.join(TESTDIR, "job_cmssw.py")
        submitter = qondor.Submitter(python_file)
        cluster_id, ads = submitter.submit()
        outfile = "root://cmseos.fnal.gov//store/user/klijnsma/qondor_testing/test.root"
        # Check and clean up manually...
