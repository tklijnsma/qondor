from unittest import TestCase
try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os
import os.path as osp
import qondor

TESTDIR = osp.abspath(osp.dirname(__file__))


class TestPreprocessing(TestCase):

    # def setUp(self):
    #     pass

    def test_line_preprocessing(self):
        preprocessor = qondor.Preprocessor()
        preprocessor.preprocess_line('pip module-install svj.core')
        self.assertIn(('svj.core', 'module-install'), preprocessor.pip)
        preprocessor.preprocess_line('htcondor executable     testjob.sh')
        self.assertEquals(preprocessor.htcondor['executable'], 'testjob.sh')
        preprocessor.preprocess_line('file cmssw_tarball CMSSW_X_X_X.tar.gz')
        self.assertEquals(preprocessor.files['cmssw_tarball'], 'CMSSW_X_X_X.tar.gz')
        preprocessor.preprocess_line('njobs 10')
        self.assertEquals(preprocessor.variables['njobs'], '10')
        preprocessor.preprocess_line('env gccsetup  /cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh')
        self.assertEquals(preprocessor.env['gccsetup'], '/cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh')

    def test_line_continuation(self):
        lines = qondor.preprocess.get_preprocess_lines([
            '#$ somevar = \\',
            '#$   somevalue '
            ])
        self.assertEquals(lines, ['somevar = somevalue'])

    def test_sh_file(self):
        preprocessor = qondor.Preprocessor()
        preprocessor.preprocess_line('pip module-install svj.core')
        preprocessor.preprocess_line('htcondor executable     testjob.sh')
        preprocessor.preprocess_line('file cmssw_tarball CMSSW_X_X_X.tar.gz')
        preprocessor.preprocess_line('njobs 10')
        preprocessor.preprocess_line('env gccsetup  /cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh')
        shfile = qondor.SHFile(preprocessor)
        lines = shfile.parse()
        self.assertEquals(lines[0], '#!/bin/bash')
        shfile.to_file('path/to/file.sh', dry=True)

    def test_get_item(self):
        preprocessor = qondor.Preprocessor()
        preprocessor.preprocess_line('split_transactions item1 item2 item3')
        self.assertEquals(preprocessor.get_item(), 'item1')
        qondor.BATCHMODE = True
        os.environ['QONDORITEM'] = 'item3'
        self.assertEquals(preprocessor.get_item(), 'item3')

    def tearDown(self):
        qondor.BATCH_MODE = False
