from unittest import TestCase
try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os
import os.path as osp
import qondor

TESTDIR = osp.abspath(osp.dirname(__file__))


class TestSeutils(TestCase):

    def setUp(self):
        qondor.seutils.set_default_mgm('root://cmseos.fnal.gov')

    def test_ls(self):
        contents = qondor.seutils.ls('/store/user/klijnsma')
        self.assertTrue(hasattr(contents, '__getitem__'))

    def test_exists(self):
        self.assertFalse(qondor.seutils.exists('/store/clearly/does/not/exist'))

    def test_isdir(self):
        self.assertTrue(qondor.seutils.isdir('/store/user/klijnsma'))

    def test_isfile(self):
        self.assertTrue(qondor.seutils.isfile('/store/user/klijnsma/qondor_testing/test_numEvent5.root'))

    def test_ls_root(self):
        contents = qondor.seutils.ls_root('/store/user/klijnsma/qondor_testing/test_numEvent5.root')
        self.assertEqual(
            contents[0],
            qondor.seutils.format('/store/user/klijnsma/qondor_testing/test_numEvent5.root')
            )

    def test_format(self):
        testfile = '/store/user/klijnsma/qondor_testing/test_numEvent5.root'
        mgm, lfn = qondor.seutils.split_mgm(testfile)
        self.assertEqual(mgm, 'root://cmseos.fnal.gov')
        self.assertEqual(
            qondor.seutils.format(lfn, mgm=mgm),
            'root://cmseos.fnal.gov//store/user/klijnsma/qondor_testing/test_numEvent5.root'
            )
        self.assertEqual(
            qondor.seutils.format(lfn),
            'root://cmseos.fnal.gov//store/user/klijnsma/qondor_testing/test_numEvent5.root'
            )

    def test_ls_root_mgm_is_okay(self):
        contents = qondor.seutils.ls_root('/store/user/klijnsma/qondor_testing')
        self.assertEqual(
            contents[0],
            'root://cmseos.fnal.gov//store/user/klijnsma/qondor_testing/test_numEvent5.root'
            )

