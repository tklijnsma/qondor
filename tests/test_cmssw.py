from unittest import TestCase
try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os
import os.path as osp
import qondor

TESTDIR = osp.abspath(osp.dirname(__file__))

class TestCMSSWReleases(TestCase):

    def setUp(self):
        pass

    def test_release_getting(self):
        self.assertEquals(qondor.get_arch('CMSSW_7_4_4'), 'slc6_amd64_gcc491')
        self.assertEquals(qondor.get_arch('CMSSW_11_1_0'), 'slc7_amd64_gcc900')
        with self.assertRaises(RuntimeError):
            qondor.get_arch('CMSSW_99_99_99')
            qondor.get_arch('blablabla')


# class TestCMSSW(TestCase):

#     def setUp(self):
#         pass

#     def test_quick_setup(self):
#         pass


#     def test_release_getting(self):
#         self.assertEquals(qondor.get_arch('CMSSW_7_4_4'), 'slc6_amd64_gcc491')
#         self.assertEquals(qondor.get_arch('CMSSW_11_1_0'), 'slc7_amd64_gcc900')
#         with self.assertRaises(RuntimeError):
#             qondor.get_arch('CMSSW_99_99_99')
#             qondor.get_arch('blablabla')
