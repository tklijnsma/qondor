from unittest import TestCase

try:
    from mock import Mock, MagicMock, patch
except ImportError:
    from unittest.mock import Mock, MagicMock, patch
import logging, os
import os.path as osp
import qondor

TESTDIR = osp.abspath(osp.dirname(__file__))


class TestIter(TestCase):
    def setUp(self):
        pass

    def test_get_ith_chunk(self):
        self.assertEqual(
            qondor.utils.get_ith_chunk([0, 1, 2, 3], n_chunks=2, i_chunk=1), [2, 3]
        )
        self.assertEqual(
            qondor.utils.get_ith_chunk([0, 1, 2, 3], n_chunks=4, i_chunk=3), [3]
        )

    def test_chunkify(self):
        self.assertEqual(len(qondor.utils.chunkify(range(2), 10)), 10)
