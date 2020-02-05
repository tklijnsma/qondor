# -*- coding: utf-8 -*-
import os
from .logger import setup_logger, setup_subprocess_logger
logger = setup_logger()
subprocess_logger = setup_subprocess_logger()

from . import utils

BATCHMODE = False
if 'QONDOR_BATCHMODE' in os.environ:
    BATCHMODE = True

from . import schedd
from .schedd import get_best_schedd, get_schedd_ads, wait
from .preprocess import Preprocessor
from .submit import SHFile, Submitter


if os.environ.get('HOSTNAME', '').endswith('fnal.gov'):
    # Fix to be able to import htcondor python bindings
    import sys
    sys.path.extend([
        '/usr/lib64/python2.6/site-packages',
        '/usr/lib64/python2.7/site-packages'
        ])
