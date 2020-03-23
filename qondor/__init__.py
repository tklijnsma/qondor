# -*- coding: utf-8 -*-
import os
from .logger import setup_logger, setup_subprocess_logger
logger = setup_logger()
subprocess_logger = setup_subprocess_logger()

from . import utils

BATCHMODE = False
if 'QONDOR_BATCHMODE' in os.environ:
    BATCHMODE = True

COLLECTOR_NODES = None
DEFAULT_MGM = None

from . import schedd
from .schedd import get_best_schedd, get_schedd_ads, wait, remove_jobs
from .preprocess import Preprocessor, preprocessing
from .submit import SHFile, Submitter, CodeSubmitter
from .cmssw import CMSSW
from . import seutils

if os.environ.get('HOSTNAME', '').endswith('fnal.gov'):
    # Fix to be able to import htcondor python bindings
    import sys
    sys.path.extend([
        '/usr/lib64/python2.6/site-packages',
        '/usr/lib64/python2.7/site-packages'
        ])
    schedd.GLOBAL_SCHEDDMAN_CLS = schedd.ScheddManagerFermiHTC
    DEFAULT_MGM = 'root://cmseos.fnal.gov'

def get_proc_id():
    if BATCHMODE:
        return os.environ['CONDOR_PROCESS_ID'] 
    else:
        logger.info('Local mode - return proc_id 0')
        return 0
