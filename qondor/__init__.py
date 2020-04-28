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
from .cmssw_releases import get_arch
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


# ___________________________________________________
# Short cuts

def get_proc_id():
    if BATCHMODE:
        return int(os.environ['CONDOR_PROCESS_ID'])
    else:
        logger.info('Local mode - return proc_id 0')
        return 0

CACHED_PREPROC = None

def get_preproc(python_file=None):
    """
    Returns and caches the preprocessing of a python file.
    By default it picks the top level python file in the stack, and calls the preprocessor on that.
    """
    global CACHED_PREPROC
    if CACHED_PREPROC: return CACHED_PREPROC
    if not python_file:
        import traceback
        stack = traceback.extract_stack()
        python_file = stack[0][0]
    CACHED_PREPROC = preprocessing(python_file)
    return CACHED_PREPROC

def get_chunk():
    """Runs the preprocessor on the top level python file, and returns the chunk for the current proc_id"""
    chunks = get_preproc().chunks
    if not chunks: raise RuntimeError('No chunks determined in preprocessing')
    return chunks[get_proc_id()]

def get_cluster_id():
    if BATCHMODE:
        return os.environ['CONDOR_CLUSTER_NUMBER']
    else:
        logger.info('Local mode - return cluster_id 1234')
        return str(1234)

def init_cmssw(tarball_key='cmssw_tarball', scram_arch=None):
    cmssw_tarball = get_preproc().files[tarball_key]
    cmssw = CMSSW.from_tarball(cmssw_tarball, scram_arch)
    return cmssw
