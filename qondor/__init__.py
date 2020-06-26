# -*- coding: utf-8 -*-
import os, os.path as osp
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
import seutils

# Read cached ls calls if this is a job
if BATCHMODE and osp.isfile(Preprocessor.LS_CACHE_FILE):
    Preprocessor.read_ls_cache(Preprocessor.LS_CACHE_FILE)

if os.environ.get('HOSTNAME', '').endswith('fnal.gov'):
    # Fix to be able to import htcondor python bindings
    import sys
    sys.path.extend([
        '/usr/lib64/python2.6/site-packages',
        '/usr/lib64/python2.7/site-packages'
        ])
    schedd.GLOBAL_SCHEDDMAN_CLS = schedd.ScheddManagerFermiHTC
    seutils.set_default_mgm('root://cmseos.fnal.gov')


# ___________________________________________________
# Short cuts

def get_proc_id():
    if BATCHMODE:
        return int(os.environ['CONDOR_PROCESS_ID'])
    else:
        logger.info('Local mode - return proc_id 0')
        return 0

def get_cluster_id():
    if BATCHMODE:
        return os.environ['CONDOR_CLUSTER_NUMBER']
    else:
        logger.info('Local mode - return cluster_id 1234')
        return str(1234)

CACHED_PREPROC = None

def get_master_preproc(python_file=None):
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

def get_preproc():
    """
    Gets the preprocessing set that this job is supposed to do.
    In local mode it just returns the first set.
    If no sets are defined, it returns just the preprocessing.
    """
    if BATCHMODE:
        iset = int(os.environ['QONDORISET'])
        for subset in get_master_preproc().sets():
            if subset.subset_index == iset:
                return subset
        else:
            raise RuntimeError(
                'Looking for set {0} failed: no such set'
                .format(iset)
                )
    else:
        logger.info('Local mode - returning simply first set')
        return next(get_master_preproc().sets())

def get_item():
    """
    Gets the item (string or list of strings) that this job is supposed to do.
    In local mode it just returns the first item
    """
    if BATCHMODE:
        try:
            item = os.environ['QONDORITEM']
            if ',' in item: item = item.split(',')
            return item
        except KeyError:
            logger.error('No item was found for this job! Are you sure you passed items?')
            raise
    else:
        logger.info('Local mode - returning first item')
        items = get_master_preproc().all_items()
        if not items: raise RuntimeError('No items determined in preprocessing')
        return items[get_proc_id()]

def init_cmssw(tarball_key='cmssw_tarball', scram_arch=None):
    """
    A shortcut function to quickly extract and setup a CMSSW tarball
    """
    cmssw_tarball = get_preproc().files[tarball_key]
    cmssw = CMSSW.from_tarball(cmssw_tarball, scram_arch)
    return cmssw

def get_var(variable):
    """
    Shortcut to get a variable defined in the preprocessing
    """
    return get_preproc().variables[variable]
