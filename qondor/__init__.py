# -*- coding: utf-8 -*-
import os, os.path as osp, uuid, logging, argparse, pprint, json
from .logger import setup_logger, setup_subprocess_logger
logger = setup_logger()
subprocess_logger = setup_subprocess_logger()

def debug(flag=True):
    if flag:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

INCLUDE_DIR = osp.join(osp.abspath(osp.dirname(__file__)), 'include')

from . import utils

BATCHMODE = False
if 'QONDOR_BATCHMODE' in os.environ:
    BATCHMODE = True

COLLECTOR_NODES = None
DEFAULT_MGM = None
TIMESTAMP_FMT = '%Y%m%d_%H%M%S'

from . import schedd
from .schedd import get_best_schedd, get_schedd_ads, wait, remove_jobs
from .submit import get_first_cluster
from .cmssw import CMSSW
from .cmssw_releases import get_arch
from . import svj
import seutils


# ___________________________________________________
# Decisions at import-time depending on whether this is a job or
# an import on an interactive node

_TRIED_ONCE = False
class Scope(argparse.Namespace):
    """
    Like an argparse.Namespace object, but when first called loads the scope either
    from a .json file or from the python file on top of the stack (first cluster).
    """

    def __init__(self, *args, **kwargs):
        super(Scope, self).__init__(*args, **kwargs)
        self.is_loaded = False

    def load_batchmode(self):
        if 'QONDORSCOPEFILE' in os.environ:
            scope_file = os.environ['QONDORSCOPEFILE']
            if osp.isfile(scope_file):
                with open(scope_file, 'r') as f:
                    d = json.load(f)
                    scope.__dict__.update(d)
                logger.info('Loaded following scope from %s:\n%s', scope_file, pprint.pformat(scope))
                self.is_loaded = True
                return
        logger.info('Could not load scope')

    def load_localmode(self):
        import traceback
        stack = traceback.extract_stack()
        python_file = stack[0][0]
        cluster = get_first_cluster(python_file)
        self.__dict__.update(cluster.scope)
        self.is_loaded = True

    def load(self):
        logger.info('Trying to load scope...')
        self.load_batchmode() if BATCHMODE else self.load_localmode()

    # argparse.Namespace doesn't have a __getitem__ method
    # def __getitem__(self, *args, **kwargs):
    #     """
    #     Hacked to call .load() once upon the first call of __getitem__
    #     """
    #     global _TRIED_ONCE
    #     if not _TRIED_ONCE:
    #         _TRIED_ONCE = True
    #         self.load()
    #     return super(Scope, self).__getitem__(*args, **kwargs)

    def __getattribute__(self, *args, **kwargs):
        """
        Hacked to call .load() once upon the first call of __getattribute__
        """
        global _TRIED_ONCE
        if not _TRIED_ONCE:
            _TRIED_ONCE = True
            self.load()
        return super(Scope, self).__getattribute__(*args, **kwargs)

scope = Scope()

def load_seutils_cache():
    if BATCHMODE and scope.is_loaded:
        seutils_cache_tarball = osp.basename(scope.transfer_files['seutils-cache'])
        if osp.isfile(seutils_cache_tarball): seutils.load_tarball_cache(seutils_cache_tarball)

if BATCHMODE:
    load_seutils_cache()
    seutils.N_COPY_RETRIES = 2 # Sets default amount of times to retry a seutils.cp statement

# Read rootcache is this is a job
if BATCHMODE and osp.isfile('rootcache.tar.gz'):
    seutils.root.load_cache('rootcache.tar.gz')

# FNAL-specific things
if os.environ.get('HOSTNAME', '').endswith('fnal.gov'):
    # Fix to be able to import htcondor python bindings
    import sys
    if sys.version_info.major < 3:
        logger.warning('Detected FNAL: Modifying path to use system htcondor python bindings')
        sys.path.extend([
            '/usr/lib64/python2.6/site-packages',
            '/usr/lib64/python2.7/site-packages'
            ])
    schedd.GLOBAL_SCHEDDMAN_CLS = schedd.ScheddManagerFermiHTC
    seutils.set_default_mgm('root://cmseos.fnal.gov')


# ___________________________________________________
# 'Globals'

DRYMODE = False
def drymode(flag=True):
    global DRYMODE
    DRYMODE = flag


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

def init_cmssw(tarball_key='cmssw_tarball', scram_arch=None, outdir=None):
    """
    A shortcut function to quickly extract and setup a CMSSW tarball.
    The first argument `tarball_key` may also be a path (either on a storage element or local).
    """
    if osp.isfile(tarball_key):
        # A path to a local tarball was given
        cmssw_tarball = tarball_key
    elif seutils.has_protocol(tarball_key):
        # A path to a tarball on a storage element was given
        cmssw_tarball = tarball_key
    else:
        # A key to a file in the preprocessing was given
        cmssw_tarball = qondor.scope[tarball_key]
    cmssw = CMSSW.from_tarball(cmssw_tarball, scram_arch, outdir=outdir)
    return cmssw

def get_submission_timestamp():
    """
    Returns the submission time as a timestamp string with format TIMESTAMP_FMT
    """
    if BATCHMODE:
        return os.environ['CLUSTER_SUBMISSION_TIMESTAMP']
    else:
        from datetime import datetime
        logger.info('Local mode - returning current time')
        return datetime.now().strftime(TIMESTAMP_FMT)

def get_submission_time():
    """
    Returns the submission time as datetime object
    """
    from datetime import datetime
    if BATCHMODE:
        return datetime.strptime(get_submission_timestamp(), TIMESTAMP_FMT)
    else:
        logger.info('Local mode - returning current time')
        return datetime.now()

def get_submission_timestr(fmt='%b%d'):
    return get_submission_time().strftime(fmt)
