# -*- coding: utf-8 -*-

import os.path as osp
import logging, subprocess, os, shutil, re, pprint, csv
import qondor
logger = logging.getLogger('qondor')

# _______________________________________________________
# Path management

def set_default_mgm(mgm):
    """
    Sets the default mgm
    """
    qondor.DEFAULT_MGM = mgm
    logger.info('Default mgm set to %s', mgm)

def get_default_mgm():
    if qondor.DEFAULT_MGM is None:
        raise RuntimeError(
            'A request relied on the default mgm to be set. '
            'Either use `qondor.seutils.set_default_mgm` or '
            'pass use the full path (starting with "root:") '
            'in your request.'
            )
    return qondor.DEFAULT_MGM

def _unsafe_split_mgm(filename):
    """
    Takes a properly formatted path starting with 'root:' and containing '/store'
    """
    if not filename.startswith('root://'):
        raise ValueError(
            'Cannot split mgm; passed filename: {0}'
            .format(filename)
            )
    elif not '/store' in filename:
        raise ValueError(
            'No substring \'/store\' in filename {0}'
            .format(filename)
            )
    i = filename.index('/store')
    mgm = filename[:i]
    lfn = filename[i:]
    return mgm, lfn

def split_mgm(path, mgm=None):
    """
    Returns the mgm and lfn that the user most likely intended to
    if path starts with 'root://', the mgm is taken from the path
    if mgm is passed, it is used as is
    if mgm is passed AND the path starts with 'root://' AND the mgm's don't agree,
      an exception is thrown
    if mgm is None and path has no mgm, the default variable DEFAULT_MGM is taken
    """
    if path.startswith('root://'):
        _mgm, lfn = _unsafe_split_mgm(path)
        if not(mgm is None) and not _mgm == mgm:
            raise ValueError(
                'Conflicting mgms determined from path and passed argument: '
                'From path {0}: {1}, from argument: {2}'
                .format(path, _mgm, mgm)
                )
        mgm = _mgm
    elif mgm is None:
        mgm = get_default_mgm()
        lfn = path
    else:
        lfn = path
    # Sanity check
    if not lfn.startswith('/store'):
        raise ValueError(
            'LFN {0} does not start with \'/store\'; something is wrong'
            .format(lfn)
            )
    return mgm, lfn

def _join_mgm_lfn(mgm, lfn):
    """
    Joins mgm and lfn, ensures correct formatting.
    Will throw an exception of the lfn does not start with '/store'
    """
    if not lfn.startswith('/store'):
        raise ValueError(
            'This function expects filenames that start with \'/store\''
            )
    if not mgm.endswith('/'): mgm += '/'
    return mgm + lfn

def format(src, mgm=None):
    """
    Formats a path to ensure it is a path on the SE
    """
    mgm, lfn = split_mgm(src, mgm=mgm)
    return _join_mgm_lfn(mgm, lfn)

# _______________________________________________________
# Interactions with SE

def create_directory(directory):
    """
    Creates a directory on the SE
    Does not check if directory already exists
    """
    mgm, directory = split_mgm(directory)
    logger.warning('Creating directory on SE: {0}'.format(_join_mgm_lfn(mgm, directory)))
    cmd = [ 'xrdfs', mgm, 'mkdir', '-p', directory ]
    qondor.utils.run_command(cmd)

def isdir(directory):
    """
    Returns a boolean indicating whether the directory exists.
    Also returns False if the passed path is a file.
    """
    mgm, directory = split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', directory ]
    return qondor.utils.get_exitcode(cmd) == 0

def exists(path):
    """
    Returns a boolean indicating whether the path exists.
    """
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', path ]
    return qondor.utils.get_exitcode(cmd) == 0

def isfile(path):
    """
    Returns a boolean indicating whether the file exists.
    Also returns False if the passed path is a directory.
    """
    mgm, path = split_mgm(path)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ]
    status = qondor.utils.get_exitcode(cmd)
    # Error code 55 means path exists, but is not a directory
    return (status == 55)

def copy(src, dst, create_parent_directory=True):
    """
    Copies a file `src` to the storage element.
    Does not format `src` or `dst`; user is responsible for formatting.
    """
    logger.warning('Copying %s --> %s', src, dst)
    if create_parent_directory:
        cmd = [ 'xrdcp', '-s', '-p', src, dst ]
    else:
        cmd = [ 'xrdcp', '-s', src, dst ]
    qondor.utils.run_command(cmd)

def ls(path):
    """
    Lists all files and directories in a directory on the se
    """
    mgm, path = split_mgm(path)
    status = qondor.utils.get_exitcode([ 'xrdfs', mgm, 'stat', '-q', 'IsDir', path ])
    if status == 55:
        # It's a file; just return the path to the file
        return [_join_mgm_lfn(mgm, path)]
    elif status == 0:
        # It's a directory; return contents
        contents = qondor.utils.run_command([ 'xrdfs', mgm, 'ls', path ])
        return [ format(l.strip(), mgm=mgm) for l in contents if not len(l.strip()) == 0 ]
    else:
        raise RuntimeError('Path \'{0}\' does not exist'.format(path))

def ls_root(path):
    """
    Like ls but returns only paths that end with .root
    """
    contents = ls(path)
    root_files = [ f for f in contents if f.endswith('.root') ]
    root_files.sort()
    return root_files
