#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, shutil, logging, subprocess, glob
import os.path as osp
import qondor
logger = logging.getLogger('qondor')
subprocess_logger = logging.getLogger('subprocess')


def _create_directory_no_checks(dirname, dry=False):
    """
    Creates a directory without doing any further checks.

    :param dirname: Name of the directory to be created
    :type dirname: str
    :param dry: Don't actually create the directory, only log
    :type dry: bool, optional
    """
    logger.warning('Creating directory {0}'.format(dirname))
    if not dry: os.makedirs(dirname)

def create_directory(dirname, force=False, must_not_exist=False, dry=False):
    """
    Creates a directory if certain conditions are met.

    :param dirname: Name of the directory to be created
    :type dirname: str
    :param force: Removes the directory `dirname` if it already exists
    :type force: bool, optional
    :param must_not_exist: Throw an OSError if the directory already exists
    :type must_not_exist: bool, optional
    :param dry: Don't actually create the directory, only log
    :type dry: bool, optional
    """
    if osp.isfile(dirname):
        raise OSError('{0} is a file'.format(dirname))
    isdir = osp.isdir(dirname)

    if isdir:
        if must_not_exist:
            raise OSError('{0} must not exist but exists'.format(dirname))
        elif force:
            logger.warning('Deleting directory {0}'.format(dirname))
            if not dry: shutil.rmtree(dirname)
        else:
            logger.warning('{0} already exists, not recreating')
            return
    _create_directory_no_checks(dirname, dry=dry)

def copy_file(src, dst, dry=False):
    logger.info('Copying %s --> %s', src, dst)
    if not dry: shutil.copy(src, dst)

class switchdir(object):
    """
    Context manager to temporarily change the working directory.

    :param newdir: Directory to change into
    :type newdir: str
    :param dry: Don't actually change directory if set to True
    :type dry: bool, optional
    """
    def __init__(self, newdir, dry=False):
        super(switchdir, self).__init__()
        self.newdir = newdir
        self._backdir = os.getcwd()
        self._no_need_to_change = (self.newdir == self._backdir)
        self.dry = dry

    def __enter__(self):
        if self._no_need_to_change:
            logger.info('Already in right directory, no need to change')
            return
        logger.info('chdir to {0}'.format(self.newdir))
        if not self.dry: os.chdir(self.newdir)

    def __exit__(self, type, value, traceback):
        if self._no_need_to_change:
            return
        logger.info('chdir back to {0}'.format(self._backdir))
        if not self.dry: os.chdir(self._backdir)

def run_command(cmd, env=None, dry=False, shell=False):
    logger.warning('Issuing command: {0}'.format(' '.join(cmd)))
    if dry: return

    if shell:
        cmd = ' '.join(cmd)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        universal_newlines=True,
        shell=shell
        )

    output = []
    for stdout_line in iter(process.stdout.readline, ""):
        subprocess_logger.info(stdout_line.rstrip('\n'))
        output.append(stdout_line)
    process.stdout.close()
    process.wait()
    returncode = process.returncode

    if returncode == 0:
        logger.info('Command exited with status 0 - all good')
    else:
        logger.error('Exit status {0} for command: {1}'.format(returncode, cmd))
        raise subprocess.CalledProcessError(cmd, returncode)
    return output


def run_multiple_commands(cmds, env=None, dry=False):
    logger.info('Sending cmds:\n{0}'.format(pprint.pformat(cmds)))
    if dry:
        logger.info('Dry mode - not running command')
        return

    process = subprocess.Popen(
        'bash',
        stdin = subprocess.PIPE,
        stdout = subprocess.PIPE,
        stderr = subprocess.STDOUT,
        env = env,
        bufsize = 1,
        close_fds = True
        )

    # Break on first error (stdin will still be written but execution will be stopped)
    process.stdin.write('set -e\n')
    process.stdin.flush()

    for cmd in cmds:
        if not(type(cmd) is str):
            cmd = ' '.join(cmd)
        if not(cmd.endswith('\n')):
            cmd += '\n'
        process.stdin.write(cmd)
        process.stdin.flush()
    process.stdin.close()

    process.stdout.flush()
    for line in iter(process.stdout.readline, ""):
        if len(line) == 0: break
        subprocess_logger.info(line.rstrip('\n'))

    process.stdout.close()
    process.wait()
    returncode = process.returncode

    if (returncode == 0):
        logger.info('Command exited with status 0 - all good')
    else:
        raise subprocess.CalledProcessError(cmd, returncode)


def is_string(string):
    """
    Checks strictly whether `string` is a string
    Python 2 / 3 compatibility (https://stackoverflow.com/a/22679982/9209944)
    """
    try:
        basestring
    except NameError:
        basestring = str
    return isinstance(string, basestring)


def tarball_python_module(module, outdir=None, ignore_uncommitted=False, dry=False):
    """
    Takes a python module or a path to a file of said module, goes to the associated
    top-level git directory, and creates a tarball.
    Will throw subprocess.CalledProcessError if there are uncommitted changes.
    """
    # Input variable may be a path
    if is_string(module):
        # Treat the input variable as a path
        path = module
    else:
        # path = module.__file__
        path = osp.abspath(module.__path__[0])
        if not osp.exists(path):
            logger.warning(
                'Path %s for module %s does not exist; reloading and trying again',
                path, module
                )
            import importlib
            reload(module)
            path = osp.abspath(module.__path__[0])
            if osp.exists(path):
                logger.warning(
                    'Path %s for module %s exists. '
                    'Did you chdir before calling this function? '
                    'module.__path__ is a relative path set at import time.',
                    path, module
                    )

    # Make sure path exists and is a directory
    if not osp.exists(path):
        logger.error('Path %s does not seem to exist; cwd: %s', path, os.getcwd())
        raise OSError('{0} is not a valid path'.format(path))
    elif osp.isfile(path):
        path = osp.dirname(path)

    # Get the top-level git dir
    with switchdir(path):
        toplevel_git_dir = run_command(['git', 'rev-parse', '--show-toplevel'])[0].strip()

    # Fix the output name of the tarball
    outdir = os.getcwd() if outdir is None else outdir
    outfile = osp.join(osp.abspath(outdir), osp.basename(toplevel_git_dir) + '.tar')

    with switchdir(toplevel_git_dir):
        if not ignore_uncommitted:
            # Check if there are uncommitted changes
            try:
                run_command(['git', 'diff-index', '--quiet', 'HEAD', '--'])
            except subprocess.CalledProcessError:
                logger.error(
                    'Uncommitted changes detected; it is unlikely you want a tarball '
                    'with some changes not committed.'
                    )
                raise
        # Create the actual tarball of the latest commit
        if not dry: run_command(['git', 'archive', '-o', outfile, 'HEAD'])
        logger.info('Created tarball {0}'.format(outfile))
        return outfile


def extract_tarball(tarball, outdir='.', dry=False):
    """
    Extracts a tarball to outdir
    """
    tarball = osp.abspath(tarball)
    outdir = osp.abspath(outdir)
    logger.warning(
        'Extracting {0} ==> {1}'
        .format(tarball, outdir)
        )
    cmd = [
        'tar', '-xvf', tarball,
        '-C', outdir
        ]
    run_command(cmd, dry=dry)


def extract_tarball_cmssw(tarball, outdir='.', dry=False):
    """
    Extracts a tarball to outdir, and returns the extracted CMSSW dir
    """
    extract_tarball(tarball, outdir, dry)
    # return the CMSSW directory
    if dry: return 'CMSSW_dry'
    return [ d for d in glob.glob(osp.join(outdir, 'CMSSW*')) if not d.endswith('.gz')][0]


def check_is_cmssw_path(path):
    """
    Checks whether the passed path contains a CMSSW distribution.
    """
    abs_path = osp.abspath(path)
    if not osp.basename(path).startswith('CMSSW'):
        raise ValueError(
            'Expected {0} to start with "CMSSW" (path: {1})'
            .format(osp.basename(path), abs_path)
            )
    if not osp.isdir(path):
        raise OSError(
            '{0} is not a directory (path: {1})'
            .format(path, abs_path)
            )
    if not osp.isdir(osp.join(path, 'src')):
        raise OSError(
            '{0} is not a directory (path: {1})'
            .format(osp.join(path, 'src'), abs_path)
            )
