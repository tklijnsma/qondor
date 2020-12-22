#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import os
import os.path as osp
import pprint
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager

import qondor

logger = logging.getLogger("qondor")
subprocess_logger = logging.getLogger("subprocess")

# Python 2.6 compatibiltiy (see https://stackoverflow.com/a/13160748/9209944)
if "check_output" not in dir(subprocess):  # duck punch it in!
    logger.warning("Duck punching subprocess.check_output; suboptimal!")

    def f(*popenargs, **kwargs):
        if "stdout" in kwargs:
            raise ValueError("stdout argument not allowed, it will be overridden.")
        process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            raise subprocess.CalledProcessError(retcode, cmd)
        return output

    subprocess.check_output = f


def _create_directory_no_checks(dirname, dry=None):
    """
    Creates a directory without doing any further checks.

    :param dirname: Name of the directory to be created
    :type dirname: str
    :param dry: Don't actually create the directory, only log
    :type dry: bool, optional
    """
    if dry is None:
        dry = qondor.DRYMODE
    if not dry:
        logger.warning("Creating directory %s", dirname)
        os.makedirs(dirname)


def create_directory(dirname, renew=False, must_not_exist=False, dry=None):
    """
    Creates a directory if certain conditions are met.

    :param dirname: Name of the directory to be created
    :type dirname: str
    :param renew: Removes the directory `dirname` if it already exists
    :type renew: bool, optional
    :param must_not_exist: Throw an OSError if the directory already exists
    :type must_not_exist: bool, optional
    :param dry: Don't actually create the directory, only log
    :type dry: bool, optional
    """
    if dry is None:
        dry = qondor.DRYMODE
    if osp.isfile(dirname):
        raise OSError("{0} is a file".format(dirname))
    isdir = osp.isdir(dirname)

    if isdir:
        if must_not_exist:
            raise OSError("{0} must not exist but exists".format(dirname))
        elif renew:
            logger.warning("Deleting directory %s", dirname)
            if not dry:
                shutil.rmtree(dirname)
        else:
            logger.debug("%s already exists, not recreating", dirname)
            return
    _create_directory_no_checks(dirname, dry=dry)


def copy_file(src, dst, dry=None):
    if dry is None:
        dry = qondor.DRYMODE
    logger.info("Copying %s --> %s", src, dst)
    if not dry:
        shutil.copy(src, dst)


class switchdir(object):
    """
    Context manager to temporarily change the working directory.

    :param newdir: Directory to change into
    :type newdir: str
    :param dry: Don't actually change directory if set to True
    :type dry: bool, optional
    """

    def __init__(self, newdir, dry=None):
        super(switchdir, self).__init__()
        self.newdir = newdir
        self._backdir = os.getcwd()
        self._no_need_to_change = self.newdir == self._backdir
        self.dry = qondor.DRYMODE if dry is None else dry

    def __enter__(self):
        if self._no_need_to_change:
            logger.info("Already in right directory, no need to change")
            return
        logger.info("chdir to {0}".format(self.newdir))
        if not self.dry:
            os.chdir(self.newdir)

    def __exit__(self, type, value, traceback):
        if self._no_need_to_change:
            return
        logger.info("chdir back to {0}".format(self._backdir))
        if not self.dry:
            os.chdir(self._backdir)


def run_command(cmd, env=None, dry=None, shell=False):
    logger.warning(
        "Issuing command: {0}".format(" ".join(cmd) if not is_string(cmd) else cmd)
    )
    if dry is None:
        dry = qondor.DRYMODE
    if dry:
        return
    if shell and not is_string(cmd):
        cmd = " ".join(cmd)
    if env == "clean":
        env = get_clean_env()
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        universal_newlines=True,
        shell=shell,
    )

    output = []
    for stdout_line in iter(process.stdout.readline, ""):
        subprocess_logger.info(stdout_line.rstrip("\n"))
        output.append(stdout_line)
    process.stdout.close()
    process.wait()
    returncode = process.returncode

    if returncode == 0:
        logger.info("Command exited with status 0 - all good")
    else:
        logger.error("Exit status {0} for command: {1}".format(returncode, cmd))
        raise subprocess.CalledProcessError(cmd, returncode)
    return output


def run_multiple_commands(cmds, env=None, dry=None):
    logger.info("Sending cmds:\n{0}".format(pprint.pformat(cmds)))
    if dry is None:
        dry = qondor.DRYMODE
    if dry:
        logger.info("Dry mode - not running command")
        return

    try:
        # Python 3 accepts encoding
        process = subprocess.Popen(
            "bash",
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            bufsize=1,
            close_fds=True,
            encoding="utf8",
        )
    except TypeError:
        # Python 2 doesn't
        process = subprocess.Popen(
            "bash",
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            bufsize=1,
            close_fds=True,
            universal_newlines=True,
        )

    # Break on first error (stdin will still be written but execution will be stopped)
    process.stdin.write("set -e\n")
    process.stdin.flush()

    for cmd in cmds:
        if not (is_string(cmd)):
            cmd = " ".join(cmd)
        if not (cmd.endswith("\n")):
            cmd += "\n"
        process.stdin.write(cmd)
        process.stdin.flush()
    process.stdin.close()

    output = []
    process.stdout.flush()
    for line in iter(process.stdout.readline, ""):
        if len(line) == 0:
            break
        line = line.rstrip("\n")
        subprocess_logger.info(line)
        output.append(line)

    process.stdout.close()
    process.wait()
    returncode = process.returncode

    if returncode == 0:
        logger.info("Command exited with status 0 - all good")
        return output
    else:
        raise subprocess.CalledProcessError(cmd, returncode)


def download_url_to_str(url):
    """
    Downloads a url and puts the contents in a string
    """
    logger.info("Retrieving url %s", url)
    try:
        import urllib.request

        with urllib.request.urlopen(url) as f:
            html = f.read().decode("utf-8")
    except ImportError:
        import urllib2

        response = urllib2.urlopen("http://www.example.com/")
        html = response.read()
    return html


def iter_strip_comments(python_code):
    """
    Strips comments from python code as a string.
    Does *not* handle '#' appearing in a string
    """
    for line in python_code.split("\n"):
        line = line.split("#")[0].strip()
        if not len(line):
            continue
        yield line


def strip_comments(python_code):
    """
    As iter_strip_comments but returns a list
    """
    return "\n".join(iter_strip_comments(python_code))


class DummyFile(object):
    def __init__(self):
        # self.text = ''
        pass

    def write(self, text):
        # logger.debug('Writing: %s', text.replace('\n','\\n'))
        # self.text += text
        pass


@contextmanager
def openfile(*args, **kwargs):
    """
    Wrapper around the standard open(...) context, with the option of drymode
    """
    dry = kwargs.pop("dry", qondor.DRYMODE)
    try:
        if dry:
            yield DummyFile()
        else:
            with open(*args, **kwargs) as f:
                yield f
    finally:
        pass


def get_exitcode(cmd):
    if is_string(cmd):
        cmd = [cmd]
    logger.debug('Getting exit code for "%s"', " ".join(cmd))
    FNULL = open(os.devnull, "w")
    if qondor.DRYMODE:
        returncode = 0
    else:
        process = subprocess.Popen(cmd, stdout=FNULL, stderr=subprocess.STDOUT)
        process.communicate()[0]
        returncode = process.returncode
    logger.debug("Got exit code %s", returncode)
    return returncode


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


def pip_has_version(package):
    """
    For pip install strings: Checks if there is a part that mentions the version
    """
    for c in ["<", "=", ">"]:
        if c in package:
            return True
    return False


def pip_split_version(package):
    """
    For pip install strings: Splits the part of the package name and the version part
    """
    split_at_index = False
    for c in ["<", "=", ">"]:
        if c in package:
            index = package.index(c)
            if split_at_index is False or index < split_at_index:
                split_at_index = index
    if split_at_index:
        return package[:split_at_index], package[split_at_index:]
    return package, ""


def get_installation_path_of_module(module):
    logger.debug("Trying to determine installation path of %s", module.__name__)
    try:
        logger.debug("Using pkg_resources")
        import pkg_resources

        distribution = pkg_resources.get_distribution(module.__name__)
        path = osp.abspath(distribution.location)
    except Exception:
        logger.debug("From module.__path__")
        path = osp.abspath(module.__path__[0])
        if not osp.exists(path):
            logger.warning(
                "Path %s for module %s does not exist; reloading and trying again",
                path,
                module,
            )
            try:
                from importlib import reload
            except ImportError:
                # Probably this is python 2 then
                pass
            reload(module)
            path = osp.abspath(module.__path__[0])
            if osp.exists(path):
                logger.warning(
                    "Path %s for module %s exists. "
                    "Did you chdir before calling this function? "
                    "module.__path__ is a relative path set at import time.",
                    path,
                    module,
                )
            else:
                raise RuntimeError(
                    "Found no way of determining where %s is installed", module.__name__
                )
    logger.info("Determined installation path of %s: %s", module.__name__, path)
    return path


def tarball_python_module(
    module, outdir=None, allow_uncommitted=True, dry=None, assume_pypi=True
):
    """
    Takes a python module or the name of a module, and attempts to make an installable
    pypi-style package tarball out of it.
    If assume_pypi is True, it will look for the pip installation directory of the package, and check
    whether there is a setup.py.
    Otherwise, it will look for the top-level git repository and make a tarball from that, including
    only files that are tracked by git. Uncommitted changes are included, unless allowed_uncommitted
    is set to False.
    """
    import importlib

    if dry is None:
        dry = qondor.DRYMODE
    outdir = os.getcwd() if outdir is None else outdir
    outdir = osp.abspath(outdir)

    # Input variable may be a module name
    if is_string(module):
        try:
            module = importlib.import_module(module)
        except ImportError:
            logger.error(
                "Cannot make tarball for package {}; package is not importable".format(
                    module
                )
            )
            raise
    path = get_installation_path_of_module(module)

    # Make sure path exists and is a directory
    if not osp.exists(path):
        logger.error("Path %s does not seem to exist; cwd: %s", path, os.getcwd())
        raise OSError("{0} is not a valid path".format(path))
    elif osp.isfile(path):
        path = osp.dirname(path)

    if assume_pypi:
        # Make sure it has a setup.py
        setuppy = osp.join(path, "setup.py")
        if not osp.isfile(setuppy):
            raise OSError(
                "Error installing module {0}; Expected file {1} to exist; not a valid pypi package".format(
                    module, setuppy
                )
            )
        outfile = osp.join(outdir, osp.basename(path) + ".tar")
        logger.info("Creating tarball from directory %s --> %s", path, outfile)
        if not dry:
            with switchdir(path):
                run_command(
                    [
                        "tar",
                        "-cvf",
                        outfile,
                        ".",
                        "--exclude",
                        "*/lib/python*",
                        "--exclude",
                        "*/include/python*",
                        "--exclude",
                        "*/bin/python*",
                        "--exclude",
                        "*.egg-info*",
                        "--exclude",
                        "*.pyc",
                        "--exclude",
                        "*/.git",
                        "--exclude",
                        "*/dist/*",
                        "--exclude",
                        "*/.fcache/*",
                        "--exclude",
                        "*/examples/*",
                    ]
                )
    else:
        logger.info("Package %s: Using top level git to create a tarball", path)
        # Get the top-level git dir
        with switchdir(path):
            toplevel_git_dir = run_command(["git", "rev-parse", "--show-toplevel"])[
                0
            ].strip()
        # Fix the output name of the tarball
        outfile = osp.join(outdir, osp.basename(toplevel_git_dir) + ".tar")
        with switchdir(toplevel_git_dir):
            if allow_uncommitted:
                logger.info(
                    "Creating tarball for %s including uncommitted changes",
                    toplevel_git_dir,
                )
                # Create the tarball with uncommitted changes in it
                if not dry:
                    run_command(
                        "git ls-files -z | xargs -0 tar -cvf {0}".format(outfile),
                        shell=True,
                    )
            else:
                # Check if there are uncommitted changes
                try:
                    run_command(["git", "diff-index", "--quiet", "HEAD", "--"])
                except subprocess.CalledProcessError:
                    logger.error(
                        "Uncommitted changes detected; it is unlikely you want a tarball "
                        "with some changes not committed."
                    )
                    raise
                # Create the actual tarball of the latest commit
                if not dry:
                    run_command(["git", "archive", "-o", outfile, "HEAD"])
    logger.info("Created tarball {0}".format(outfile))
    return outfile


def extract_tarball(tarball, outdir=".", verbose=False):
    """
    Extracts a tarball to outdir
    """
    tarball = osp.abspath(tarball)
    outdir = osp.abspath(outdir)
    create_directory(outdir)
    logger.warning("Extracting {0} ==> {1}".format(tarball, outdir))
    cmd = ["tar", "-x{}f".format("v" if verbose else ""), tarball, "-C", outdir]
    run_command(cmd)


def extract_tarball_cmssw(tarball, outdir="."):
    """
    Extracts a tarball to outdir, and returns the extracted CMSSW dir
    """
    extract_tarball(tarball, outdir)
    # return the CMSSW directory
    if qondor.DRYMODE:
        return "CMSSW_dry"
    # Get the extracted directory from the tarball:
    import tarfile

    for name in tarfile.open(tarball).getnames():
        if name.startswith("CMSSW"):
            break
    else:
        raise RuntimeError(
            'Could not find any directory in {} that starts with "CMSSW"'.format(
                tarball
            )
        )
    return osp.join(outdir, name)


def check_is_cmssw_path(path):
    """
    Checks whether the passed path contains a CMSSW distribution.
    """
    abs_path = osp.abspath(path)
    if not osp.basename(path).startswith("CMSSW"):
        raise ValueError(
            'Expected {0} to start with "CMSSW" (path: {1})'.format(
                osp.basename(path), abs_path
            )
        )
    if not osp.isdir(path):
        raise OSError("{0} is not a directory (path: {1})".format(path, abs_path))
    if not osp.isdir(osp.join(path, "src")):
        raise OSError(
            "{0} is not a directory (path: {1})".format(osp.join(path, "src"), abs_path)
        )


def get_clean_env():
    env = os.environ.copy()
    for var in [
        "ROOTSYS",
        "PATH",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "SHLIB_PATH",
        "LIBPATH",
        "PYTHONPATH",
        "MANPATH",
        "CMAKE_PREFIX_PATH",
        "JUPYTER_PATH",
        # Added due to ROOT-env.sh
        "CPLUS_INCLUDE_PATH",
        "CXX",
        "ZLIB_HOME",
        "CURL_HOME",
        "DAVIX_HOME",
        "GSL_HOME",
        "SETUPTOOLS_HOME",
        "FONTCONFIG_HOME",
        "CAIRO_HOME",
        "SQLITE_HOME",
        "PIXMAN_HOME",
        "FREETYPE_HOME",
        "TBB_HOME",
        "FC",
        "PKG_CONFIG_HOME",
        "VC_HOME",
        "PNG_HOME",
        "FFTW_HOME",
        "BOOST_HOME",
        "VDT_HOME",
        "ROOT_HOME",
        "ZEROMQ_HOME",
        "LIBXML2_HOME",
        "PKG_CONFIG_PATH",
        "EXPAT_HOME",
        "COMPILER_PATH",
        "BLAS_HOME",
        "R_HOME",
        "XROOTD_HOME",
        "MYSQL_HOME",
        "GFAL_HOME",
        "CC",
        "C_INCLUDE_PATH",
        "PYTHON_HOME",
        "PYTHONHOME",
        "ORACLE_HOME",
        "GPERF_HOME",
        "SRM_IFCE_HOME",
        "NUMPY_HOME",
        "DCAP_HOME",
    ]:
        if var in env:
            del env[var]
    return env


def convert_to_utc(local_time):
    """
    Converts a local time to UTC.
    Implemented for only a few basic timezones.
    Needs to be extended with pytz if this function
    get seriously used.
    """
    # See: https://stackoverflow.com/a/10854983/9209944
    delta = datetime.timedelta(
        seconds=time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    )
    new_time = local_time + delta
    logger.debug(
        "Offsetting %s by %s hours to get to UTC %s", local_time, delta, new_time
    )
    return new_time


def get_now_utc():
    return convert_to_utc(datetime.datetime.now())


def sleep_until(runtime_utc, allowed_lateness=300, is_not_utc=False):
    if is_not_utc:
        runtime_utc = convert_to_utc(runtime_utc)
    now_utc = get_now_utc()
    logger.info("Current time (UTC):       %s", now_utc)
    logger.info("Scheduled run time (UTC): %s", runtime_utc)

    delta = runtime_utc - now_utc
    delta_seconds = abs(delta.total_seconds())

    if delta < datetime.timedelta(seconds=0):
        # The job is too late; runtime_utc has already passed
        if delta_seconds < allowed_lateness:
            logger.info(
                "Job is late by %s seconds, which is within the allowed window"
                " of max %s seconds late",
                delta_seconds,
                allowed_lateness,
            )
            return 0
        else:
            logger.error(
                "Job is late by %s seconds, which is OUTSIDE the allowed window"
                " of max %s seconds late - throwing exception",
                delta_seconds,
                allowed_lateness,
            )
            raise RuntimeError
    else:
        logger.info("Job is early by %s seconds, sleeping", delta_seconds)
        time.sleep(delta_seconds)
        return 0


def check_proxy():
    """
    Asserts that the user has a grid proxy that is valid for at least 168 more hours (1 week)
    """
    # cmd = 'voms-proxy-info -exists -valid 168:00' # Check if there is an existing proxy for a full week
    if qondor.DRYMODE:
        return
    try:
        subprocess.check_output(["grid-proxy-info", "-exists", "-valid", "168:00"])
        logger.info("Found a valid proxy")
    except subprocess.CalledProcessError:
        logger.error(
            "Grid proxy is not valid for at least 1 week. Renew it using:\n"
            "voms-proxy-init -voms cms -valid 192:00"
        )
        raise
    except OSError:
        logger.error(
            "The grid-proxy-info is not found or behaves weird; "
            "Continuing but note that your grid proxy may not be valid"
        )
        pass


def dist_is_editable(dist):
    """
    Is distribution an editable install?
    see: https://stackoverflow.com/a/42583363/9209944
    """
    # If a string is passed, convert it to a module object
    if is_string(dist):
        import pkg_resources

        try:
            dist = pkg_resources.get_distribution(dist)
        except pkg_resources.DistributionNotFound:
            logger.info(
                "Package %s is not installed; assuming non-editable install", dist
            )
            return False
    # Check if the module's .egg-link is somewhere on the python path
    for path_item in sys.path:
        egg_link = osp.join(path_item, dist.project_name + ".egg-link")
        if osp.isfile(egg_link):
            return True
    return False


def _iter_chunkify_nrange(list_length, n_chunks):
    """
    Makes n_chunks chunks out of a range(list_length) list.
    Returns empty lists if n_chunks > list_length.
    """
    n_per_chunk_f = float(list_length) / n_chunks
    boundaries = [(i * n_per_chunk_f, (i + 1) * n_per_chunk_f) for i in range(n_chunks)]
    for left, right in boundaries:
        indices_in_chunk = []
        for i in range(list_length):
            if i >= left and i < right:
                indices_in_chunk.append(i)
        yield indices_in_chunk


def iter_chunkify(mylist, n_chunks):
    """
    Makes n_chunks chunks out of mylist.
    """
    for indices in _iter_chunkify_nrange(len(mylist), n_chunks):
        yield [mylist[i] for i in indices]


def chunkify(mylist, n_chunks):
    return list(iter_chunkify(mylist, n_chunks))


def get_ith_chunk(mylist, n_chunks, i_chunk):
    return chunkify(mylist, n_chunks)[i_chunk]
