# -*- coding: utf-8 -*-
import qondor
import logging, os, os.path as osp, pprint, shutil, uuid, re
from time import strftime
from contextlib import contextmanager
import seutils
logger = logging.getLogger('qondor')


class SHFile(object):
    """
    Writes a .sh file based on a preprocessing object.
    """
    def __init__(self, preprocessing, python_script_args=None):
        self.preprocessing = preprocessing
        self.python_script_args = python_script_args

    def to_file(self, filename, dry=None):
        if dry is None: dry = qondor.DRYMODE
        sh = '\n'.join(self.parse())
        logger.info('Parsed the following .sh file:\n%s', sh)
        logger.info('Writing to %s', filename)
        if not dry:
            with open(filename, 'w') as f:
                f.write(sh)

    def parse(self):
        lines = []
        lines.extend(self.initialize())
        lines.extend(self.set_env_variables())
        lines.extend(self.set_cms_env_and_pip())
        lines.extend(self.pip_installations())
        lines.extend(self.sleep_until_runtime())
        lines.extend(self.python_call())
        return lines

    def initialize(self):
        lines = [
            '#!/bin/bash',
            'set -e',
            'echo "hostname: $(hostname)"',
            'echo "date:     $(date)"',
            'echo "pwd:      $(pwd)"',
            'echo "Redirecting all output to stderr from here on out"',
            'exec 1>&2',
            ''
            ]
        return lines

    def set_env_variables(self):
        lines = []
        for key, value in self.preprocessing.env.items():
            lines.append('export {0}={1}'.format(key, value))
        lines.append('')
        return lines

    def set_cms_env_and_pip(self):
        lines = [
            'export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/',
            'source /cvmfs/cms.cern.ch/cmsset_default.sh',
            'source "${gccsetup}"',
            'source "${rootsetup}"',
            'export PATH="${pipdir}/bin:${PATH}"',
            'export PYTHONPATH="${pipdir}/lib/python2.7/site-packages/:${PYTHONPATH}"',
            '',
            'set -uxoE pipefail',
            'echo "Setting up custom pip install dir"',
            'HOME="$(pwd)"',
            'pip(){ ${pipdir}/bin/pip "$@"; }  # To avoid any local pip installations',
            'export pip_install_dir="$(pwd)/install"',
            'mkdir -p "${pip_install_dir}/bin"',
            'mkdir -p "${pip_install_dir}/lib/python2.7/site-packages"',
            'export PATH="${pip_install_dir}/bin:${PATH}"',
            'export PYTHONPATH="${pip_install_dir}/lib/python2.7/site-packages:${PYTHONPATH}"',
            '',
            'pip -V',
            'which pip',
            ''
            ]
        return lines

    def pip_installations(self):
        lines = []
        for package, install_instruction in self.preprocessing.pip:
            package = package.replace('.', '-').rstrip('/')
            if install_instruction == 'module-install':
                # Editable install: Manually give tarball, extract, and install
                lines.extend([
                    'mkdir {0}'.format(package),
                    'tar xf {0}.tar -C {0}'.format(package),
                    'pip install --install-option="--prefix=${{pip_install_dir}}" -e {0}/'.format(package)
                    ])
            elif install_instruction == 'pypi-install':
                # Non-editable install from pypi
                lines.append(
                    'pip install --install-option="--prefix=${{pip_install_dir}}" {0}'.format(package)
                    )
        return lines

    def sleep_until_runtime(self):
        if not(self.preprocessing.delayed_runtime is None):
            if not(self.preprocessing.allowed_lateness is None):
                allowed_lateness = '--allowed_lateness {0}'.format(self.preprocessing.allowed_lateness)
            else:
                allowed_lateness = ''
            return [
                'qondor-sleepuntil "{0}" {1}'.format(
                    self.preprocessing.delayed_runtime.strftime('%Y-%m-%d %H:%M:%S'),
                    allowed_lateness
                    ),
                ''
                ]
        else:
            return []

    def python_call(self):
        python_cmd = 'python {0}'.format(osp.basename(self.preprocessing.filename))
        if self.python_script_args:
            # Add any arguments for the python script to this line
            try:  # py3
                from shlex import quote
            except ImportError:  # py2
                from pipes import quote
            python_cmd += ' ' + ' '.join([quote(s) for s in self.python_script_args])
        lines = [
            python_cmd,
            'echo "$?" > exitcode_${CONDOR_CLUSTER_NUMBER}_${CONDOR_PROCESS_ID}.txt', # Store the python exit code in a file
            ''
            ]
        return lines


class BaseSubmitter(object):
    """
    Base class for Submitter: Translates user input to one of the following:
    - a submission dict + the number of jobs to submit
    - a submission dict + a list of items
    - a submission dict + a list of chunks
    , and creates a directory with the needed input files for the job
    """

    def __init__(self, python_script_args=None):
        super(BaseSubmitter, self).__init__()
        self.transfer_files = []
        self.python_script_args = python_script_args
        self._created_python_module_tarballs = []

    # _________________________________________________________
    # Methods to prep submission dict

    def format_for_htcondor_interface(self, shfile, preprocessor):
        """
        Formats a submission dict and possibly an extra argument (njobs/items/chunks).
        Returns a tuple (submissiondict, extra_settings), where the extra_settings is
        a dictionary with the following possible keys:
        - 'items' in case there were items found in the preprocessing
        - 'rootfile_chunks' in case there were rootfile_chunks found in the preprocessing
        - 'njobs' in case the njobs variable was set in the preprocessing
        - 'njobsmax'?
        """
        extra_submission_settings = {}
        sub = qondor.schedd.get_default_sub()
        sub['executable'] =  osp.basename(shfile)
        sub['+QondorRundir']  =  '"' + self.rundir + '"'
        sub['environment']['QONDORISET'] = str(preprocessor.subset_index)
        # Overwrite htcondor keys defined in the preprocessing
        sub.update(preprocessor.htcondor)
        # Flatten files into a string, excluding files on storage elements
        transfer_files = self.transfer_files + [f for f in preprocessor.files.values() if not seutils.has_protocol(f)]
        if len(transfer_files) > 0:
            sub['transfer_input_files'] = ','.join(transfer_files)
        # Otherwise continue processing items
        if preprocessor.items and preprocessor.rootfile_chunks:
            raise ValueError(
                'Both regular items and rootfiles-split-by-entries are specified; '
                'This is not supported.'
                )
        elif preprocessor.items:
            logger.info('Items:\n%s', pprint.pformat(preprocessor.items))
            extra_submission_settings['items'] = preprocessor.items
        elif preprocessor.rootfile_chunks:
            logger.info('Items as rootfile chunks:\n%s', pprint.pformat(preprocessor.rootfile_chunks))
            extra_submission_settings['rootfile_chunks'] = preprocessor.rootfile_chunks
        else:
            # Simple case with no items; just use the njobs variable with a default of 1
            if 'njobs' in preprocessor.variables:
                extra_submission_settings['njobs'] = int(preprocessor.variables['njobs'])
        return sub, extra_submission_settings

    # _________________________________________________________
    # Prep functions for files needed for the job

    def tar_python_module(self, module_name):
        """
        Creates a tarball of a python module that is needed inside the job
        """
        if module_name in self._created_python_module_tarballs:
            logger.debug('Not tarring up %s again, tarball already created', module_name)
            return
        logger.info('Creating tarball for python module %s', module_name)
        import importlib
        module = importlib.import_module(module_name)
        tarball = qondor.utils.tarball_python_module(
            module,
            outdir = self.rundir,
            )
        self.transfer_files.append(tarball)
        self._created_python_module_tarballs.append(module_name)

    def make_rundir(self):
        """
        Creates a directory in from which to submit the job
        """
        self.rundir = osp.abspath('qondor_{0}_{1}'.format(
            self.python_name,
            strftime(qondor.TIMESTAMP_FMT)
            ))
        qondor.utils.create_directory(
            self.rundir,
            must_not_exist=True,
            )

    def dump_ls_cache_file(self):
        """
        Creates the ls cache file for the job
        """
        cache_file = osp.join(self.rundir, qondor.Preprocessor.LS_CACHE_FILE)
        qondor.Preprocessor.dump_ls_cache(cache_file)
        self.transfer_files.append(cache_file)

    def dump_rootcache(self):
        """
        Dumps the current state of the seutils root cache to a file to be used in the job
        """
        if seutils.root.USE_CACHE:
            rootcache_file = osp.join(self.rundir, 'rootcache.tar.gz')
            if not qondor.DRYMODE: seutils.root.cache_to_file(rootcache_file)
            self.transfer_files.append(rootcache_file)

    def iter_submissions(self):
        """
        Does all the prep work for the submission directory, and then
        loops over all sets and yields (submission_dict, extra_settings) as
        returned by self.format_for_htcondor_interface
        """
        # First do all prep
        at_least_one_job_submitted = False
        try:
            self.make_rundir()
            self.copy_python_file()
            self.dump_ls_cache_file()
            self.dump_rootcache()
            # Loop over all 'sets'
            # If there are no subsets, this is just a len(1) iterator of the preprocessing
            for i_set, preprocessor in enumerate(self.preprocessing.sets()):
                # Create tarballs for local python modules
                for package, install_instruction in preprocessor.pip:
                    if install_instruction == 'module-install':
                        self.tar_python_module(package)
                # Create the bash script entry point for this job
                shfile = osp.join(self.rundir, '{}_{}.sh'.format(self.python_name, i_set))
                SHFile(preprocessor, self.python_script_args).to_file(shfile)
                # Submit jobs to htcondor
                sub, extra_settings = self.format_for_htcondor_interface(shfile, preprocessor)
                yield sub, extra_settings, preprocessor
                at_least_one_job_submitted = True
        except StopIteration:
            pass
        except Exception as e:
            logger.error('Exception %s was raised', e)
            if not at_least_one_job_submitted:
                logger.error('Error during submission; cleaning up %s', self.rundir)
                if osp.isdir(self.rundir) and not qondor.DRYMODE: shutil.rmtree(self.rundir)
            raise

    def submissions(self):
        return list(iter_submissions)


class Submitter(BaseSubmitter):
    """
    Standard Submitter based on a python file.
    Upon running `.submit()`, will create a new directory,
    transfer all relevant files from the job, and start
    running.
    """
    def __init__(self, python_file, python_script_args=None):
        super(Submitter, self).__init__(python_script_args=python_script_args)
        self.original_python_file = osp.abspath(python_file)
        self.python_base = osp.basename(self.original_python_file)
        self.python_name = self.python_base.replace('.py','')
        self.preprocessing = qondor.Preprocessor(self.original_python_file)

    def copy_python_file(self):
        self.python_file = osp.join(self.rundir, self.python_base)
        qondor.utils.copy_file(self.original_python_file, self.python_file)
        self.transfer_files.append(self.python_file)


class CodeSubmitter(BaseSubmitter):
    """
    Like submitter, but rather than being instantiated from a python file
    it is instantiated from python code in a string
    """
    def __init__(self, python_code, preprocessing_code=None, name='', dry=False):
        super(CodeSubmitter, self).__init__(dry)
        self.python_code = python_code.split('\n') if qondor.utils.is_string(python_code) else python_code
        if preprocessing_code is None: preprocessing_code = []
        self.preprocessing_code = preprocessing_code.split('\n') if qondor.utils.is_string(preprocessing_code) else preprocessing_code
        self.python_name = name if name else 'fromcode'
        self.preprocessing = qondor.Preprocessor.from_lines(self.preprocessing_code)

    def copy_python_file(self):
        self.python_file = osp.join(self.rundir, 'pythoncode.py')
        logger.info('Writing %s lines of code to %s', len(self.python_code), self.python_file)
        with open(self.python_file, 'w') as f:
            f.write('\n'.join(['#$ ' + l for l in self.preprocessing_code]))
            f.write('\n')
            f.write('\n'.join(self.python_code))
        self.transfer_files.append(self.python_file)
        self.preprocessing.filename = self.python_file

    def run_local_exec(self):
        logger.warning('Doing exec() on the following code:\n%s', '\n'.join(self.python_code))
        if '__file__' in self.python_code:
            logger.error(
                'Note that "__file__" will crash, as it is not defined in exec(). '
                'Use .run_local() instead.'
                )
        logger.warning('Output:')
        exec('\n'.join(self.python_code))

    def run_local(self):
        python_file = 'temp-{0}.py'.format(uuid.uuid4())
        logger.info('Put python code in {0} and running:'.format(python_file))
        try:
            with open(python_file, 'w') as f:
                f.write('\n'.join(['#$ ' + l for l in self.preprocessing_code]))
                f.write('\n')
                f.write('\n'.join(self.python_code))
            return qondor.utils.run_command(['python', python_file])
        finally:
            logger.info('Removing %s', python_file)
            os.remove(python_file)


def cmsconnect_settings(sub, preprocessor=None, cli=False):
    """
    Adds special cmsconnect settings to submission dict in order to submit
    on cmsconnect via the python bindings, or in case of submitting by the
    command line, to set the DESIRED_Sites key.
    Modifies the dict in place.
    """
    # Read the central config for cmsconnect
    try:
        from configparser import RawConfigParser # python 3
    except ImportError:
        import ConfigParser # python 2
        RawConfigParser = ConfigParser.RawConfigParser
    cfg = RawConfigParser()
    cfg.read('/etc/ciconnect/config.ini')
    sites = cfg.get('submit', 'DefaultSites').split(',')
    all_sites = set(sites)
    # Check whether the user whitelisted or blacklisted some sites
    if preprocessor:
        import fnmatch
        blacklisted = []
        whitelisted = []
        # Build the blacklist
        if 'sites_blacklist' in preprocessor.variables:
            for blacksite in preprocessor.variables['sites_blacklist'].split():
                for site in sites:
                    if fnmatch.fnmatch(site, blacksite):
                        blacklisted.append(site)
        # Build the whitelist
        if 'sites_whitelist' in preprocessor.variables:
            for whitesite in preprocessor.variables['sites_whitelist'].split():
                for site in sites:
                    if fnmatch.fnmatch(site, whitesite):
                        whitelisted.append(site)
        blacklisted = list(set(blacklisted))
        blacklisted.sort()
        whitelisted = list(set(whitelisted))
        whitelisted.sort()
        logger.info('Blacklisting: %s', ','.join(blacklisted))
        logger.info('Whitelisting: %s', ','.join(whitelisted))
        sites = list( (set(sites) - set(blacklisted)).union(set(whitelisted)) )
        sites.sort()
    logger.info('Submitting to sites: %s', ','.join(sites))
    # Add a plus only if submitting via .jdl file
    addplus = lambda key: '+' + key if cli else key
    if all_sites != set(sites):
        sub[addplus('DESIRED_Sites')] = '"' + ','.join(sites) + '"'
    if not cli:
        sub[addplus('ConnectWrapper')] = '"2.0"'
        sub[addplus('CMSGroups')] = '"/cms,T3_US_FNALLPC"'
        sub[addplus('MaxWallTimeMins')] = '500'
        sub[addplus('ProjectName')] = '"cms.org.fnal"'
        sub[addplus('SubmitFile')] = '"irrelevant.jdl"'
        sub[addplus('AccountingGroup')] = '"analysis.{0}"'.format(os.environ['USER'])
        logger.warning('FIXME: CMS Connect settings currently hard-coded for a FNAL user')
