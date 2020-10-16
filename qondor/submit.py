# -*- coding: utf-8 -*-
import qondor
import logging, os, os.path as osp, pprint, shutil, uuid, re, json
from datetime import datetime
from time import strftime
from contextlib import contextmanager
import seutils
logger = logging.getLogger('qondor')


def split_runcode_submitcode(lines):
    """
    Splits a list of lines into two strings, the python to run in the job
    and the other the python code to submit the jobs.
    """
    runcode_lines = []
    submitcode_lines = []
    is_runcode_mode = True
    is_submitcode_mode = False
    for line in lines:
        if line.startswith('"""# submit'):
            if is_submitcode_mode:
                raise Exception(
                    'Encountered submit code opening tag, but was already in submit mode'
                    )
            # Toggle the mode
            is_runcode_mode = not(is_runcode_mode)
            is_submitcode_mode = not(is_submitcode_mode)
            continue
        elif line.startswith('"""# endsubmit'):
            if is_runcode_mode:
                raise Exception(
                    'Encountered submit code closing tag, but was already not in submit mode'
                    )
            # Toggle the mode
            is_runcode_mode = not(is_runcode_mode)
            is_submitcode_mode = not(is_submitcode_mode)
            continue
        else:
            if is_runcode_mode:
                runcode_lines.append(line)
            else:
                submitcode_lines.append(line)
    submitcode = '\n'.join(submitcode_lines)
    runcode = '\n'.join(runcode_lines)
    return runcode, submitcode

def split_runcode_submitcode_file(filename):
    """
    Wrapper for split_runcode_submitcode that opens up the file first
    """
    with open(filename, 'r') as f:
        return split_runcode_submitcode(f.readlines())

def submit_python_job_file(filename, cli=False, njobsmax=None, run_args=None):
    """
    Builds submission clusters from a python job file
    """
    runcode, submitcode = split_runcode_submitcode_file(filename)
    # Run the submitcode
    # First create exec scope dict, with a few handy functions
    session = Session(name=osp.basename(filename).replace('.py',''))
    def submit_fn(*args, **kwargs):
        cluster = Cluster(runcode, *args, session=session, run_args=run_args, **kwargs)
        session.add_submission(cluster)
    def submit_now_fn():
        session.submit(cli)
    exec_scope = {
        'qondor' : qondor,
        'session' : session,
        'submit' : submit_fn,
        'submit_now' : submit_now_fn,
        'runcode' : runcode
        }
    logger.warning('Running submission code now')
    exec(submitcode, exec_scope)
    # Do a submit_now call in case it wasn't done yet (if submit_now was called in the submit code this is a no-op)
    session.submit(cli)

class StopProcessing(Exception):
    """
    Special exception to stop execution inside an exec statement
    """
    pass

def get_first_cluster(filename):
    """
    Returns the first cluster that would be submitted if the python job file
    would be submitted
    """
    runcode, submitcode = split_runcode_submitcode_file(filename)
    cluster = [None]
    def submit_fn(*args, **kwargs):
        cluster[0] = Cluster(runcode, *args, **kwargs)
        raise StopProcessing
    def submit_now_fn():
        session.submit(cli)
    exec_scope = {
        'cluster' : cluster,
        'qondor' : qondor,
        'submit' : submit_fn,
        'submit_now' : submit_now_fn,
        'runcode' : runcode
        }
    logger.warning('Running submission code now')
    try:
        exec(submitcode, exec_scope)
    except StopProcessing:
        pass
    cluster = cluster[0]
    if cluster is None:
        raise Exception('No cluster was submitted in the submit code')
    return cluster


RUN_ENVS = {
    'sl7-py27' : [
        'export pipdir="/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-centos7-gcc7-opt"',
        'export SCRAM_ARCH="slc7_amd64_gcc820"',
        'source /cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh',
        'source /cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-centos7-gcc7-opt/ROOT-env.sh',
        'export PATH="${pipdir}/bin:${PATH}"',
        'export PYTHONPATH="${pipdir}/lib/python2.7/site-packages/:${PYTHONPATH}"',
        ],
    'sl6-py27' : [
        'export pipdir="/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-slc6-gcc7-opt"',
        'export SCRAM_ARCH="slc6_amd64_gcc700"',
        'source /cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-slc6-gcc7-opt/setup.sh',
        'source /cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-slc6-gcc7-opt/ROOT-env.sh',
        'export PATH="${pipdir}/bin:${PATH}"',
        'export PYTHONPATH="${pipdir}/lib/python2.7/site-packages/:${PYTHONPATH}"',
        ],
    }

class SHFile():
    pass


def get_default_sub(submission_time=None):
    """
    Returns the default submission dict (the equivalent of a .jdl file)
    to be used by the submitter.
    """
    if submission_time is None: submission_time = datetime.now()
    sub = {
        'universe' : 'vanilla',
        'output' : 'out_$(Cluster)_$(Process).txt',
        'error' : 'err_$(Cluster)_$(Process).txt',
        'log' : 'log_$(Cluster)_$(Process).txt',
        'should_transfer_files' : 'YES',
        'environment' : {
            'QONDOR_BATCHMODE' : '1',
            'CONDOR_CLUSTER_NUMBER' : '$(Cluster)',
            'CONDOR_PROCESS_ID' : '$(Process)',
            'CLUSTER_SUBMISSION_TIMESTAMP' : submission_time.strftime(qondor.TIMESTAMP_FMT),
            },
        }
    # Try to set some more things
    try:
        sub['x509userproxy'] = os.environ['X509_USER_PROXY']
    except KeyError:
        try:
            sub['x509userproxy'] = qondor.utils.run_command(['voms-proxy-info', '-path'])[0].strip()
            logger.info('Set x509userproxy to "%s" based on output from voms-proxy-info', sub['x509userproxy'])
        except:
            logger.warning(
                'Could not find a x509userproxy to pass; manually '
                'set the htcondor variable \'x509userproxy\' if your '
                'htcondor setup requires it.'
                )
    try:
        sub['environment']['USER'] = os.environ['USER']
    except KeyError:
        # No user specified, no big deal
        pass
    return sub


class Session(object):
    """
    Over-arching object that controls submission of a number of clusters
    """
    def __init__(self, name=None):
        self.submission_time = datetime.now()
        name = 'qondor_' + name if name else 'qondor'
        self.rundir = osp.abspath('{}_{}'.format(name, self.submission_time.strftime(qondor.TIMESTAMP_FMT)))
        self.transfer_files = []
        self._created_python_module_tarballs = {}
        self._i_seutils_tarball = 0
        self.submittables = []

    def dump_seutils_cache(self):
        """
        Dumps the seutils cache to a tarball, to be included in the job.
        """
        proposed_tarball = osp.join(self.rundir, 'seutils-cache-{}.tar.gz'.format(self._i_seutils_tarball))
        # If no new seutils calls were made, the tarball won't be remade
        with seutils.drymode_context(qondor.DRYMODE):
            actual_tarball = seutils.tarball_cache(proposed_tarball, only_if_updated=True)
        if actual_tarball == proposed_tarball: self._i_seutils_tarball += 1
        logger.info('Using seutils-cache tarball %s', actual_tarball)
        return actual_tarball

    def handle_python_package_tarballs(self, cluster):
        # Put in python package tarballs required for the code in the job
        for package, install_instruction in cluster.pips:
            # Determine whether package was installed editably
            if install_instruction == 'auto' and qondor.utils.dist_is_editable(package): install_instruction = 'editable-install'
            # If package was installed editably, tarball it up and include it
            if install_instruction == 'editable-install':
                # Create the tarball if it wasn't already created
                if not package in self._created_python_module_tarballs:
                    self._created_python_module_tarballs[package] = qondor.utils.tarball_python_module(package, outdir=self.rundir)
                # Add the tarball as input file for this cluster
                cluster.transfer_files['_packagetarball_{}'.format(package)] = self._created_python_module_tarballs[package]

    def make_sub(self, cluster):
        sub = get_default_sub(self.submission_time)
        sub['executable'] =  osp.basename(cluster.sh_entrypoint_filename)
        sub['+QondorRundir']  =  '"' + self.rundir + '"'
        sub['environment']['QONDORICLUSTER'] = str(cluster.i_cluster)
        sub['environment'].update(cluster.env)
        # Overwrite htcondor keys defined in the preprocessing
        sub.update(cluster.htcondor)
        # Flatten files into a string, excluding files on storage elements
        print(cluster.transfer_files.values())
        transfer_files = self.transfer_files + [f for f in cluster.transfer_files.values() if not seutils.has_protocol(f)]
        if len(transfer_files) > 0:
            sub['transfer_input_files'] = ','.join(transfer_files)
        logger.info('Prepared submission dict for cluster %s:\n%s', cluster.i_cluster, pprint.pformat(sub))
        return sub

    def add_submission(self, cluster, cli=True, njobs=1):
        qondor.utils.check_proxy()
        qondor.utils.create_directory(self.rundir)
        cluster.rundir = self.rundir
        # Possibly create tarballs out of required python packages
        self.handle_python_package_tarballs(cluster)
        # Possibly create tarball for the seutils cache and include it in the job
        if seutils.USE_CACHE: cluster.transfer_files['seutils-cache'] = self.dump_seutils_cache()
        # Dump cluster contents to file and build scope
        cluster.runcode_to_file()
        cluster.sh_entrypoint_to_file()
        cluster.scope_to_file()
        # Compile the submission dict
        sub = self.make_sub(cluster)
        # Add it to the submittables
        self.submittables.append((sub, njobs))

    def submit_pythonbindings(self, njobsmax=None):
        if not self.submittables: return
        import htcondor
        if njobsmax is None: njobsmax = 1e7
        n_jobs_summed = sum([ njobs for _, njobs in self.submittables ])
        n_jobs_total = min(n_jobs_summed, njobsmax)
        logger.info('Submitting all jobs; %s out of %s', n_jobs_total, n_jobs_summed)
        schedd = qondor.schedd.get_best_schedd()
        n_jobs_todo = n_jobs_total
        ads = []
        with qondor.utils.switchdir(self.rundir):
            with qondor.schedd._transaction(schedd) as transaction:
                submit_object = htcondor.Submit()
                for sub, njobs in self.submittables:
                    sub = sub.copy() # Keep original dict intact
                    sub['environment'] = qondor.schedd.format_env_htcondor(sub['environment'])
                    njobs = min(njobs, n_jobs_todo)
                    n_jobs_todo -= njobs
                    # Load the dict into the submit object
                    for key in sub:
                        submit_object[key] = sub[key]
                    new_ads = []
                    cluster_id = int(submit_object.queue(transaction, njobs, new_ads)) if not qondor.DRYMODE else 0
                    logger.info('Submitted %s jobs to cluster %s', len(new_ads) if not qondor.DRYMODE else njobs, cluster_id)
                    ads.extend(new_ads)
        # Clear submittables
        self.submittables = []

    def submit_cli(self, njobsmax=None):
        if not self.submittables: return
        if njobsmax is None: njobsmax = 1e7
        n_jobs_summed = sum([ njobs for _, njobs in self.submittables ])
        n_jobs_total = min(n_jobs_summed, njobsmax)
        logger.info('Submitting all jobs; %s out of %s', n_jobs_total, n_jobs_summed)
        schedd = qondor.schedd.get_best_schedd()
        n_jobs_todo = n_jobs_total
        # Compile the .jdl file
        get_cluster_nr = lambda sub: sub['environment']['QONDORICLUSTER']
        jdl_file = osp.join(
            self.rundir,
            'qondor_{}-{}.jdl'.format(get_cluster_nr(self.submittables[0][0]), get_cluster_nr(self.submittables[-1][0]))
            )
        with qondor.utils.openfile(jdl_file, 'w') as jdl:
            for sub, njobs in self.submittables:
                njobs = min(njobs, n_jobs_todo)
                n_jobs_todo -= njobs
                jdl.write('# Cluster {}\n'.format(sub['environment']['QONDORICLUSTER']))
                # Dump the submission to a jdl file
                for key in sub.keys():
                    val = sub[key]
                    if key.lower() == 'environment': val = qondor.schedd.format_env_htcondor(val)
                    jdl.write('{} = {}\n'.format(key, val))
                jdl.write('queue {}\n\n'.format(njobs))

            if qondor.DRYMODE: logger.info('Compiled %s:\n%s', jdl_file, jdl.text)
        # Run the actual submit command
        with qondor.utils.switchdir(self.rundir):
            qondor.utils.run_command(['condor_submit', osp.basename(jdl_file)])
        self.submittables = []

    def submit(self, cli, *args, **kwargs):
        """
        Wrapper that just picks the specific submit method
        """
        return self.submit_cli(*args, **kwargs) if cli else self.submit_pythonbindings(*args, **kwargs)


class Cluster(object):

    ICLUSTER = 0

    def __init__(self,
        runcode,
        scope=None,
        env=None,
        pips=None,
        htcondor=None,
        run_args=None,
        run_env='sl7-py27',
        rundir='.',
        session=None,
        **kwargs
        ):
        self.i_cluster = self.__class__.ICLUSTER
        self.__class__.ICLUSTER += 1
        self.rundir = rundir
        self.runcode = runcode
        self.env = {} if env is None else env
        self.scope = {} if scope is None else scope
        self.htcondor = [] if htcondor is None else htcondor
        self.run_args = run_args
        if qondor.utils.is_string(run_env): run_env = RUN_ENVS[run_env]
        self.run_env = run_env
        self.transfer_files = {}
        self.session = session
        # Base filenames needed for the job
        self.runcode_filename = 'cluster{}.py'.format(self.i_cluster)
        self.sh_entrypoint_filename = 'cluster{}.sh'.format(self.i_cluster)
        self.scope_filename = 'cluster{}.json'.format(self.i_cluster)
        # Process pip packages
        self.pips = []
        pips = [] if pips is None else pips
        for pip in [('qondor', 'auto'), ('seutils', 'auto')] + pips:
            if qondor.utils.is_string(pip):
                self.pips.append((pip, 'auto'))
            else:
                self.pips.append((pip[0], pip[1]))
        # Add addtional keywords to the scope
        self.scope.update(kwargs)

    def runcode_to_file(self):
        self.runcode_filename = osp.join(self.rundir, self.runcode_filename)
        if osp.isfile(self.runcode_filename):
            raise OSError('{} exists'.format(self.runcode_filename))
        self.transfer_files['runcode'] = self.runcode_filename
        logger.info('Dumping python code for cluster %s to %s', self.i_cluster, self.runcode_filename)
        if not(qondor.DRYMODE):
            with open(self.runcode_filename, 'w') as f:
                f.write(self.runcode)

    def sh_entrypoint_to_file(self):
        self.sh_entrypoint_filename = osp.join(self.rundir, self.sh_entrypoint_filename)
        if osp.isfile(self.sh_entrypoint_filename):
            raise OSError('{} exists'.format(self.sh_entrypoint_filename))
        sh = self.parse_sh_entrypoint()
        self.transfer_files['sh_entrypoint'] = self.sh_entrypoint_filename
        logger.info('Dumping .sh entrypoint for cluster %s to %s', self.i_cluster, self.sh_entrypoint_filename)
        if not(qondor.DRYMODE):
            with open(self.sh_entrypoint_filename, 'w') as f:
                f.write(sh)

    def scope_to_file(self):
        self.scope_filename = osp.join(self.rundir, self.scope_filename)
        if osp.isfile(self.scope_filename):
            raise OSError('{} exists'.format(self.scope_filename))
        self.transfer_files['scope'] = self.scope_filename
        self.env['QONDORSCOPEFILE'] = osp.basename(self.scope_filename)
        # Some last-minute additions before sending to a file
        self.scope['transfer_files'] = self.transfer_files
        self.scope['pips'] = self.pips
        logger.info(
            'Dumping the following scope for cluster %s to %s:\n%s',
            self.i_cluster, self.scope_filename, pprint.pformat(self.scope)
            )
        if not(qondor.DRYMODE):
            with open(self.scope_filename, 'w') as f:
                json.dump(self.scope, f)

    def parse_sh_entrypoint(self):
        # Basic setup: Divert almost all output to the stderr, and setup cms scripts
        sh = [
            '#!/bin/bash',
            'set -e',
            'echo "hostname: $(hostname)"',
            'echo "date:     $(date)"',
            'echo "pwd:      $(pwd)"',
            'echo "ls -al:"',
            'ls -al',
            'echo "Redirecting all output to stderr from here on out"',
            'exec 1>&2',
            '',
            'export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/',
            'source /cvmfs/cms.cern.ch/cmsset_default.sh',
            '',
            ]
        # Set the runtime environment (typically sourcing scripts to get the right python/gcc/ROOT/etc.)
        sh += self.run_env + ['']
        # Set up a directory to install python packages in, and put on the path
        # Currently requires $pipdir to be defined... might want to figure out something more clever
        sh += [
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
        # `pip install` the required pip packages for the job
        for package, install_instruction in self.pips:
            package = package.replace('.', '-').rstrip('/')
            if install_instruction == 'auto':
                install_instruction = 'editable-install' if qondor.utils.dist_is_editable(package) else 'pypi-instal'
            if install_instruction == 'editable-install':
                # Editable install: Manually give tarball, extract, and install
                sh.extend([
                    'mkdir {0}'.format(package),
                    'tar xf {0}.tar -C {0}'.format(package),
                    'pip install --install-option="--prefix=${{pip_install_dir}}" -e {0}/'.format(package)
                    ])
            else:
                # Non-editable install from pypi
                sh.append(
                    'pip install --install-option="--prefix=${{pip_install_dir}}" {0}'.format(package)
                    )
        # Make the actual python call to run the required job code
        # Also echo the exitcode of the python command to a file, to easily check whether jobs succeeded
        # First compile the command - which might take some command line arguments
        python_cmd = 'python {0}'.format(osp.basename(self.runcode_filename))
        if self.run_args:
            # Add any arguments for the python script to this line
            try:  # py3
                from shlex import quote
            except ImportError:  # py2
                from pipes import quote
            python_cmd += ' ' + ' '.join([quote(s) for s in self.run_args])
        sh += [
            python_cmd,
            'echo "$?" > exitcode_${CONDOR_CLUSTER_NUMBER}_${CONDOR_PROCESS_ID}.txt', # Store the python exit code in a file
            ''            
            ]
        sh = '\n'.join(sh)
        logger.info('Parsed the following .sh entrypoint for cluster %s:\n%s', self.i_cluster, sh)
        return sh

    def submit(self, *args, session=None, **kwargs):
        """
        Like Session.submit(cluster, *args, **kwargs), but then initiated from the cluster object.
        If session is None, a clean session is started.
        """
        if session is None: session = Session()
        session.submit(self, *args, **kwargs)


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
        sub['environment']['QONDORISCOPE'] = str(preprocessor.subscope_index)
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
        loops over all scopes and yields (submission_dict, extra_settings) as
        returned by self.format_for_htcondor_interface
        """
        # First do all prep
        at_least_one_job_submitted = False
        try:
            self.make_rundir()
            self.create_python_file()
            self.dump_ls_cache_file()
            self.dump_rootcache()
            # Loop over all 'scopes'
            # If there are no subscopes, this is just a len(1) iterator of the preprocessing
            for i_scope, preprocessor in enumerate(self.preprocessing.scopes()):
                # Create tarballs for local python modules
                for package, install_instruction in preprocessor.pip:
                    if install_instruction == 'editable-install':
                        self.tar_python_module(package)
                # Create the bash script entry point for this job
                shfile = osp.join(self.rundir, '{}_{}.sh'.format(self.python_name, i_scope))
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
        return list(self.iter_submissions())


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

    def create_python_file(self):
        self.python_file = osp.join(self.rundir, self.python_base)
        qondor.utils.copy_file(self.original_python_file, self.python_file)
        self.transfer_files.append(self.python_file)


class SubmitterPreproc(BaseSubmitter):
    """
    Submits from a preprocessor object directly.
    """
    def __init__(self, preprocessing):
        pass


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

    def create_python_file(self):
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
