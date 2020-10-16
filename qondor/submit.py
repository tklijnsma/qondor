# -*- coding: utf-8 -*-
import qondor
import logging, os, os.path as osp, pprint, shutil, uuid, re, json, sys
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
        kwargs.setdefault('session', session)
        kwargs.setdefault('run_args', run_args)
        cluster = Cluster(runcode, *args, **kwargs)
        session.add_submission(cluster, njobsmax=njobsmax)
    def submit_now_fn():
        session.submit(cli, njobsmax=njobsmax)
    exec_scope = {
        'qondor' : qondor,
        'session' : session,
        'submit' : submit_fn,
        'submit_now' : submit_now_fn,
        'runcode' : runcode,
        'run_args' : run_args,
        'cli' : cli,
        'njobsmax' : njobsmax
        }
    logger.warning('Running submission code now')
    _exec_in_scope(submitcode, exec_scope)
    # Do a submit_now call in case it wasn't done yet (if submit_now was called in the submit code this is a no-op)
    session.submit(cli, njobsmax=njobsmax)

def _exec_in_scope(code, scope):
    # if sys.version_info.major < 3:
    #     if not code.endswith('\n'): code += '\n'
    #     logger.warning('Python 2 style')
    #     exec code in scope
    # else:
    exec(code, scope)

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
        _exec_in_scope(submitcode, exec_scope)
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
        self._njobs_submitted = 0
        self.htcondor = get_default_sub(self.submission_time)

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

    def make_sub(self, cluster, cli):
        # Base off of global settings only for python-binding mode
        # For the condor_submit cli method, it's better to just write the global keys once at the top of the file
        # Little unsafe, if you modiy these 'global' settings in one job they propegate to all the next ones,
        # but otherwise submission of many jobs becomes very slow
        sub = {} if cli else self.htcondor.copy()
        sub['executable'] =  osp.basename(cluster.sh_entrypoint_filename)
        sub['+QondorRundir']  =  '"' + self.rundir + '"'
        sub['environment'] = {}
        sub['environment']['QONDORICLUSTER'] = str(cluster.i_cluster)
        sub['environment'].update(cluster.env)
        # Overwrite htcondor keys defined in the preprocessing
        sub.update(cluster.htcondor)
        # Flatten files into a string, excluding files on storage elements
        transfer_files = self.transfer_files + [f for f in cluster.transfer_files.values() if not seutils.has_protocol(f)]
        if len(transfer_files) > 0:
            sub['transfer_input_files'] = ','.join(transfer_files)
        sub.update(cluster.htcondor)
        logger.info('Prepared submission dict for cluster %s:\n%s', cluster.i_cluster, pprint.pformat(sub))
        return sub

    def add_submission(self, cluster, cli=True, njobs=1, njobsmax=None):
        if njobsmax: njobs = min(njobs, njobsmax - self._njobs_submitted)
        if njobsmax and njobs == 0:
            logger.debug('Not adding submission for cluster %s - reached njobsmax %s', cluster.i_cluster, njobsmax)
            return
        self._njobs_submitted += njobs
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
        sub = self.make_sub(cluster, cli)
        # Potentially process cmsconnect specific settings
        blacklist = sub.pop('blacklist', None)
        whitelist = sub.pop('whitelist', None)
        if os.uname()[1] == 'login.uscms.org' or os.uname()[1] == 'login-el7.uscms.org':
            qondor.logger.warning('Detected CMS Connect; loading specific settings')
            cmsconnect_settings(sub, cli=cli, blacklist=blacklist, whitelist=whitelist)
        # Add it to the submittables
        self.submittables.append((sub, njobs))

    def submit_pythonbindings(self, njobsmax=None):
        qondor.utils.check_proxy()
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
                for sub_orig, njobs in self.submittables:
                    sub = sub_orig.copy() # Keep original dict intact? Global settings already contained
                    sub['environment'] = qondor.schedd.format_env_htcondor(sub['environment'])
                    njobs = min(njobs, n_jobs_todo)
                    n_jobs_todo -= njobs
                    # Load the dict into the submit object
                    for key in sub:
                        submit_object[key] = sub[key]
                    new_ads = []
                    cluster_id = int(submit_object.queue(transaction, njobs, new_ads)) if not qondor.DRYMODE else 0
                    logger.info(
                        'Submitted %s jobs for i_cluster %s to htcondor cluster %s',
                        len(new_ads) if not qondor.DRYMODE else njobs,
                        sub_orig['environment']['QONDORICLUSTER'], cluster_id
                        )
                    ads.extend(new_ads)
                    if n_jobs_todo == 0: break
        logger.info('Summary: Submitted %s jobs to cluster %s', n_jobs_total, cluster_id)
        # Clear submittables
        self.submittables = []

    def submit_cli(self, njobsmax=None):
        qondor.utils.check_proxy()
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
        jdl_contents = []
        # First write 'global' jdl settings            
        for key in self.htcondor.keys():
            val = self.htcondor[key]
            if key.lower() == 'environment': val = qondor.schedd.format_env_htcondor(val)
            jdl_contents.append('{} = {}'.format(key, val))
        jdl_contents.append('')
        # Then write the settings per job
        for sub, njobs in self.submittables:
            njobs = min(njobs, n_jobs_todo)
            n_jobs_todo -= njobs
            jdl_contents.append('# Cluster {}'.format(sub['environment']['QONDORICLUSTER']))
            # Dump the submission to a jdl file
            for key in sub.keys():
                val = sub[key]
                if key.lower() == 'environment': val = qondor.schedd.format_env_htcondor(val)
                jdl_contents.append('{} = {}'.format(key, val))
            jdl_contents.append('queue {}\n'.format(njobs))
        # Dump to file
        jdl_contents = '\n'.join(jdl_contents)
        with qondor.utils.openfile(jdl_file, 'w') as jdl:
            jdl.write(jdl_contents)
        logger.info('Compiled %s:\n%s', jdl_file, jdl_contents)
        # Run the actual submit command
        with qondor.utils.switchdir(self.rundir):
            output = qondor.utils.run_command(['condor_submit', osp.basename(jdl_file)])
        # Clear the submittables
        self.submittables = []
        # Get some info from the condor_submit command, if not dry mode
        if qondor.DRYMODE:
            logger.info('Summary: Submitted %s jobs to cluster 0', n_jobs_total)
        else:
            match = re.search(r'(\d+) job\(s\) submitted to cluster (\d+)', '\n'.join(output))
            if match:
                logger.info('Submitted %s jobs to cluster_id %s', match.group(1), match.group(2))
            else:
                logger.error(
                    'condor_submit exited ok but could not determine where and how many jobs were submitted'
                    )

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
        self.htcondor = {} if htcondor is None else htcondor
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

    def submit(self, *args, **kwargs):
        """
        Like Session.submit(cluster, *args, **kwargs), but then initiated from the cluster object.
        If session is not provided as a keyword argument, a clean session is started.
        """
        session = kwargs.pop('session', Session()) # Make new session if not set by keyword
        session.submit(self, *args, **kwargs)


def cmsconnect_get_all_sites():
    """
    Reads the central config for cmsconnect to determine the list of all available sites
    """
    try:
        from configparser import RawConfigParser # python 3
    except ImportError:
        import ConfigParser # python 2
        RawConfigParser = ConfigParser.RawConfigParser
    cfg = RawConfigParser()
    cfg.read('/etc/ciconnect/config.ini')
    sites = cfg.get('submit', 'DefaultSites').split(',')
    all_sites = set(sites)
    return all_sites

def cmsconnect_settings(sub, blacklist=None, whitelist=None, cli=False):
    """
    Adds special cmsconnect settings to submission dict in order to submit
    on cmsconnect via the python bindings, or in case of submitting by the
    command line, to set the DESIRED_Sites key.
    Modifies the dict in place.
    """
    all_sites = cmsconnect_get_all_sites()

    # Check whether the user whitelisted or blacklisted some sites
    desired_sites = None
    if blacklist or whitelist:
        import fnmatch
        blacklisted = []
        whitelisted = []
        # Build the blacklist
        if blacklist:
            for blacksite_pattern in blacklist:
                for site in all_sites:
                    if fnmatch.fnmatch(site, blacksite_pattern):
                        blacklisted.append(site)
        # Build the whitelist
        if whitelist:
            for whitesite_pattern in whitelist:
                for site in all_sites:
                    if fnmatch.fnmatch(site, whitesite_pattern):
                        whitelisted.append(site)
        # Convert to list and sort
        blacklisted = list(set(blacklisted))
        blacklisted.sort()
        whitelisted = list(set(whitelisted))
        whitelisted.sort()
        logger.info('Blacklisting: %s', ','.join(blacklisted))
        logger.info('Whitelisting: %s', ','.join(whitelisted))
        desired_sites = list( (set(all_sites) - set(blacklisted)).union(set(whitelisted)) )
        desired_sites.sort()

    # Add a plus only if submitting via .jdl file
    addplus = lambda key: '+' + key if cli else key
    if desired_sites:
        logger.info('Submitting to desired sites: %s', ','.join(desired_sites))
        sub[addplus('DESIRED_Sites')] = '"' + ','.join(desired_sites) + '"'
    else:
        logger.info('Submitting to all sites: %s', ','.join(all_sites))
    if not cli:
        sub[addplus('ConnectWrapper')] = '"2.0"'
        sub[addplus('CMSGroups')] = '"/cms,T3_US_FNALLPC"'
        sub[addplus('MaxWallTimeMins')] = '500'
        sub[addplus('ProjectName')] = '"cms.org.fnal"'
        sub[addplus('SubmitFile')] = '"irrelevant.jdl"'
        sub[addplus('AccountingGroup')] = '"analysis.{0}"'.format(os.environ['USER'])
        logger.warning('FIXME: CMS Connect settings currently hard-coded for a FNAL user')
