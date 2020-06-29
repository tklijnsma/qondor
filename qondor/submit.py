# -*- coding: utf-8 -*-
import qondor
import logging, os, os.path as osp, pprint, shutil, uuid, re
from time import strftime
import seutils
logger = logging.getLogger('qondor')


class SHFile(object):
    """docstring"""
    def __init__(self, preprocessing, python_script_args=None):
        self.preprocessing = preprocessing
        self.python_script_args = python_script_args

    def to_file(self, filename, dry=False):
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
        return [python_cmd, '']


class BaseSubmitter(object):

    def __init__(self, dry=False):
        super(BaseSubmitter, self).__init__()
        self.transfer_files = []
        self.dry = dry
        self._created_python_module_tarballs = []

    @staticmethod
    def get_default_sub_dict():
        """
        Returns the default submission dict (the equivalent of a .jdl file)
        to be used by the submitter. Implemented like this to be subclassable.
        """
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
                'CLUSTER_SUBMISSION_TIMESTAMP' : strftime('%Y%m%d_%H%M%S'),
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
        # Special settings for CMS Connect
        if os.uname()[1] == 'login.uscms.org':
            sites = [
                'T3_BY_NCPHEP', 'T2_IT_Bari', 'T3_US_Baylor', 'T2_CN_Beijing', 'T3_IT_Bologna', 'T2_UK_SGrid_Bristol',
                'T2_UK_London_Brunel', 'T0_CH_CERN', 'T2_CH_CERN', 'T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_ES_CIEMAT',
                'T1_IT_CNAF', 'T3_CN_PKU', 'T2_TH_CUNSTDA', 'T2_US_Caltech', 'T3_US_Colorado', 'T3_US_Cornell', 'T3_RU_FIAN',
                'T3_US_FIT', 'T1_US_FNAL', 'T3_US_FNALLPC', 'T3_US_Omaha', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR', 'T2_BR_UERJ',
                'T2_FI_HIP', 'T2_AT_Vienna', 'T2_HU_Budapest', 'T2_ES_IFCA', 'T2_RU_IHEP', 'T2_BE_IIHE', 'T3_FR_IPNL', 'T2_RU_INR',
                'T2_FR_IPHC', 'T2_RU_ITEP', 'T2_GR_Ioannina', 'T3_US_JHU', 'T2_RU_JINR', 'T1_RU_JINR', 'T2_UA_KIPT', 'T1_DE_KIT',
                'T2_KR_KNU', 'T3_KR_KNU', 'T3_US_Kansas', 'T2_IT_Legnaro', 'T2_BE_UCL', 'T3_RU_MEPhI', 'T2_TR_METU', 'T2_US_MIT',
                'T2_PT_NCG_Lisbon', 'T2_PK_NCP', 'T3_TW_NCU', 'T3_TW_NTU_HEP', 'T3_US_NotreDame', 'T2_TW_NCHC', 'T2_US_Nebraska',
                'T3_US_NU', 'T3_US_OSU', 'T3_ES_Oviedo', 'T3_UK_SGrid_Oxford', 'T1_ES_PIC', 'T2_RU_PNPI', 'T3_CH_PSI',
                'T3_IN_PUHEP', 'T3_IT_Perugia', 'T3_US_Princeton_ICSE', 'T2_US_Purdue', 'T3_UK_London_QMUL', 'T1_UK_RAL',
                'T3_UK_London_RHUL', 'T2_DE_RWTH', 'T3_US_Rice', 'T2_IT_Rome', 'T3_US_Rutgers', 'T2_UK_SGrid_RALPP', 'T2_RU_SINP',
                'T2_BR_SPRACE', 'T2_CH_CSCS_HPC', 'T2_PL_Swierk', 'T3_US_MIT', 'T3_US_NERSC', 'T3_US_SDSC', 'T3_CH_CERN_CAF',
                'T3_US_FIU', 'T3_US_FSU', 'T3_US_OSG', 'T3_US_TAMU', 'T2_IN_TIFR', 'T3_IN_TIFRCloud', 'T3_US_TTU', 'T3_IT_Trieste',
                'T3_US_UCD', 'T3_US_UCSB', 'T2_US_UCSD', 'T3_UK_ScotGrid_GLA', 'T3_US_UMD', 'T3_US_UMiss', 'T3_CO_Uniandes',
                'T3_KR_UOS', 'T2_MY_UPM_BIRUNI', 'T3_US_PuertoRico', 'T3_BG_UNI_SOFIA', 'T2_US_Vanderbilt', 'T2_US_Wisconsin',
                'T3_MX_Cinvestav', 'T3_US_FNALLPC'
                ]
            sub['+ConnectWrapper'] = '"2.0"'
            logger.warning('FIXME: CMS Connect settings currently hard-coded for a FNAL user')
            sub['+CMSGroups'] = '"/cms,T3_US_FNALLPC"'
            sub['+DESIRED_Sites'] = '"' + ','.join(sites) + '"'
            sub['+MaxWallTimeMins'] = '500'
            sub['+ProjectName'] = '"cms.org.fnal"'
            sub['+SubmitFile'] = '"irrelevant.jdl"'
            sub['+AccountingGroup'] = '"analysis.{0}"'.format(os.environ['USER'])
        return sub

    def tar_python_module(self, module_name):
        if module_name in self._created_python_module_tarballs:
            logger.debug('Not tarring up %s again, tarball already created', module_name)
            return
        logger.info('Creating tarball for python module %s', module_name)
        import importlib
        module = importlib.import_module(module_name)
        tarball = qondor.utils.tarball_python_module(
            module,
            outdir = self.rundir,
            dry = self.dry
            )
        self.transfer_files.append(tarball)
        self._created_python_module_tarballs.append(module_name)

    def submit_to_htcondor(self, shfile, preprocessor):
        sub = self.__class__.get_default_sub_dict()
        sub['executable'] =  osp.basename(shfile)
        sub['+QondorRundir']  =  '"' + self.rundir + '"'
        # Overwrite keys from the preprocessing
        sub.update(preprocessor.htcondor)
        # Flatten files into a string, excluding files on storage elements
        transfer_files = self.transfer_files + [f for f in preprocessor.files.values() if not seutils.has_protocol(f)]
        if len(transfer_files) > 0:
            sub['transfer_input_files'] = ','.join(transfer_files)
        # Simple case with no items; just use the njobs variable        
        if not preprocessor.items:
            njobs = int(preprocessor.variables.get('njobs', 1))
            logger.info('Submitting %s jobs with:\n%s', njobs, pprint.pformat(sub))
            if not self.dry:
                return htcondor_submit(sub, njobs, submission_dir=self.rundir)
        # Otherwise continue processing items
        logger.info('Items:\n%s', pprint.pformat(preprocessor.items))
        if not self.dry:
            subcopy = sub.copy()
            subcopy['environment']['QONDORISET'] = str(preprocessor.subset_index)
            return htcondor_submit(subcopy, 1, submission_dir=self.rundir, items=preprocessor.items)

    def make_rundir(self):
        self.rundir = osp.abspath('qondor_{0}_{1}'.format(
            self.python_name,
            strftime('%Y%m%d_%H%M%S')
            ))
        qondor.utils.create_directory(
            self.rundir,
            must_not_exist=True,
            dry=self.dry
            )

    def dump_ls_cache_file(self):
        """
        Creates the ls cache file for the job
        """
        cache_file = osp.join(self.rundir, qondor.Preprocessor.LS_CACHE_FILE)
        qondor.Preprocessor.dump_ls_cache(cache_file)
        self.transfer_files.append(cache_file)

    def submit(self, python_script_args=None):
        """
        Main submission method
        """
        all_cluster_ids = []
        all_ads = []
        at_least_one_job_submitted = False
        try:
            self.make_rundir()
            self.copy_python_file()
            self.dump_ls_cache_file()
            # Loop over all 'sets'
            # If there are no subsets, this is just a len(1) iterator of the preprocessing
            for i_set, preprocessor in enumerate(self.preprocessing.sets()):
                # Create tarballs for local python modules
                for package, install_instruction in preprocessor.pip:
                    if install_instruction == 'module-install':
                        self.tar_python_module(package)
                # Create the bash script entry point for this job
                shfile = osp.join(self.rundir, '{0}_{1}.sh'.format(self.python_name, i_set))
                SHFile(preprocessor, python_script_args).to_file(shfile, dry=self.dry)
                # Submit jobs to htcondor
                cluster_id, ads = self.submit_to_htcondor(shfile, preprocessor)
                all_cluster_ids.append(cluster_id)
                all_ads.extend(ads)
                at_least_one_job_submitted = True
            return all_cluster_ids, all_ads
        except:
            if not at_least_one_job_submitted:
                logger.error('Error during submission; cleaning up %s', self.rundir)
                if osp.isdir(self.rundir):
                    shutil.rmtree(self.rundir)
            raise


class Submitter(BaseSubmitter):
    """
    Standard Submitter based on a python file.
    Upon running `.submit()`, will create a new directory,
    transfer all relevant files from the job, and start
    running.
    """
    def __init__(self, python_file, dry=False):
        super(Submitter, self).__init__(dry)
        self.original_python_file = osp.abspath(python_file)
        self.python_base = osp.basename(self.original_python_file)
        self.python_name = self.python_base.replace('.py','')
        self.preprocessing = qondor.Preprocessor(self.original_python_file)

    def copy_python_file(self):
        self.python_file = osp.join(self.rundir, self.python_base)
        qondor.utils.copy_file(self.original_python_file, self.python_file, dry=self.dry)
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


def htcondor_submit(sub, njobs=1, submission_dir='.', items=None):
    """
    Submits the submission dict `sub` to the best scheduler.
    Returns the cluster id and class ad of the submitted job
    """
    import htcondor
    schedd = qondor.get_best_schedd(renew=True)
    # Create a copy to keep original dict unmodified
    # Make the transaction
    with qondor.utils.switchdir(submission_dir):
        with schedd.transaction() as transaction:
            if items:
                # Items logic: Turn any potential list into a ','-separated string,
                # and set the environment variable QONDORITEM to that string.
                ads = []
                subcopy = sub.copy()
                subcopy['environment']['QONDORITEM'] = 'placeholder' # Placeholder
                subcopy['environment'] = htcondor_format_environment(subcopy['environment'])
                submit_object = htcondor.Submit(subcopy)
                for item in items:
                    # Ensure the string will be ',' separated (a len-1 list will not have a comma)
                    # Also ensure there are no quotes in it, which would screw up condor's reading
                    if qondor.utils.is_string(item): item = [item]
                    item = ','.join([i.replace('\'','').replace('"','') for i in item])
                    # Change the env variable in the submitobject
                    change_submitobject_env_variable(submit_object, 'QONDORITEM', item)
                    # Submit again and record cluster_id and job ads
                    ad = []
                    cluster_id = submit_object.queue(transaction, njobs, ad)
                    cluster_id = int(cluster_id)
                    ads.extend(ad)
            else:
                subcopy = sub.copy()
                subcopy['environment'] = htcondor_format_environment(subcopy['environment'])
                submit_object = htcondor.Submit(subcopy)
                ads = []
                cluster_id = submit_object.queue(transaction, njobs, ads)
                cluster_id = int(cluster_id)
    logger.info('Submitted %s jobs to cluster %s', len(ads), cluster_id)
    return cluster_id, ads


def change_submitobject_env_variable(submitobject, key, value):
    """
    Tries to replace an environment variable in a Submit-object.
    This hack is needed to have items be in the same cluster.
    """
    env = submitobject['environment']
    new_env = re.sub(key + r'=\'.*?\'', '{0}=\'{1}\''.format(key, value), env)
    logger.debug('Replacing:\n  %s\n  by\n  %s', env, new_env)
    submitobject['environment'] = new_env


def htcondor_format_environment(env):
    """
    Takes a dict of key : value pairs that are both strings, and
    returns a string that is formatted so that htcondor can turn it
    into environment variables
    """
    return ('"' +
        ' '.join(
            [ '{0}=\'{1}\''.format(key, value) for key, value in env.items() ]
            )
        + '"'
        )
