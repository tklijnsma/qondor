# -*- coding: utf-8 -*-
import qondor
import logging, os, os.path as osp, pprint, shutil, uuid
from time import strftime
logger = logging.getLogger('qondor')


class SHFile(object):

    """docstring for Preprocessor"""
    def __init__(self, preprocessing):
        self.preprocessing = preprocessing

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
        for key, value in self.preprocessing.env.iteritems():
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
            elif install_instruction == 'install':
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
        return [
            'python {0}'.format(osp.basename(self.preprocessing.filename)),
            ''
            ]




class BaseSubmitter(object):

    def __init__(self, dry=False):
        super(BaseSubmitter, self).__init__()
        self.transfer_files = []
        self.dry = dry

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

    def tar_python_module(self, module_name):
        logger.info('Creating tarball for python module %s', module_name)
        import importlib
        module = importlib.import_module(module_name)
        tarball = qondor.utils.tarball_python_module(
            module,
            outdir = self.rundir,
            dry = self.dry
            )
        self.transfer_files.append(tarball)

    def submit_to_htcondor(self):
        sub = self.__class__.get_default_sub_dict()
        sub['executable'] =  osp.basename(self.shfile)
        sub['+QondorRundir']  =  '"' + self.rundir + '"'

        # Overwrite keys from the preprocessing
        for key, value in self.preprocessing.htcondor.iteritems():
            sub[key] = value

        # Flatten files in a string
        transfer_files = self.transfer_files + self.preprocessing.files.values()
        if len(transfer_files) > 0:
            sub['transfer_input_files'] = ','.join(transfer_files)
        
        # Determine njobs
        njobs = int(self.preprocessing.variables.get('njobs', 1))

        if len(self.preprocessing.split_transactions) == 0:
            logger.info('Submitting %s jobs with:\n%s', njobs, pprint.pformat(sub))
            sub['environment'] = htcondor_format_environment(sub['environment'])
            if not self.dry:
                return htcondor_submit(sub, njobs, submission_dir=self.rundir)
        else:
            cluster_ids = []
            ads = []
            for item in self.preprocessing.split_transactions:
                subcopy = sub.copy()
                # Give it an env variable
                subcopy['environment']['QONDORITEM'] = item
                subcopy['environment'] = htcondor_format_environment(subcopy['environment'])
                logger.info(
                    'Submitting %s jobs for item %s with:\n%s',
                    njobs, item, pprint.pformat(subcopy)
                    )
                if not self.dry:
                    cluster_id, ad = htcondor_submit(subcopy, njobs, submission_dir=self.rundir)
                    cluster_ids.append(cluster_id)
                    ads.append(ad)
            return cluster_ids, ads

    def create_shfile(self):
        self.shfile = osp.join(self.rundir, self.python_name + '.sh')
        SHFile(self.preprocessing).to_file(
            self.shfile,
            dry = self.dry
            )

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

    def submit(self):
        """
        Main submission method
        """
        try:
            return self._submit()
        except:
            logger.error('Error during submission; cleaning up %s', self.rundir)
            if osp.isdir(self.rundir):
                shutil.rmtree(self.rundir)
            raise

    def _submit(self):
        self.make_rundir()
        self.copy_python_file()
        for package, install_instruction in self.preprocessing.pip:
            if install_instruction == 'module-install':
                self.tar_python_module(package)
        self.create_shfile()
        return self.submit_to_htcondor()


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
                f.write('\n'.join(self.python_code))
            return qondor.utils.run_command(['python', python_file])
        finally:
            logger.info('Removing %s', python_file)
            os.remove(python_file)


def htcondor_submit(sub, njobs=1, submission_dir='.'):
    """
    Submits the submission dict `sub` to the best scheduler.
    Returns the cluster id and class ad of the submitted job
    """
    import htcondor
    schedd = qondor.get_best_schedd(renew=True)
    with qondor.utils.switchdir(submission_dir):
        submit_object = htcondor.Submit(sub)
        with schedd.transaction() as transaction:
            ad = []
            cluster_id = submit_object.queue(transaction, njobs, ad)
            cluster_id = int(cluster_id)
    return cluster_id, ad


def htcondor_format_environment(env):
    """
    Takes a dict of key : value pairs that are both strings, and
    returns a string that is formatted so that htcondor can turn it
    into environment variables
    """
    return ('"' +
        ' '.join(
            [ '{0}=\'{1}\''.format(key, value) for key, value in env.iteritems() ]
            )
        + '"'
        )
