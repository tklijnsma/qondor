# -*- coding: utf-8 -*-
import qondor
import logging, os, os.path as osp, pprint
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
        lines.extend(self.python_call())
        return lines

    def initialize(self):
        lines = [
            '#!/bin/bash',
            'set -e',
            'echo "hostname: $(hostname)"',
            'echo "date:     $(date)"',
            'echo "pwd:      $(pwd)"',
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
            package = package.replace('.', '-')
            if install_instruction == 'module-install':
                lines.extend([
                    'mkdir {0}'.format(package),
                    'tar xf {0}.tar -C {0}'.format(package),
                    ])
            lines.extend([
                'pip install --install-option="--prefix=${{pip_install_dir}}" {0}'.format(package),
                ''
                ])
        return lines

    def python_call(self):
        return [
            'python {0}'.format(osp.basename(self.preprocessing.filename)),
            ''
            ]


class Submitter(object):
    """docstring for Submitter"""

    ignore_uncommitted_changes_in_python_modules = False

    def __init__(self, python_file, dry=False):
        super(Submitter, self).__init__()
        self.original_python_file = osp.abspath(python_file)

        self.python_base = osp.basename(self.original_python_file)
        self.python_name = self.python_base.replace('.py','')
        self.dry = dry
        self.preprocessing = qondor.Preprocessor(self.original_python_file)
        self.transfer_files = []

    def submit(self):
        self.make_rundir()
        self.copy_python_file()
        for package, install_instruction in self.preprocessing.pip:
            if install_instruction == 'module-install':
                self.tar_python_module(package)
        self.create_shfile()
        return self.submit_to_htcondor()

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

    def copy_python_file(self):
        self.python_file = osp.join(self.rundir, self.python_base)
        qondor.utils.copy_file(self.original_python_file, self.python_file, dry=self.dry)
        self.transfer_files.append(self.python_file)

    def tar_python_module(self, module_name):
        logger.info('Creating tarball for python module %s', module_name)
        import importlib
        module = importlib.import_module(module_name)
        tarball = qondor.utils.tarball_python_module(
            module,
            outdir = self.rundir,
            ignore_uncommitted = self.ignore_uncommitted_changes_in_python_modules,
            dry=self.dry
            )
        self.transfer_files.append(tarball)

    def create_shfile(self):
        self.shfile = osp.join(self.rundir, self.python_name + '.sh')
        SHFile(self.preprocessing).to_file(
            self.shfile,
            dry = self.dry
            )

    def submit_to_htcondor(self):
        sub = {
            'universe' : 'vanilla',
            'output' : 'out_$(Cluster)_$(Process).txt',
            'error' : 'err_$(Cluster)_$(Process).txt',
            'log' : 'log_$(Cluster)_$(Process).txt',
            'x509userproxy' : '/uscms/home/{0}/x509up_u55957'.format(os.environ['USER']),
            'executable': osp.basename(self.shfile),
            '+QondorRundir' : self.rundir,
            'environment' : {
                'CONDOR_CLUSTER_NUMBER' : '$(Cluster)',
                'CONDOR_PROCESS_ID' : '$(Process)',
                'USER' : os.environ['USER'],
                'CLUSTER_SUBMISSION_TIMESTAMP' : strftime('%Y%m%d_%H%M%S'),
                },
            }

        # Flatten files in a string
        transfer_files = self.transfer_files + self.preprocessing.files.values()
        if len(transfer_files) > 0:
            sub['transfer_input_files'] = ','.join(transfer_files)

        # Turn dict-like environment into formatted string
        sub['environment'] = htcondor_format_environment(sub['environment'])

        # Determine njobs now
        njobs = self.preprocessing.variables.get('njobs', 1)
        logger.info('Submitting %s jobs with:%s', njobs, pprint.pformat(sub))
        if not self.dry:
            import htcondor
            schedd = qondor.get_best_schedd()
            with qondor.utils.switchdir(self.rundir):
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

