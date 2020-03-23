# -*- coding: utf-8 -*-
import qondor
import logging, re, os.path as osp, datetime, os
from collections import OrderedDict
logger = logging.getLogger('qondor')



def preprocessing(filename):
    """
    Convenience function that returns a Preprocessor object
    """
    return Preprocessor(filename)


def iter_preprocess_lines(lines):
    _linebreak_cache = ''
    for line in lines:
        line = line.strip()
        if line.startswith('#$'):
            line = line.lstrip('#$').strip()
            if line.endswith('\\'):
                # Line continuation
                _linebreak_cache += line[:-1].strip() + ' '
                logger.debug('Line continuation: set _linebreak_cache to "%s"', _linebreak_cache)
                continue
            elif not _linebreak_cache == '':
                # If there was line continuation and this line does not
                # continue, it must be the last of the continuation
                yield _linebreak_cache + line
                _linebreak_cache = ''
                continue
            yield line

def get_preprocess_lines(lines):
    return list(iter_preprocess_lines(lines))

def iter_preprocess_file(filename):
    with open(filename, 'r') as f:
        # File object should support iteration
        for line in iter_preprocess_lines(f):
            yield line

def get_preprocess_file(filename):
    return list(iter_preprocess_file(filename))



class Preprocessor(object):
    """docstring for Preprocessor"""

    allowed_pip_install_instructions = [
        'module-install',
        'install'
        ]

    @classmethod
    def from_lines(cls, lines):
        preprocessor = cls()
        for line in lines:
            preprocessor.preprocess_line(line)
        return preprocessor

    def __init__(self, filename=None):
        super(Preprocessor, self).__init__()
        if not(filename is None): self.filename = osp.abspath(filename)
        self.htcondor = {}
        self.pip = [
            # Always pip install qondor itself
            ('qondor', self.get_pip_install_instruction('qondor'))
            ]
        self.variables = {}
        self.files = {}
        if 'el6' in os.uname()[2]:
            logger.info('Detected slc6')
            self.env = {
                'gccsetup' : '/cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-slc6-gcc7-opt/setup.sh',
                'pipdir' : '/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-slc6-gcc7-opt',
                'rootsetup' : '/cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-slc6-gcc7-opt/ROOT-env.sh',
                'SCRAM_ARCH' : 'slc6_amd64_gcc700',
                }
        else:
            self.env = {
                'gccsetup' : '/cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh',
                'pipdir' : '/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-centos7-gcc7-opt',
                'rootsetup' : '/cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-centos7-gcc7-opt/ROOT-env.sh',
                'SCRAM_ARCH' : 'slc7_amd64_gcc820',
                }
        self.split_transactions = []
        self.delayed_runtime = None
        self.allowed_lateness = None
        if not(filename is None): self.preprocess()

    def get_pip_install_instruction(self, package_name):
        """
        Checks whether a package is installed editable 
        (i.e. via `pip install -e package`), and if so chooses
        the module-install instruction, which means the editable code will
        be tarred up and sent along with the job. 
        """
        import sys, pkg_resources
        distribution = pkg_resources.get_distribution(package_name)
        if qondor.utils.dist_is_editable(distribution):
            return 'module-install'
        else:
            return 'install'

    def get_item(self):
        if not(len(self.split_transactions)):
            raise RuntimeError(
                '.get_item() should only be called if transactions are split. '
                'Either .preprocess() is not yet called, or there is no split_transactions '
                'directive.'
                )
        if qondor.BATCHMODE:
            return os.environ['QONDORITEM']
        else:
            logger.debug('Local mode: returning first item of %s', self.split_transactions)
            return self.split_transactions[0]

    def preprocess(self):
        for line in iter_preprocess_file(self.filename):
            self.preprocess_line(line)

    def preprocess_line(self, line):
        logger.debug('Processing line: %s', line)
        if   line.startswith('htcondor '):
            self.preprocess_line_htcondor(line)
        elif line.startswith('pip '):
            self.preprocess_line_pip(line)
        elif line.startswith('file '):
            self.preprocess_line_file(line)
        elif line.startswith('env '):
            self.preprocess_line_env(line)
        elif line.startswith('split_transactions '):
            self.preprocess_line_split_transactions(line)
        elif line.startswith('delay '):
            self.preprocess_line_delay(line)
        elif line.startswith('allowed_lateness '):
            self.preprocess_line_allowed_lateness(line)
        else:
            self.preprocess_line_variable(line)

    def preprocess_line_htcondor(self, line):
        # Remove 'htcondor' and assume 'key value' structure further on
        try:
            key, value = line.split(None, 2)[1:3]
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        logger.debug('htcondor[%s] = %s', key, value)
        self.htcondor[key] = value

    def preprocess_line_pip(self, line):
        try:
            install_instruction, value = line.split(None, 2)[1:3]
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        if not install_instruction in self.allowed_pip_install_instructions:
            logger.error('pip install_instruction %s is not valid', install_instruction)
            raise ValueError
        logger.debug('pip %s %s', install_instruction, value)
        self.pip.append((value, install_instruction))

    def preprocess_line_file(self, line):
        try:
            key, path = line.split(None, 2)[1:3]
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        if qondor.BATCHMODE:
            logger.debug('BATCHMODE: %s --> %s', path, osp.basename(path))
            path = osp.basename(path)
        else:
            if not osp.isabs(path):
                # Make sure path is relative to the file that is preprocessed
                path = osp.abspath(osp.join(osp.dirname(self.filename), path))
        logger.debug('file[%s] = %s', key, path)
        self.files[key] = path

    def preprocess_line_env(self, line):
        try:
            key, value = line.split(None, 2)[1:3]
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        logger.debug('(environment) %s = %s', key, value)
        self.env[key] = value

    def preprocess_line_split_transactions(self, line):
        self.split_transactions = line.split()[1:]
        if not len(self.split_transactions):
            logger.error('line "%s" did not have expected structure', line)
            raise ValueError
        logger.debug('Will split transactions per items: %s', self.split_transactions)

    def preprocess_line_variable(self, line):
        try:
            key, value = line.split(None, 1)
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        logger.debug('%s = %s', key, value)
        self.variables[key] = value


    # Special keywords preprocessing

    def preprocess_line_delay_or_lateness(self, line):
        try:
            components = line.split()[1:]
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        unit = 's' if len(components) <= 1 else components[1]
        conversion_to_seconds = {'s' : 1, 'm' : 60, 'h' : 3600}
        if not unit in conversion_to_seconds:
            raise ValueError(
                'Delay unit should be in %s', conversion_to_seconds.keys()
                )
        n_seconds = int(components[0]) * conversion_to_seconds[unit]
        return n_seconds        

    def preprocess_line_delay(self, line):
        n_seconds_delay = self.preprocess_line_delay_or_lateness(line)
        self.delayed_runtime = qondor.utils.get_now_utc() + datetime.timedelta(seconds=n_seconds_delay)
        logger.debug('Jobs will sleep until %s (%s seconds in the future)', self.delayed_runtime, n_seconds_delay)

    def preprocess_line_allowed_lateness(self, line):
        self.allowed_lateness = self.preprocess_line_delay_or_lateness(line)
        logger.debug('Allowed lateness is set to %s seconds', self.allowed_lateness)
