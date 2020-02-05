# -*- coding: utf-8 -*-
import qondor
import logging, re, os.path as osp
from collections import OrderedDict
logger = logging.getLogger('qondor')


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
        return iter_preprocessing_lines(f)

def get_preprocess_file(filename):
    return list(iter_preprocess_file(filename))



class Preprocessor(object):
    """docstring for Preprocessor"""

    allowed_pip_install_instructions = [
        'module-install',
        'install'
        ]

    def __init__(self, filename):
        super(Preprocessor, self).__init__()
        self.filename = osp.abspath(filename)
        self.htcondor = {}
        self.pip = [
            # Always pip install qondor itself
            ('qondor', 'module-install')
            ]
        self.variables = {}
        self.files = {}
        self.env = {
            'gccsetup' : '/cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh',
            'pipdir' : '/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-centos7-gcc7-opt',
            'rootsetup' : '/cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-centos7-gcc7-opt/ROOT-env.sh',
            }

    def preprocess():
        for line in iter_preprocess_file(self.filename):
            self.preprocess_line(line)

    def preprocess_line(self, line):
        logger.debug('Processing line: %s', line)
        if line.startswith('htcondor '):
            self.preprocess_line_htcondor(line)
        elif line.startswith('pip '):
            self.preprocess_line_pip(line)
        elif line.startswith('file '):
            self.preprocess_line_file(line)
        elif line.startswith('env '):
            self.preprocess_line_env(line)
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

    def preprocess_line_variable(self, line):
        try:
            key, value = line.split(None, 1)
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        logger.debug('%s = %s', key, value)
        self.variables[key] = value
