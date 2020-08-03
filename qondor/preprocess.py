# -*- coding: utf-8 -*-
import qondor, seutils
import logging, re, os.path as osp, datetime, os, math, glob, pprint, json
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
            if len(line) == 0:
                continue
            elif line.endswith('\\'):
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

    # Cached output from ls calls
    LS_CACHE_FILE = 'ls_cache.json' # Can be overwritten
    LS_CACHE = {}

    @classmethod
    def add_ls_cache(cls, argument, is_se='auto'):
        if argument in cls.LS_CACHE:
            logger.info('Argument "%s" is already cached', argument)
            return cls.get_ls_cache(argument)
        # Check if this concerns a storage element
        if is_se == 'auto': is_se = seutils.has_protocol(argument)
        if is_se:
            contents = seutils.ls_wildcard(argument)
        else:
            contents = [ osp.join(os.getcwd(), i) for i in glob.glob(argument)]
        logger.debug('ls("%s") yielded:\n%s', argument, pprint.pformat(contents))
        cls.LS_CACHE[argument] = contents
        return contents

    @classmethod
    def get_ls_cache(cls, argument):
        return cls.LS_CACHE[argument]

    @classmethod
    def read_ls_cache(cls, cache_file):
        """
        Reads a cache file into LS_CACHE
        """
        logger.info('Reading ls cache file %s', cache_file)
        with open(cache_file, 'r') as f:
            cls.LS_CACHE = json.load(f)

    @classmethod
    def dump_ls_cache(cls, cache_file):
        """
        Dumps the LS_CACHE class dict to a file
        """
        logger.info('Dumping ls cache (%s arguments) to %s', len(cls.LS_CACHE.keys()), cache_file)
        with open(cache_file, 'w') as f:
            json.dump(cls.LS_CACHE, f)

    # Counter for the number of subsets that have been created
    ISET = 0

    allowed_pip_install_instructions = [
        'module-install',
        'pypi-install',
        'install'
        ]

    @classmethod
    def from_lines(cls, lines):
        """For legacy"""
        return cls(lines=lines)

    def __init__(self, filename=None, lines=None, parent=None, ls_cache_file=None):
        super(Preprocessor, self).__init__()
        self.parent = parent
        self.subsets = []
        self.htcondor = {}
        self.pip = []
        self.env = {}
        self.variables = {}
        self.files = {}
        self.items = []
        self.rootfile_chunks = []
        self.delayed_runtime = None
        self.allowed_lateness = None
        # File with cached output from ls processing
        self.ls_cache = {}
        self.ls_cache_file = ls_cache_file
        # Count how many Preprocessor instances there are
        self.subset_index = self.__class__.ISET
        self.__class__.ISET += 1
        if self.is_master():
            # It's the master Preprocessor instance; set some defaults
            self.pip = [
                # Always pip install qondor itself
                ('qondor', self.get_pip_install_instruction('qondor'))
                ]
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
        # Read lines if necessary
        if filename and not lines:
            self.filename = osp.abspath(filename)
            self.preprocess()
        elif lines and not filename:
            self.preprocess_lines(lines)
        elif lines and filename:
            raise ValueError('Pass either filename or lines as an init argument')

    def is_master(self):
        return self.parent is None # Should only be true for the master processor

    def merge(self, base):
        """
        Takes base preprocessor variables and adds self on top of it, so that base things are inherited
        """
        # Overwrite style:
        self.htcondor = dict(base.htcondor, **self.htcondor)
        self.variables = dict(base.variables, **self.variables)
        self.env = dict(base.env, **self.env)
        self.files = dict(base.files, **self.files)
        if hasattr(base, 'filename') and not hasattr(self, 'filename'): self.filename = base.filename
        if self.delayed_runtime is None: self.delayed_runtime = base.delayed_runtime
        if self.allowed_lateness is None: self.allowed_lateness = base.allowed_lateness
        # Append style:
        self.items = base.items + self.items
        self.rootfile_chunks = base.rootfile_chunks + self.rootfile_chunks
        self.pip = base.pip + self.pip

    def sets(self):
        """
        Iteratively loops through subsets, updates with parent variables,
        and yields the lowest level subsets (i.e. ones without children).
        Each subset is a full Preprocessor instance.
        """
        if len(self.subsets) == 0:
            yield self
        else:
            for subset in self.subsets:
                subset.merge(self)
                # Iterate
                for s in subset.sets():
                    yield s

    def all_items(self):
        """
        Returns all items of subsets below.
        """
        all_items = []
        for s in self.sets():
            all_items.extend(s.items)
        return all_items

    def all_rootfile_chunks(self):
        """
        Returns all rootfile_chunks of subsets below.
        """
        all_rootfile_chunks = []
        for s in self.sets():
            all_rootfile_chunks.extend(s.rootfile_chunks)
        return all_rootfile_chunks

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
            instruction = 'module-install'
        else:
            instruction = 'pypi-install'
        logger.debug('Determined install instruction %s for package %s', instruction, package_name)
        return instruction

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
        """
        Wrapper for self.preprocess_lines that just takes a path to a file
        rather than direct lines
        """
        # Pass the list rather than the iterator, since sets might make slices
        self.preprocess_lines(get_preprocess_file(self.filename))

    def preprocess_lines(self, lines):
        # Preprocesses a list of lines
        # Starts a subset upon encountering the keyword 'set'
        line_iterator = enumerate(lines)
        for i_line, line in line_iterator:
            logger.debug('Processing l%s: %s', i_line, line)
            line = line.strip()
            if len(line) == 0:
                continue
            elif line.startswith('endset'):
                # This marks the end of a set, so stop preprocessing lines in this processor
                # If this preprocessor is the master, an endset must have been encountered without
                # a matching starting set-line
                if self.is_master():
                    raise ValueError(
                        'Encountered endset without a matching opening set:\n'
                        '--> l{0}: {1}'.format(i_line, line)
                        )
                # Return the line index so that the parent processor can skip the lines processed
                # in this set.
                # Note this line number will be the 'local' line number of the subset of lines
                # that was passed to the child processor
                logger.debug('Closing set at line %s', i_line)
                return i_line
            elif line.startswith('set ') or line == 'set':
                # Start a subset from this line forward
                logger.debug('Starting new set')
                subset = Preprocessor(parent=self)
                self.subsets.append(subset)
                i_line_endset = subset.preprocess_lines(lines[i_line+1:])
                # Skip lines processed in the subset
                for i in range(i_line_endset+1):
                    try:
                        i_line_skip, line_skip = next(line_iterator)
                        logger.debug('Skipping l%s: "%s" (already used in subset)', i_line_skip, line_skip)
                    except StopIteration:
                        # If this happens there is a bug in the code
                        logger.error('StopIteration occured while skipping lines in a subset')
                        raise
                continue
            else:
                self.preprocess_line(line)
        # Only the master should make it to here - subsets should be closed with an endset tag
        if not self.is_master():
            raise ValueError(
                'Encountered EOF without an expected closing endset'
                )

    def preprocess_line(self, line):
        if   line.startswith('htcondor '):
            self.preprocess_line_htcondor(line)
        elif line.startswith('pip '):
            self.preprocess_line_pip(line)
        elif line.startswith('file '):
            self.preprocess_line_file(line)
        elif line.startswith('env '):
            self.preprocess_line_env(line)
        elif line.startswith('delay '):
            self.preprocess_line_delay(line)
        elif line.startswith('allowed_lateness '):
            self.preprocess_line_allowed_lateness(line)
        elif line.startswith('items '):
            self.preprocess_line_items(line)
        else:
            self.preprocess_line_variable(line)

    def preprocess_line_items(self, line):
        """
        Splits a line and returns a list of items, each to be processed in one job.
        If multiple 'items' lines are encountered, items are simply added
        Line elements "b=<number>" and "n=<number>" are considered chunkification parameters
        """
        unprocessed_items = line.split()[1:] # Drop the keyword
        items = []
        n_chunks = None
        chunk_size = None
        chunk_size_events = None
        for item in unprocessed_items:
            # If an 'item' starts with n= or b=, read that value and don't consider it
            # an item to process. These 'flags' are to configure the chunkification.
            # Only the last parameter counts, any previous parameter will be overwritten.
            # New flag: e= to make chunks with a specific number entries. Only works for
            # for root files.
            if item.startswith('n='):
                n_chunks = int(item.split('=')[-1])
                chunk_size = None
                chunk_size_events = None
                continue
            elif item.startswith('b='):
                chunk_size = int(item.split('=')[-1])
                n_chunks = None
                chunk_size_events = None
                continue
            elif item.startswith('e='):
                chunk_size_events = int(item.split('=')[-1])
                n_chunks = None
                chunk_size = None
                continue
            else:
                # It's an item; see if it's meant to be expanded:
                match = re.match(r'ls\((.*)\)', item)
                if match:
                    # Evaluate the ls argument
                    argument = match.group(1)
                    if qondor.BATCHMODE:
                        # Only read the cache, don't execute listing
                        contents = self.get_ls_cache(argument)
                    else:
                        # Add it to the cache
                        contents = self.add_ls_cache(argument)
                    if not len(contents): logger.warning('%s yielded no items', match.group())
                    items.extend(contents)
                else:
                    items.append(item)
        n_items = len(items)
        if n_chunks is None and chunk_size is None and chunk_size_events is None:
            # There was no chunkification flag;
            # Use the default of 1 item per chunk, and let's not bother putting it in a list
            self.items.extend(items)
        # Else use the chunkification parameters; items will be lists
        else:
            if chunk_size:
                # If chunk_size is given, calculate the number of chunks that fit in the items
                n_chunks = int(math.ceil(float(n_items) / chunk_size))
                self.items.extend(qondor.utils.chunkify(items, n_chunks))
            elif n_chunks:
                self.items.extend(qondor.utils.chunkify(items, n_chunks))
            elif chunk_size_events:
                self.rootfile_chunks.extend(
                    list(seutils.root.iter_chunkify_rootfiles_by_entries(items, chunk_size_events))
                    )

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
        # If plain 'install', check whether it's from pypi or an editable package to be tarballed up
        if install_instruction == 'install': install_instruction = self.get_pip_install_instruction(value)
        logger.debug('pip %s %s', install_instruction, value)
        self.pip.append((value, install_instruction))

    def preprocess_line_file(self, line):
        try:
            key, path = line.split(None, 2)[1:3]
        except ValueError:
            logger.error('line "%s" did not have expected structure', line)
            raise
        if seutils.has_protocol(path):
            # Do nothing; files on an SE need no processing
            pass
        elif qondor.BATCHMODE:
            logger.debug('BATCHMODE: %s --> %s', path, osp.basename(path))
            path = osp.basename(path)
        else:
            # Make sure path will be absolute
            # If path is currently relative, assume it's relative w.r.t. to the python file
            if not osp.isabs(path):
                if hasattr(self, 'filename'):
                    # Make sure path is relative to the python file that is preprocessed
                    path = osp.abspath(osp.join(osp.dirname(self.filename), path))
                else:
                    path = osp.abspath(path)
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
