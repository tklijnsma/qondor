#!/usr/bin/env python
# -*- coding: utf-8 -*-
import qondor
import logging, re, pprint, os, os.path as osp
from time import sleep, strftime
from contextlib import contextmanager
logger = logging.getLogger('qondor')


CACHED_FUNCTIONS = []
def cache_return_value(func):
    """
    Decorator that only calls a function once, and
    subsequent calls just return the cached return value
    """
    global CACHED_FUNCTIONS
    def wrapper(*args, **kwargs):
        if not getattr(wrapper, 'is_called', False):
            wrapper.is_called = True
            wrapper.cached_return_value = func(*args, **kwargs)
            CACHED_FUNCTIONS.append(wrapper)
        else:
            logger.debug(
                'Returning cached value for %s: %s',
                func.__name__, wrapper.cached_return_value
                )
        return wrapper.cached_return_value
    return wrapper

def clear_cache():
    global CACHED_FUNCTIONS
    for func in CACHED_FUNCTIONS:
        func.is_called = False
    CACHED_FUNCTIONS = []


class ScheddManager(object):
    """
    Class that interacts with htcondor scheduler daemons ('schedds').
    Will first run `get_collector_node_addresses` to obtain htcondor collectors,
    and will get the schedd_ads from the collectors.
    The algorithm for `get_best_schedd` is based on the weight calculation
    implemented on the Fermilab HTCondor cluster.

    Most methods are decorated with `cache_return_value`, meaning subsequent calls
    will return the same value as the first call until the cache is cleared.
    """

    def __init__(self):
        super(ScheddManager, self).__init__()
        self.schedd_ads = []
        self.schedd_constraints = None

    def clear_cache(self):
        """Convenience function to not have to import clear_cache everywhere"""
        clear_cache()

    @cache_return_value
    def get_collector_node_addresses(self):
        """
        Sets the attribute self.collector_node_addresses to a list of addresses to
        htcondor collectors. The collectors subsequently are able to return
        schedulers to which one can submit.

        First looks if qondor.COLLECTOR_NODES is set to anything. If so, it takes the
        addresses from that, allowing the user to easily configure custom collector nodes.

        Otherwise, it tries to obtain addresses from the htcondor parameter COLLECTOR_HOST_STRING.
        """
        import htcondor

        if not(qondor.COLLECTOR_NODES is None):
            logger.info('Setting collectors to qondor.COLLECTOR_NODES %s', qondor.COLLECTOR_NODES)
            self.collector_node_addresses = qondor.COLLECTOR_NODES
            return

        err_msg = (
            'Could not find any collector nodes. '
            'Either set qondor.COLLECTOR_NODES manually to a \', \'-separated '
            'list of addresses, or set the htcondor parameter \'COLLECTOR_HOST_STRING\','
            ' or subclass qondor.schedd.ScheddManager so that it can find the collector nodes.'
            )
        try:
            collector_node_addresses = htcondor.param['COLLECTOR_HOST_STRING'].strip().strip('"')
        except:
            logger.error(err_msg)
            raise
        if collector_node_addresses is None:
            RuntimeError(err_msg)

        logger.debug('Found collector nodes %s', collector_node_addresses)
        # Seems like a very obfuscated way of writing ".split()" but keep it for now
        self.collector_node_addresses = re.findall(r'[\w\/\:\/\-\/\.]+', collector_node_addresses)
        logger.debug('Using collector_node_addresses %s', self.collector_node_addresses)

    @cache_return_value
    def get_schedd_ads(self):
        import htcondor
        schedd_ad_projection = [
            'Name', 'MyAddress', 'MaxJobsRunning', 'ShadowsRunning',
            'RecentDaemonCoreDutyCycle', 'TotalIdleJobs'
            ]
        try:
            logger.debug('First trying default collector and schedd')
            # This is most likely to work for most batch systems
            collector = htcondor.Collector()
            limited_schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd)
            logger.debug('Retrieved limited schedd ad:\n%s', pprint.pformat(limited_schedd_ad))
            self.schedd_ads = collector.query(
                    htcondor.AdTypes.Schedd,
                    projection = schedd_ad_projection,
                    constraint = 'MyAddress=?="{0}"'.format(limited_schedd_ad['MyAddress'])
                    )
        except Exception as e:
            logger.debug('Default collector and schedd did not work:\n%s\nTrying via collector host string', e)
            self.get_collector_node_addresses()
            for node in self.collector_node_addresses:
                logger.debug('Querying %s for htcondor.AdTypes.Schedd', node)
                collector = htcondor.Collector(node)
                try:
                    self.schedd_ads = collector.query(
                        htcondor.AdTypes.Schedd,
                        projection = schedd_ad_projection,
                        constraint = self.schedd_constraints
                        )
                    if self.schedd_ads:
                        # As soon as schedd_ads are found in one collector node, use those
                        # This may not be the correct choice for some batch systems
                        break
                except Exception as e:
                    logger.debug('Failed querying %s: %s', node, e)
                    continue
            else:
                logger.error('Failed to collect any schedds from %s', self.collector_node_addresses)
                raise RuntimeError

        logger.debug('Found schedd ads: \n%s', pprint.pformat([dict(d) for d in self.schedd_ads]))
        return self.schedd_ads

    @cache_return_value
    def get_best_schedd(self):
        import htcondor
        self.get_schedd_ads()
        if len(self.schedd_ads) == 1:
            best_schedd_ad = self.schedd_ads[0]
        else:
            self.schedd_ads.sort(key = self.get_schedd_weight)
            best_schedd_ad = self.schedd_ads[0]
        logger.debug('Best schedd is %s', best_schedd_ad['Name'])
        schedd = htcondor.Schedd(best_schedd_ad)
        return schedd

    @cache_return_value
    def get_all_schedds(self):
        import htcondor
        return [ htcondor.Schedd(schedd_ad) for schedd_ad in self.get_schedd_ads() ]

    def get_schedd_weight(self, schedd_ad):
        duty_cycle = schedd_ad['RecentDaemonCoreDutyCycle'] * 100
        occupancy = (schedd_ad['ShadowsRunning'] / schedd_ad['MaxJobsRunning']) * 100
        n_idle_jobs = schedd_ad['TotalIdleJobs']
        weight = 0.7 * duty_cycle + 0.2 * occupancy + 0.1 * n_idle_jobs
        logger.debug(
            'Weight calc for %s: weight = %s, '
            'duty_cycle = %s, occupancy = %s, n_idle_jobs = %s',
            schedd_ad['Name'], weight, duty_cycle, occupancy, n_idle_jobs
            )
        return weight


class ScheddManagerFermiHTC(ScheddManager):
    """
    Subclass of ScheddManager specifically for the Fermilab HTCondor setup
    """
    def __init__(self):
        super(ScheddManagerFermiHTC, self).__init__()
        self.schedd_constraints = 'FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"'

    @cache_return_value
    def get_collector_node_addresses(self):
        import htcondor
        try:
            collector_node_addresses = htcondor.param['FERMIHTC_REMOTE_POOL']
            self.collector_node_addresses = re.findall(r'[\w\/\:\/\-\/\.]+', collector_node_addresses)
            logger.info('Set collector_node_addresses to %s', collector_node_addresses)
        except KeyError:
            super(ScheddManagerFermiHTC, self).get_collector_node_addresses()


# -------------- Convenience functions
# Define a GLOBAL_SCHEDDMAN, so that for basic usage one
# can simply call e.g. 'get_best_schedd'

GLOBAL_SCHEDDMAN_CLS = ScheddManager
GLOBAL_SCHEDDMAN = None

def _get_scheddman(renew):
    global GLOBAL_SCHEDDMAN
    if GLOBAL_SCHEDDMAN is None:
        # Create a new instance
        GLOBAL_SCHEDDMAN = GLOBAL_SCHEDDMAN_CLS()
    if renew:
        GLOBAL_SCHEDDMAN.clear_cache()
    return GLOBAL_SCHEDDMAN

def get_best_schedd(renew=False):
    return _get_scheddman(renew).get_best_schedd()

def get_schedd_ads(renew=False):
    return _get_scheddman(renew).get_schedd_ads()

def get_schedds(renew=False):
    return _get_scheddman(renew).get_all_schedds()


# Some basic condor utilities

def get_jobs(cluster_id, proc_id=None):
    requirements = 'ClusterId=={0}'.format(cluster_id)
    if not(proc_id is None): requirements += ' && ProcId == {0}'.format(proc_id)
    classads = []
    logger.debug('requirements = %s', requirements)
    for schedd in get_schedds():
        classads.extend(list(schedd.xquery(
            requirements = requirements,
            projection = ['ProcId', 'JobStatus']
            )))
    logger.info('Query returned %s', classads)
    return classads

def remove_jobs(cluster_id):
    import htcondor
    logger.info('Removing cluster_id %s', cluster_id)
    for schedd in get_schedds():
        schedd.act(htcondor.JobAction.Remove, 'ClusterId=?="{0}"'.format(cluster_id))

def wait(cluster_id, proc_id=None, n_sleep=10):
    while True:
        states = [j['JobStatus'] for j in get_jobs(cluster_id, proc_id)]
        is_done = all([ not(state in [1, 2]) for state in states ])
        if is_done:
            logger.info('ClusterId %s ProcId %s seems done', cluster_id, proc_id)
            break
        else:
            logger.info(
                'ClusterId %s ProcId %s not yet done:\n%s',
                cluster_id, proc_id, states
                )
            logger.info('Sleeping for %s seconds before checking again', n_sleep)
            sleep(n_sleep)

# _____________________________________________________________________
# Submission utils

def get_default_sub():
    """
    Returns the default submission dict (the equivalent of a .jdl file)
    to be used by the submitter.
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
            'CLUSTER_SUBMISSION_TIMESTAMP' : strftime(qondor.TIMESTAMP_FMT),
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

def _change_submitobject_env_variable(submitobject, key, value):
    """
    Tries to replace an environment variable in a Submit-object.
    This hack is needed to have items be in the same cluster.
    """
    env = submitobject['environment']
    new_env = re.sub(key + r'=\'.*?\'', '{0}=\'{1}\''.format(key, value), env)
    logger.debug('Replacing:\n  %s\n  by\n  %s', env, new_env)
    submitobject['environment'] = new_env

def _htcondor_format_environment(env):
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

@contextmanager
def _transaction(schedd, dry=None):
    """
    Wrapper for schedd.transaction, + the ability for dry mode
    """
    if dry is None: dry = qondor.DRYMODE
    try:
        if not dry:
            with schedd.transaction() as transaction:
                yield transaction
        else:
            yield None
    finally:
        pass

def _format_item(item):
    """
    Format an item (most likely a list) to a flat string (which will read by the job)
    """
    # Ensure the string will be ',' separated (a len-1 list will not have a comma)
    # Also ensure there are no quotes in it, which would screw up condor's reading
    if qondor.utils.is_string(item):
        item = [item]
    elif len(item) == 1:
        # Exceptional case: a len-1 list passed is indistinguishable from just a string
        # If chunk_size is set to 1, it is intended that QONDORITEM is a len-1 list,
        # but the ','.join() statement makes the item look like a simple string.
        # Add an initial comma as a hack to tell qondor that the item is meant to be a
        # len-1 list.
        item[0] = ',' + item[0]
    item = ','.join([i.replace('\'','').replace('"','') for i in item])
    return item

def _format_chunk(chunk):
    """
    Format a rootfile chunk (most likely a list) to a flat string (which will read by the job)
    """
    # Set the chunk as a string in the environment as follows:
    # rootfile,first,last,is_whole_file;rootfile,first,last,is_whole_file;...
    # Convert is_whole_file to either 0 or 1 first (converting bool to str
    # yields 'True' or 'False', but str('False') yields True...)
    return ';'.join(
        [','.join([ str(e[0]), str(e[1]), str(e[2]), str(int(e[3])) ]) for e in chunk]
        )

def submit_pythonbindings(
    submissiondict, submissiondir='.', schedd=None, dry=None,
    items=None, rootfile_chunks=None, njobs=1, njobsmax=None
    ):
    """
    Main interface to htcondor via the python bindings.
    """
    if dry is None: dry = qondor.DRYMODE # Inherit global drymode flag
    import htcondor
    if schedd is None: schedd = get_best_schedd(renew=True)
    sub = submissiondict.copy() # Create a copy to keep original dict unmodified
    # List to store all submitted job ads
    job_ads = []
    cluster_id = [0]
    n_queued = [0] # Make it a 1-element list so that it's a 'pointer'
    with qondor.utils.switchdir(submissiondir):
        # Make the transaction
        with _transaction(schedd) as transaction:
            # Helper function to actually submit a job to this transaction
            # Modifies the 'global' job_ads and cluster_id variables
            def queue(submit_object, njobs=1):
                ads = []
                n_queued[0] += njobs
                if dry: return
                cluster_id[0] = int(submit_object.queue(transaction, njobs, ads))
                job_ads.extend(ads)
            # Helper function to test whether the needed amount of jobs to submit
            # is already reached
            def should_quit_now():
                if not(njobsmax is None) and n_queued[0] >= njobsmax:
                    return True
                return False
            # Choose specific submit behavior based on what extra keywords were passed
            if items:
                # Items logic: Turn any potential list into a ','-separated string,
                # and set the environment variable QONDORITEM to that string.
                sub['environment']['QONDORITEM'] = 'placeholder' # Placeholder
                sub['environment'] = _htcondor_format_environment(sub['environment'])
                submit_object = htcondor.Submit(sub)
                for item in items:
                    # Change the env variable in the submitobject
                    _change_submitobject_env_variable(submit_object, 'QONDORITEM', _format_item(item))
                    # Submit again and record cluster_id and job ads
                    queue(submit_object)
                    if should_quit_now(): break
            elif rootfile_chunks:
                sub['environment']['QONDORROOTFILECHUNK'] = 'placeholder'
                sub['environment'] = _htcondor_format_environment(sub['environment'])
                submit_object = htcondor.Submit(sub)                
                for chunk in rootfile_chunks:
                    _change_submitobject_env_variable(submit_object, 'QONDORROOTFILECHUNK', _format_chunk(chunk))
                    queue(submit_object)
                    if should_quit_now(): break
            else:
                sub['environment'] = _htcondor_format_environment(sub['environment'])
                submit_object = htcondor.Submit(sub)
                queue(submit_object, njobs if (njobsmax is None) else min(njobs, njobsmax))
    logger.info('Submitted %s jobs to cluster %s', n_queued[0], cluster_id)
    return cluster_id[0], n_queued[0] if dry else job_ads

def submit_condor_submit_commandline(
    submissiondict, submissiondir='.', dry=None,
    items=None, rootfile_chunks=None, njobs=1, njobsmax=None,
    do_not_submit=False
    ):
    """
    Main interface to htcondor via the condor_submit command line tool.
    """
    if dry is None: dry = qondor.DRYMODE # Inherit global drymode flag
    sub = submissiondict.copy() # Create a copy to keep original dict unmodified
    # List to store all submitted job ads
    job_ads = []
    cluster_id = 0
    n_queued = 0
    # Helper function to test whether we should stop submitting now
    def should_quit_now():
        return not(njobsmax is None) and n_queued >= njobsmax
    with qondor.utils.switchdir(submissiondir):
        # Write the .jdl file
        jdl_file = osp.splitext(submissiondict['executable'])[0] + '.jdl'
        logger.debug('Writing submission to %s', jdl_file)
        with qondor.utils.openfile(jdl_file, 'w') as jdl:
            # Write (most) keys in the submission dict to a file
            for key in submissiondict.keys():
                if key.lower() == 'environment': continue
                val = submissiondict[key]
                jdl.write('{} = {}\n'.format(key, val))
            # Choose specific submit behavior based on what extra keywords were passed
            if items:
                # Items logic: Turn any potential list into a ','-separated string,
                # and set the environment variable QONDORITEM to that string.
                for item in items:
                    sub['environment']['QONDORITEM'] = _format_item(item)
                    jdl.write('\nenvironment = {}\n'.format(_htcondor_format_environment(sub['environment'])))
                    jdl.write('queue\n')
                    n_queued += 1
                    if should_quit_now(): break
            elif rootfile_chunks:
                for chunk in rootfile_chunks:
                    sub['environment']['QONDORROOTFILECHUNK'] = _format_chunk(chunk)
                    jdl.write('\nenvironment = {}\n'.format(_htcondor_format_environment(sub['environment'])))
                    jdl.write('queue\n')
                    n_queued += 1
                    if should_quit_now(): break
            else:
                if not(njobsmax is None): njobs = min(njobsmax, njobs)
                jdl.write('environment = {}\n'.format(_htcondor_format_environment(sub['environment'])))
                jdl.write('queue {}\n'.format(njobs))
                n_queued += njobs
        logger.debug('Queued %s jobs', n_queued)
        # Quit now if we don't actually want to submit
        if do_not_submit:
            logger.debug('do_not_submit == True, quiting now')
            return 0, n_queued
        # Run condor_submit on the jdl file (still in the submission directory)
        output = qondor.utils.run_command(
            ['condor_submit', jdl_file],
            # env=qondor.utils.get_clean_env()
            )
        if dry:
            logger.info('Submitted %s jobs to cluster_id 0', n_queued)
            return 0, n_queued
        match = re.search(r'(\d+) job\(s\) submitted to cluster (\d+)', '\n'.join(output))
        if match:
            n_submitted = int(match.group(1))
            cluster_id = match.group(2)
            logger.info('Submitted %s jobs to cluster_id %s', n_submitted, cluster_id)
            return cluster_id, n_submitted
        else:
            logger.error(
                'condor_submit exited ok but could not determine where and how many jobs were submitted'
                )
            return 0, n_queued
