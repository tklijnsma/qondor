#!/usr/bin/env python
# -*- coding: utf-8 -*-
import qondor
import logging, re
from time import sleep
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
            logger.debug('Returning cached value for %s', func.__name__)
        return wrapper.cached_return_value
    return wrapper

def clear_cache():
    global CACHED_FUNCTIONS
    for func in CACHED_FUNCTIONS:
        func.is_called = False
    CACHED_FUNCTIONS = []


class ScheddManager(object):
    """docstring for ScheddManager """
    def __init__(self):
        super(ScheddManager, self).__init__()
        self.schedd_ads = []

    def clear_cache(self):
        """Convenience function to not have to import clear_cache everywhere"""
        clear_cache()

    @cache_return_value
    def get_a_string(self):
        s = 'blablo'
        logger.info('Getting string %s', s)
        return s

    @cache_return_value
    def get_fermihtc_remote_pool(self):
        import htcondor
        try:
            self.FERMIHTC_REMOTE_POOL = htcondor.param.get("FERMIHTC_REMOTE_POOL")
        except:
            logger.error('htcondor.param.get("FERMIHTC_REMOTE_POOL") failed')
            raise
        if self.FERMIHTC_REMOTE_POOL is None:
            logger.error('htcondor.param.get("FERMIHTC_REMOTE_POOL") returned None')
            raise RuntimeError
        logger.debug('Found FERMIHTC_REMOTE_POOL %s', self.FERMIHTC_REMOTE_POOL)
        # Seems like a very obfuscated way of writing ".split()" but keep it for now
        self.remote_pool = re.findall(r'[\w\/\:\/\-\/\.]+', self.FERMIHTC_REMOTE_POOL)
        logger.debug('Using remote_pool %s', self.remote_pool)

    @cache_return_value
    def get_schedd_ads(self):
        import htcondor
        self.get_fermihtc_remote_pool()
        for node in self.remote_pool:
            collector = htcondor.Collector(node)
            try:
                self.schedd_ads = collector.query(
                    htcondor.AdTypes.Schedd,
                    projection = [
                        'Name', 'MyAddress', 'MaxJobsRunning', 'ShadowsRunning',
                        'RecentDaemonCoreDutyCycle', 'TotalIdleJobs'
                        ],
                    constraint = 'FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"'
                    )
                if self.schedd_ads:
                    break
            except Exception as e:
                logger.debug('Failed querying %s: %s', node, e)
                continue
        else:
            logger.error('Failed to collect any schedds from %s', self.remote_pool)
            raise RuntimeError
        logger.debug('Found schedd ads %s', self.schedd_ads)

    @cache_return_value
    def get_best_schedd(self):
        import htcondor
        self.get_schedd_ads()
        self.schedd_ads.sort(key = self.get_schedd_weight)
        best_schedd_ad = self.schedd_ads[0]
        logger.debug('Best schedd is %s', best_schedd_ad['Name'])
        schedd = htcondor.Schedd(best_schedd_ad)
        return schedd

    @cache_return_value
    def get_all_schedds(self):
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


# Convenience functions

def _get_scheddman(renew):
    scheddman = ScheddManager()
    if renew: scheddman.clear_cache()
    return scheddman    

def get_best_schedd(renew=False):
    return _get_scheddman(renew).get_best_schedd()

def get_schedd_ads(renew=False):
    return _get_scheddman(renew).get_schedd_ads()

def get_schedds(renew=False):
    return _get_scheddman(renew).get_all_schedds()



def get_jobs(cluster_id, proc_id=None):
    requirements = 'ClusterId=={0}'.format(cluster_id)
    if not(proc_id is None): requirements += ' && ProcId == {0}'.format(proc_id)
    classads = []
    for schedd in get_schedds():
        classads.extend(list(schedd.xquery(
            requirements = requirements,
            projection = ['ProcId', 'JobStatus']
            )))
    logger.info('Query returned %s', classads)
    return classads

def wait(cluster_id, proc_id=None, n_sleep=10):
    while True:
        states = [j['JobStatus'] for j in get_jobs(cluster_id, proc_id)]
        is_done = [ not(state in [1, 2]) for state in states ]
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
