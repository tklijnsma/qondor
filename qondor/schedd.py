#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import pprint
import re
from contextlib import contextmanager
from time import sleep, strftime

import qondor

logger = logging.getLogger("qondor")


CACHED_FUNCTIONS = []


def cache_return_value(func):
    """
    Decorator that only calls a function once, and
    subsequent calls just return the cached return value
    """
    global CACHED_FUNCTIONS

    def wrapper(*args, **kwargs):
        if not getattr(wrapper, "is_called", False):
            wrapper.is_called = True
            wrapper.cached_return_value = func(*args, **kwargs)
            CACHED_FUNCTIONS.append(wrapper)
        else:
            logger.debug(
                "Returning cached value for %s: %s",
                func.__name__,
                wrapper.cached_return_value,
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

        if not (qondor.COLLECTOR_NODES is None):
            logger.info(
                "Setting collectors to qondor.COLLECTOR_NODES %s",
                qondor.COLLECTOR_NODES,
            )
            self.collector_node_addresses = qondor.COLLECTOR_NODES
            return

        err_msg = (
            "Could not find any collector nodes. "
            "Either set qondor.COLLECTOR_NODES manually to a ', '-separated "
            "list of addresses, or set the htcondor parameter 'COLLECTOR_HOST_STRING',"
            " or subclass qondor.schedd.ScheddManager so that it can find the collector nodes."
        )
        try:
            collector_node_addresses = (
                htcondor.param["COLLECTOR_HOST_STRING"].strip().strip('"')
            )
        except Exception:
            logger.error(err_msg)
            raise
        if collector_node_addresses is None:
            RuntimeError(err_msg)

        logger.debug("Found collector nodes %s", collector_node_addresses)
        # Seems like a very obfuscated way of writing ".split()" but keep it for now
        self.collector_node_addresses = re.findall(
            r"[\w\/\:\/\-\/\.]+", collector_node_addresses
        )
        logger.debug("Using collector_node_addresses %s", self.collector_node_addresses)

    @cache_return_value
    def get_schedd_ads(self):
        import htcondor

        schedd_ad_projection = [
            "Name",
            "MyAddress",
            "MaxJobsRunning",
            "ShadowsRunning",
            "RecentDaemonCoreDutyCycle",
            "TotalIdleJobs",
        ]
        try:
            logger.debug("First trying default collector and schedd")
            # This is most likely to work for most batch systems
            collector = htcondor.Collector()
            limited_schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd)
            logger.debug(
                "Retrieved limited schedd ad:\n%s", pprint.pformat(limited_schedd_ad)
            )
            self.schedd_ads = collector.query(
                htcondor.AdTypes.Schedd,
                projection=schedd_ad_projection,
                constraint='MyAddress=?="{0}"'.format(limited_schedd_ad["MyAddress"]),
            )
        except Exception as e:
            logger.debug(
                "Default collector and schedd did not work:\n%s\nTrying via collector host string",
                e,
            )
            self.get_collector_node_addresses()
            for node in self.collector_node_addresses:
                logger.debug("Querying %s for htcondor.AdTypes.Schedd", node)
                collector = htcondor.Collector(node)
                try:
                    self.schedd_ads = collector.query(
                        htcondor.AdTypes.Schedd,
                        projection=schedd_ad_projection,
                        constraint=self.schedd_constraints,
                    )
                    if self.schedd_ads:
                        # As soon as schedd_ads are found in one collector node, use those
                        # This may not be the correct choice for some batch systems
                        break
                except Exception as e:
                    logger.debug("Failed querying %s: %s", node, e)
                    continue
            else:
                logger.error(
                    "Failed to collect any schedds from %s",
                    self.collector_node_addresses,
                )
                raise RuntimeError

        logger.debug(
            "Found schedd ads: \n%s", pprint.pformat([dict(d) for d in self.schedd_ads])
        )
        return self.schedd_ads

    @cache_return_value
    def get_best_schedd(self):
        import htcondor

        self.get_schedd_ads()
        if len(self.schedd_ads) == 1:
            best_schedd_ad = self.schedd_ads[0]
        else:
            self.schedd_ads.sort(key=self.get_schedd_weight)
            best_schedd_ad = self.schedd_ads[0]
        logger.debug("Best schedd is %s", best_schedd_ad["Name"])
        schedd = htcondor.Schedd(best_schedd_ad)
        return schedd

    @cache_return_value
    def get_all_schedds(self):
        import htcondor

        return [htcondor.Schedd(schedd_ad) for schedd_ad in self.get_schedd_ads()]

    def get_schedd_weight(self, schedd_ad):
        duty_cycle = schedd_ad["RecentDaemonCoreDutyCycle"] * 100
        occupancy = (schedd_ad["ShadowsRunning"] / schedd_ad["MaxJobsRunning"]) * 100
        n_idle_jobs = schedd_ad["TotalIdleJobs"]
        weight = 0.7 * duty_cycle + 0.2 * occupancy + 0.1 * n_idle_jobs
        logger.debug(
            "Weight calc for %s: weight = %s, "
            "duty_cycle = %s, occupancy = %s, n_idle_jobs = %s",
            schedd_ad["Name"],
            weight,
            duty_cycle,
            occupancy,
            n_idle_jobs,
        )
        return weight


class ScheddManagerFermiHTC(ScheddManager):
    """
    Subclass of ScheddManager specifically for the Fermilab HTCondor setup
    """

    def __init__(self):
        super(ScheddManagerFermiHTC, self).__init__()
        self.schedd_constraints = (
            'FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"'
        )

    @cache_return_value
    def get_collector_node_addresses(self):
        import htcondor

        try:
            collector_node_addresses = htcondor.param["FERMIHTC_REMOTE_POOL"]
            self.collector_node_addresses = re.findall(
                r"[\w\/\:\/\-\/\.]+", collector_node_addresses
            )
            logger.info("Set collector_node_addresses to %s", collector_node_addresses)
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


# _____________________________________________________________________
# Some basic condor utilities

_status_str_to_int = {
    "U": 0,  # Unexpanded
    "I": 1,  # Idle
    "R": 2,  # Running
    "X": 3,  # Removed
    "C": 4,  # Completed
    "H": 5,  # Held
    "E": 6,  # Submission_err
}

_status_int_to_str = {
    0: "Unexpanded",
    1: "Idle",
    2: "Running",
    3: "Removed",
    4: "Completed",
    5: "Held",
    6: "Submission_err",
}


class QueuedJob(object):
    """
    Simple container for a job that is currently in the queue of htcondor.
    Can be initialized both by the python-bindings or the condor_q command line
    """

    @classmethod
    def from_ad(cls, ad):
        return cls(
            ad["ClusterId"],
            ad["ProcId"],
            ad["JobStatus"],
            user=ad["Owner"],
            rundir=ad["Iwd"],
        )

    def __init__(
        self, cluster_id, proc_id, status, user=None, sh_base=None, rundir=None
    ):
        self.cluster_id = cluster_id
        self.proc_id = proc_id
        self.status = status
        self.user = user
        self.sh_base = sh_base
        self.rundir = rundir

    def __repr__(self):
        return (
            super(QueuedJob, self)
            .__repr__()
            .replace(
                "object",
                "object {}.{} {}".format(
                    self.cluster_id, self.proc_id, self.status_str()
                ),
            )
        )

    def status_str(self):
        return _status_int_to_str[self.status]


def sort_jobs(list_of_jobs):
    list_of_jobs.sort(key=lambda job: (job.cluster_id, job.proc_id))


def get_jobs_bindings(cluster_id=None, proc_id=None, user="auto"):
    requirements = []
    if cluster_id:
        requirements.append("ClusterId=={0}".format(cluster_id))
    if proc_id:
        requirements.append("ProcId == {0}".format(proc_id))
    if user == "auto":
        import getpass

        user = getpass.getuser()
    if user:
        requirements.append('Owner == "{0}"'.format(user))
    requirements = " && ".join(requirements)
    classads = []
    logger.debug("requirements = %s", requirements)
    for schedd in get_schedds():
        classads.extend(
            list(
                schedd.xquery(
                    requirements=requirements,
                    projection=["ClusterId", "ProcId", "JobStatus", "Iwd", "Owner"],
                )
            )
        )
    logger.info("Query returned %s", classads)
    jobs = [QueuedJob.from_ad(ad) for ad in classads]
    sort_jobs(jobs)
    return jobs


def remove_jobs(cluster_id):
    import htcondor

    logger.info("Removing cluster_id %s", cluster_id)
    for schedd in get_schedds():
        schedd.act(htcondor.JobAction.Remove, 'ClusterId=?="{0}"'.format(cluster_id))


def wait(cluster_id, proc_id=None, n_sleep=10):
    while True:
        states = [j["JobStatus"] for j in get_jobs(cluster_id, proc_id)]
        is_done = all([not (state in [1, 2]) for state in states])
        if is_done:
            logger.info("ClusterId %s ProcId %s seems done", cluster_id, proc_id)
            break
        else:
            logger.info(
                "ClusterId %s ProcId %s not yet done:\n%s", cluster_id, proc_id, states
            )
            logger.info("Sleeping for %s seconds before checking again", n_sleep)
            sleep(n_sleep)


def get_jobs_cli(cluster_id=None, proc_id=None):
    output = qondor.utils.run_command("condor_q", env="clean", shell=True)
    jobs = []
    for line in output:
        line = line.strip()
        if not (len(line)):
            continue
        components = line.split()
        if not re.match(r"\d+\.\d+", components[0]):
            continue
        this_cluster_id, this_proc_id = map(int, components[0].split("."))
        if not (cluster_id is None) and this_cluster_id != cluster_id:
            continue
        if not (proc_id is None) and this_proc_id != proc_id:
            continue
        status_str = components[5]
        status = _status_str_to_int.get(status_str, -1)
        sh_base = components[-1]
        user = components[1]
        jobs.append(
            QueuedJob(this_cluster_id, this_proc_id, status, user=user, sh_base=sh_base)
        )
    sort_jobs(jobs)
    return jobs


def get_jobs(*args, **kwargs):
    method = kwargs.pop("method", None)
    if method is None:
        try:
            import htcondor  # noqa F401

            return get_jobs_bindings(*args, **kwargs)
        except ImportError:
            return get_jobs_cli(*args, **kwargs)


# _____________________________________________________________________
# Submission utils


def get_default_sub():
    """
    Returns the default submission dict (the equivalent of a .jdl file)
    to be used by the submitter.
    """
    sub = {
        "universe": "vanilla",
        "output": "out_$(Cluster)_$(Process).txt",
        "error": "err_$(Cluster)_$(Process).txt",
        "log": "log_$(Cluster)_$(Process).txt",
        "should_transfer_files": "YES",
        "environment": {
            "QONDOR_BATCHMODE": "1",
            "CONDOR_CLUSTER_NUMBER": "$(Cluster)",
            "CONDOR_PROCESS_ID": "$(Process)",
            "CLUSTER_SUBMISSION_TIMESTAMP": strftime(qondor.TIMESTAMP_FMT),
        },
    }
    # Try to set some more things
    try:
        sub["x509userproxy"] = os.environ["X509_USER_PROXY"]
    except KeyError:
        try:
            sub["x509userproxy"] = qondor.utils.run_command(
                ["voms-proxy-info", "-path"]
            )[0].strip()
            logger.info(
                'Set x509userproxy to "%s" based on output from voms-proxy-info',
                sub["x509userproxy"],
            )
        except Exception:
            logger.warning(
                "Could not find a x509userproxy to pass; manually "
                "set the htcondor variable 'x509userproxy' if your "
                "htcondor setup requires it."
            )
    try:
        sub["environment"]["USER"] = os.environ["USER"]
    except KeyError:
        # No user specified, no big deal
        pass
    return sub


def _change_submitobject_env_variable(submitobject, key, value):
    """
    Tries to replace an environment variable in a Submit-object.
    This hack is needed to have items be in the same cluster.
    """
    env = submitobject["environment"]
    new_env = re.sub(key + r"=\'.*?\'", "{0}='{1}'".format(key, value), env)
    logger.debug("Replacing:\n  %s\n  by\n  %s", env, new_env)
    submitobject["environment"] = new_env


def format_env_htcondor(env):
    """
    Takes a dict of key : value pairs that are both strings, and
    returns a string that is formatted so that htcondor can turn it
    into environment variables
    """
    return (
        '"'
        + " ".join(["{0}='{1}'".format(key, value) for key, value in env.items()])
        + '"'
    )


@contextmanager
def _transaction(schedd, dry=None):
    """
    Wrapper for schedd.transaction, + the ability for dry mode
    """
    if dry is None:
        dry = qondor.DRYMODE
    try:
        if not dry:
            with schedd.transaction() as transaction:
                yield transaction
        else:
            yield None
    finally:
        pass
