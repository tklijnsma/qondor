# -*- coding: utf-8 -*-
import glob
import logging
import os.path as osp
import re

import qondor

logger = logging.getLogger("qondor")


def build_resubmission(jsonfile):
    if osp.isdir(jsonfile):
        # Treat it as a rundir
        jsonfile = osp.join(jsonfile, "submission_latest.json")
    if not osp.isfile(jsonfile):
        raise Exception("Pass a path to a rundir or a specific json file")
    logger.debug("Loading %s", jsonfile)
    with open(jsonfile, "r") as f:
        submission = qondor._json_load_byteified(f)
    resub = Resubmission(jsonfile, **submission)
    return resub


class ClusterFlags:
    PARTIALLY_NOT_STARTED = 1
    COMPLETED_UNSUCCESSFULLY = 10
    COMPLETED_SUCCESSFULLY = 11

    @classmethod
    def strrep(cls, i):
        for key in cls.__dict__:
            if cls.__dict__[key] == i:
                return key
        else:
            raise Exception("No such state: {}".format(i))


class JobFlags:
    GOT_SUBMITTED = 0
    NOT_STARTED = 1
    IN_HTCONDOR = 2
    HTCONDOR_ERROR = 3
    EXITED = 4
    GOOD_EXITCODE = 5
    BAD_EXITCODE = 6
    NO_EXITCODE = 7


SUCCESSES = [JobFlags.GOOD_EXITCODE]
FAILURES = [JobFlags.HTCONDOR_ERROR, JobFlags.BAD_EXITCODE, JobFlags.NO_EXITCODE]


def int_to_str(status, color=True):
    for key in JobFlags.__dict__:
        if JobFlags.__dict__[key] == status:
            if color:
                if status in SUCCESSES:
                    key = qondor.colored(key, "green")
                elif status in FAILURES:
                    key = qondor.colored(key, "red")
                elif status in [JobFlags.NOT_STARTED, JobFlags.IN_HTCONDOR]:
                    key = qondor.colored(key, "yellow")
            return key
    else:
        raise Exception("No such flag: {}".format(status))


def ints_to_str(statuss, color=True):
    return ",".join([int_to_str(s, color) for s in statuss])


class Job(object):
    """
    Simple container to store the current state of a job
    """

    def __init__(self, cluster_id, proc_id, flags=None):
        self.cluster_id = cluster_id
        self.proc_id = proc_id
        self.flags = [] if flags is None else flags
        self.htcondor_status = None
        self.htcondor_status_str = None

    def is_running(self):
        return JobFlags.IN_HTCONDOR in self.flags and self.htcondor_status not in [5, 6]

    def is_failed(self):
        if self.is_running():
            return False
        for failure in FAILURES:
            if failure in self.flags:
                return True
        return False

    def is_success(self):
        return JobFlags.GOOD_EXITCODE in self.flags

    def __repr__(self):
        r = "{}.{} {}".format(self.cluster_id, self.proc_id, ints_to_str(self.flags))
        if self.htcondor_status_str is not None:
            r += " " + self.htcondor_status_str
        return r


class Resubmission(object):
    def __init__(self, jsonfile, **kwargs):
        self.jsonfile = osp.abspath(jsonfile)
        self.rundir = osp.dirname(self.jsonfile)
        self.__dict__.update(**kwargs)
        self.out_files = glob.glob(self.rundir + "/out_*.txt")
        self.err_files = glob.glob(self.rundir + "/err_*.txt")
        self.log_files = glob.glob(self.rundir + "/log_*.txt")
        self.exitcode_files = glob.glob(self.rundir + "/exitcode_*.txt")
        self._jobs_cache = []
        self._jobs_read = False

    def iter_only_failed_jobs(self):
        for sub, cluster_id, njobs, proc_ids, jobs in self._jobs_cache:
            failed_jobs = list(filter(lambda j: j.is_failed(), jobs))
            proc_ids = [j.proc_id for j in failed_jobs]
            yield sub, cluster_id, njobs, proc_ids, failed_jobs

    def jobs(self, only_failed=False):
        if not self._jobs_read:
            self.read_jobs()
        if only_failed:
            return list(self.iter_only_failed_jobs())
        return self._jobs_cache

    def job_objects(self, only_failed=False):
        for _, _, _, _, jobs in self.jobs(only_failed):
            yield jobs

    def read_jobs(self):
        self._jobs_read = True
        self._jobs_cache = []
        all_queued_jobs = qondor.schedd.get_jobs()
        for sub, cluster_id, njobs, proc_ids in self.submitted:
            name = sub["environment"]["QONDORCLUSTERNAME"]
            logger.debug(
                "Found job %s: cluster_id=%s, njobs=%s, proc_ids=%s",
                name,
                cluster_id,
                njobs,
                proc_ids,
            )
            # Gather the files for this submission created by htcondor
            out_files = []
            err_files = []
            log_files = []
            exitcode_files = []
            for filetype in ["out", "err", "log", "exitcode"]:
                thelist = locals()[filetype + "_files"]
                pat = re.compile(r"{}_{}_\d+_\d+\.txt".format(filetype, name))
                for file in getattr(self, filetype + "_files"):
                    if pat.match(file):
                        thelist.append(filetype)
            # Get the queued jobs for this submission in htcondor
            queued_jobs = list(
                filter(lambda job: job.cluster_id == cluster_id, all_queued_jobs)
            )

            def find_in_queue(job):
                for queued_job in queued_jobs:
                    if queued_job.proc_id == job.proc_id:
                        return queued_job
                else:
                    return None

            # Per job in the submission, determine the flags
            jobs = []
            for proc_id in proc_ids:
                job = Job(cluster_id, proc_id)
                # Check if the job exists in the htcondor queue
                queued_job = find_in_queue(job)
                if not (queued_job is None):
                    job.flags.append(JobFlags.IN_HTCONDOR)
                    job.htcondor_status = queued_job.status
                    job.htcondor_status_str = queued_job.status_str()
                    # htcondor status's 5 and 6 mean an error
                    if job.htcondor_status == 5 or job.htcondor_status == 6:
                        job.flags.append(JobFlags.HTCONDOR_ERROR)
                # Check if a .log file was created for the job
                if osp.isfile(
                    osp.join(
                        self.rundir,
                        "log_{}_{}_{}.txt".format(name, cluster_id, proc_id),
                    )
                ):
                    job.flags.append(JobFlags.GOT_SUBMITTED)
                else:
                    job.flags.append(JobFlags.NOT_STARTED)
                # Check if a .out file was created for the job; if so, the job must have exited
                if osp.isfile(
                    osp.join(
                        self.rundir,
                        "out_{}_{}_{}.txt".format(name, cluster_id, proc_id),
                    )
                ):
                    job.flags.append(JobFlags.EXITED)
                # Check if an exitcode file was created for the job, and if so check wether
                # the exitcode equals 0
                exitcode_file = osp.join(
                    self.rundir,
                    "exitcode_{}_{}_{}.txt".format(name, cluster_id, proc_id),
                )
                if osp.isfile(exitcode_file):
                    with open(exitcode_file, "r") as f:
                        exitcode = int(f.read().strip())
                    if exitcode == 0:
                        job.flags.append(JobFlags.GOOD_EXITCODE)
                    else:
                        job.flags.append(JobFlags.BAD_EXITCODE)
                else:
                    job.flags.append(JobFlags.NO_EXITCODE)
                jobs.append(job)
            sub_tuple = (sub, cluster_id, njobs, proc_ids, jobs)
            self._jobs_cache.append(sub_tuple)

    def print_jobs(self, only_failed=False, summary=False):
        r = []
        n_good = 0
        n_failed = 0
        n_running = 0
        n_total = 0
        for sub, cluster_id, njobs, proc_ids, jobs in self.jobs(only_failed):
            if not len(jobs):
                continue
            name = sub["environment"]["QONDORCLUSTERNAME"]
            i_cluster = int(sub["environment"]["QONDORICLUSTER"])
            r.append("{: <3} {}".format(i_cluster, name))
            for proc_id, job in zip(proc_ids, jobs):
                r.append("  " + repr(job))
                if job.is_success():
                    n_good += 1
                elif job.is_failed():
                    n_failed += 1
                else:
                    n_running += 1
                n_total += 1
        if summary:
            r.append(
                "total: {}, good: {} ({:.1f}%), failed: {} ({:.1f}%), running: {} ({:.1f}%)".format(
                    n_total,
                    n_good,
                    (100.0 * n_good) / n_total,
                    n_failed,
                    (100.0 * n_failed) / n_total,
                    n_running,
                    (100.0 * n_running) / n_total,
                )
            )
        print("\n".join(r))

    def resubmit(self, cli=True):
        with qondor.utils.switchdir(self.rundir):
            session = qondor.submit.Session("resubmission")
            for sub_orig, cluster_id, njobs, proc_ids, jobs in self.jobs(
                only_failed=True
            ):
                sub = sub_orig.copy()
                for job in jobs:
                    logger.info("Submitting job %s", job)
                    sub["environment"]["QONDOR_PROC_ID_RESUBMISSION"] = job.proc_id
                    session.submittables.append((sub, 1))
            session.submit(cli)
