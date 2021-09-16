# -*- coding: utf-8 -*-
import json
import logging
import os
import os.path as osp
import pprint
import re
import shutil
from datetime import datetime
from collections import OrderedDict

import seutils

import qondor

logger = logging.getLogger("qondor")


# _____________________________________________________________________
# Interface to translate python code (typically in a file) into job submissions


def split_runcode_submitcode(lines):
    """
    Splits a list of lines into two strings, the python to run in the job
    and the other the python code to submit the jobs.
    """
    runcode_lines = []
    submitcode_lines = []
    is_runcode_mode = True
    is_submitcode_mode = False
    for line in lines:
        line = line.strip("\n")
        if line.startswith('"""# submit'):
            if is_submitcode_mode:
                raise Exception(
                    "Encountered submit code opening tag, but was already in submit mode"
                )
            # Toggle the mode
            is_runcode_mode = not (is_runcode_mode)
            is_submitcode_mode = not (is_submitcode_mode)
            continue
        elif line.startswith('"""# endsubmit'):
            if is_runcode_mode:
                raise Exception(
                    "Encountered submit code closing tag, but was already not in submit mode"
                )
            # Toggle the mode
            is_runcode_mode = not (is_runcode_mode)
            is_submitcode_mode = not (is_submitcode_mode)
            continue
        else:
            if is_runcode_mode:
                runcode_lines.append(line)
            else:
                submitcode_lines.append(line)
    submitcode = "\n".join(submitcode_lines)
    runcode = "\n".join(runcode_lines)
    return runcode, submitcode


def split_runcode_submitcode_file(filename):
    """
    Wrapper for split_runcode_submitcode that opens up the file first
    """
    with open(filename, "r") as f:
        return split_runcode_submitcode(f.readlines())


def exec_wrapper(code, scope):
    """
    Python 2 has problems with function definitions in the same scope as the exec.
    Solution: Run exec in a subscope with this wrapper function.
    """
    exec(code, scope)


class StopProcessing(Exception):
    """
    Special exception to stop execution inside an exec statement
    """

    pass

# Session: pips, run_args, n_jobs_max, cli, htcondor settings, environment variables
# Cluster: scope, environment variables


def submit_python_job_file(
    filename, cli=False, njobsmax=None, run_args=None, return_first_cluster=False
):
    """
    Builds submission clusters from a python job file
    """
    runcode, submitcode = split_runcode_submitcode_file(filename)
    # Run the submitcode
    # First create exec scope dict, with a few handy functions
    _first_cluster_ptr = [None]
    n_calls_to_submit_fn = [0]
    session = Session(name=osp.basename(filename).replace(".py", ""))
    # Place to store 'global' pip installs
    pips = []

    def htcondor_setting(key, value):
        session.htcondor(key, value)

    def pip_fn(package, install_instruction=None):
        if install_instruction:
            pips.append((package, install_instruction))
        else:
            pips.append(package)

    def submit_fn(*args, **kwargs):
        kwargs.setdefault("session", session)
        kwargs.setdefault("run_args", run_args)
        kwargs["pips"] = kwargs.get("pips", []) + pips  # Add 'global' installs
        njobs = kwargs.get("njobs", 1)
        cluster = Cluster(runcode, *args, **kwargs)
        if return_first_cluster:
            _first_cluster_ptr[0] = cluster
            raise StopProcessing
        session.add_submission(cluster, cli=cli, njobsmax=njobsmax, njobs=njobs)
        n_calls_to_submit_fn[0] += 1

    def submit_now_fn():
        session.submit(cli, njobsmax=njobsmax)

    exec_scope = {
        "qondor": qondor,
        "session": session,
        "htcondor": htcondor_setting,
        "submit": submit_fn,
        "pip": pip_fn,
        "submit_now": submit_now_fn,
        "runcode": runcode,
        "run_args": run_args,
        "cli": cli,
        "njobsmax": njobsmax,
        "return_first_cluster": return_first_cluster,
    }
    logger.info("Running submission code now")
    if return_first_cluster:
        try:
            exec_wrapper(submitcode, exec_scope)
        except StopProcessing:
            pass
        cluster = _first_cluster_ptr[0]
        if cluster is None:
            raise Exception("No cluster was submitted in the submit code")
        return cluster
    else:
        exec_wrapper(submitcode, exec_scope)
        # Special case: There was no call to submit
        # Just submit 1 job in that case
        if n_calls_to_submit_fn[0] == 0:
            logger.info(
                "No calls to submit() were made in the submit code; submitting 1 job"
            )
            cluster = Cluster(runcode, session=session, run_args=run_args, njobs=1)
            session.add_submission(cluster, cli=cli, njobsmax=njobsmax, njobs=1)
        # Do a submit call in case it wasn't done yet (if submit_now was called in the submit code this is a no-op)
        session.submit(cli, njobsmax=njobsmax)


def get_first_cluster(filename):
    """
    Returns the first cluster that would be submitted if the python job file
    would be submitted
    """
    return submit_python_job_file(filename, return_first_cluster=True)




class Session(object):
    """
    Over-arching object that stores settings global to the jobs,
    e.g. htcondor settings, run environment, pip installations, etc.
    """

    def __init__(self, name=None, rundir=None):
        self.submission_time = datetime.now()
        # Make a central run directory in which job files will appear;
        # prefer `rundir` argument over a potentially supplied `name` argument,
        # but always force the user to at least supply some hint as to what
        # the directory name should be.
        if rundir is None and name is None:
            raise ValueError('Initialize Session as Session(name) or Session(rundir="path/")')
        elif rundir:
            self.rundir = rundir
        else:
            self.rundir = osp.abspath(
                "qondor_{}_{}".format(name, self.submission_time.strftime(qondor.TIMESTAMP_FMT))
            )

        self.python_code = None
        self.transfer_files = []
        self.pips = []
        self.environment_variables = []
        # For htcondor settings, set some reasonable default settings
        self.htcondor_settings = OrderedDict(
            universe = "vanilla",
            output = "out_$(Cluster)_$(Process).txt",
            error = "err_$(Cluster)_$(Process).txt",
            log = "log_$(Cluster)_$(Process).txt",
            should_transfer_files = "YES",
            environment = {
                "QONDOR_BATCHMODE": "1",
                "CONDOR_CLUSTER_NUMBER": "$(Cluster)",
                "CONDOR_PROCESS_ID": "$(Process)",
                "CLUSTER_SUBMISSION_TIMESTAMP": self.submission_time.strftime(qondor.TIMESTAMP_FMT),
            },
        )
        if "USER" in os.environ: self.htcondor_settings["environment"]["USER"] = os.environ["USER"]
        self._x509userproxy_heuristic()


    def _x509userproxy_heuristic(self):
        """
        Tries to supply the grid proxy as an htcondor setting;
        Warns upon failure.
        """
        # Try to set some more things
        try:
            self.htcondor_settings["x509userproxy"] = os.environ["X509_USER_PROXY"]
        except KeyError:
            try:
                self.htcondor_settings["x509userproxy"] = qondor.utils.run_command(
                    ["voms-proxy-info", "-path"]
                )[0].strip()
                logger.info(
                    'Set x509userproxy to "%s" based on output from voms-proxy-info',
                    self.htcondor_settings["x509userproxy"],
                )
            except Exception:
                logger.warning(
                    "Could not find a x509userproxy to pass; manually "
                    "set the htcondor variable 'x509userproxy' if your "
                    "htcondor setup requires it."
                )



class Job(object):
    """
    Container for settings that differ per job
    """
    def __init__(self, scope=None, njobs=1):
        self.scope = {} if scope is None else scope
        self.njobs = njobs



def submit_pythonbindings(jobs, session=None, njobsmax=None):
    qondor.utils.check_proxy()
    import htcondor

    if njobsmax is None: njobsmax = 1e7
    n_jobs_summed = sum([job.njobs for job in jobs])
    n_jobs_total = min(n_jobs_summed, njobsmax)
    logger.info("Submitting all jobs; %s out of %s", n_jobs_total, n_jobs_summed)
    schedd = qondor.schedd.get_best_schedd()
    n_jobs_todo = n_jobs_total

    if session is None: session = Session('nameless')
    qondor.utils.create_directory(session.rundir)

    ads = []
    returnable = []
    with qondor.utils.switchdir(session.rundir):
        with qondor.schedd._transaction(schedd) as transaction:
            submit_object = htcondor.Submit()
            session_sub = compile_session_submission_dict(session)
            for job in jobs:
                sub = compile_submission_dict(session_sub, job)
                njobs = min(job.njobs, n_jobs_todo)
                n_jobs_todo -= njobs
                # Load the dict into the submit object
                for key in sub:
                    if key.lower() == 'environment':
                        submit_object[key] = qondor.schedd.format_env_htcondor(sub[key])
                    else:
                        submit_object[key] = sub[key]
                new_ads = []
                # Here the actual call to the htcondor python bindings to schedule this submission
                cluster_id = (
                    int(submit_object.queue(transaction, njobs, new_ads))
                    if not qondor.DRYMODE
                    else 0
                )
                logger.warning(
                    "Submitted %s jobs for i_cluster %s (%s) to htcondor cluster %s",
                    len(new_ads) if not qondor.DRYMODE else njobs,
                    sub["environment"]["QONDORICLUSTER"],
                    sub["environment"]["QONDORCLUSTERNAME"],
                    cluster_id,
                )
                ads.extend(new_ads)
                returnable.append(
                    (
                        sub,
                        cluster_id,
                        len(new_ads),
                        [ad["ProcId"] for ad in new_ads],
                    )
                )
                if n_jobs_todo == 0:
                    break

    logger.info(
        "Summary: Submitted %s jobs to cluster %s", n_jobs_total, cluster_id
    )
    return returnable


def compile_session_submission_dict(session):
    sub = session.htcondor_settings.copy()
    sub["transfer_input_files"] = ",".join(session.transfer_files)
    # TODO: sub["executable"] is probably session level, not cluster level?
    return sub

def compile_submission_dict(session_sub, job):
    sub = session_sub.copy()
    sub["environment"]["QONDORICLUSTER"] = str(cluster.i_cluster)
    



class Session(object):
    """
    Over-arching object that controls submission of a number of clusters
    """

    def __init__(self, name=None):
        self.submission_time = datetime.now()
        name = "qondor_" + name if name else "qondor"
        self.rundir = osp.abspath(
            "{}_{}".format(name, self.submission_time.strftime(qondor.TIMESTAMP_FMT))
        )
        self.transfer_files = []
        self.submittables = []
        self.htcondor_settings = get_default_sub(self.submission_time)

        self._submitted = []
        self._njobs_submitted = 0
        self._created_python_module_tarballs = {}
        self._fixed_cmsconnect_specific_settings = False


    def htcondor(self, key, value):
        """
        Shortcut to add htcondor settings
        """
        self.htcondor_settings[key] = value

    def fix_cmsconnect_specific_settings_once(self, cli):
        """
        Potentially process cmsconnect specific settings
        Changes self.htcondor_settings once
        """
        if self._fixed_cmsconnect_specific_settings:
            return
        self._fixed_cmsconnect_specific_settings = True
        blacklist = self.htcondor_settings.pop("cmsconnect_blacklist", None)
        whitelist = self.htcondor_settings.pop("cmsconnect_whitelist", None)
        if os.uname()[1] == "login.uscms.org" or os.uname()[1] == "login-el7.uscms.org":
            qondor.logger.warning("Detected CMS Connect; loading specific settings")
            qondor.cmsconnect.cmsconnect_settings(
                self.htcondor_settings,
                cli=cli,
                blacklist=blacklist,
                whitelist=whitelist,
            )

    def dump_seutils_cache(self):
        """
        Dumps the seutils cache to a tarball, to be included in the job.
        """
        proposed_tarball = osp.join(
            self.rundir, "seutils-cache-{}.tar.gz".format(self._i_seutils_tarball)
        )
        # If no new seutils calls were made, the tarball won't be remade
        with seutils.drymode_context(qondor.DRYMODE):
            actual_tarball = seutils.tarball_cache(
                proposed_tarball, only_if_updated=True
            )
        if actual_tarball == proposed_tarball:
            self._i_seutils_tarball += 1
        logger.info("Using seutils-cache tarball %s", actual_tarball)
        return actual_tarball

    def handle_python_package_tarballs(self, cluster):
        # Put in python package tarballs required for the code in the job
        for package, install_instruction in cluster.pips:
            # Packages with a specific version should always be installed from pypi
            for c in ["<", "=", ">"]:
                if c in package:
                    install_instruction = "pypi"
            # Determine whether package was installed editably
            if install_instruction == "auto" and qondor.utils.dist_is_editable(package):
                install_instruction = "editable"
            # If package was installed editably, tarball it up and include it
            if install_instruction == "editable":
                # Create the tarball if it wasn't already created
                if package not in self._created_python_module_tarballs:
                    self._created_python_module_tarballs[
                        package
                    ] = qondor.utils.tarball_python_module(package, outdir=self.rundir)
                # Add the tarball as input file for this cluster
                cluster.transfer_files[
                    "_packagetarball_{}".format(package)
                ] = self._created_python_module_tarballs[package]

    def add_submission(self, cluster, cli=True, njobs=1, njobsmax=None):
        if njobsmax:
            njobs = min(njobs, njobsmax - self._njobs_submitted)
        if njobsmax and njobs == 0:
            logger.debug(
                "Not adding submission for cluster %s - reached njobsmax %s",
                cluster.i_cluster,
                njobsmax,
            )
            return
        # These two lines here rather than in __init__, to allow self.rundir
        # to be overwritten in the submission code without breaking things
        self.rundir = osp.abspath(self.rundir)
        self.htcondor_settings["+QondorRundir"] = '"' + self.rundir + '"'

        self._njobs_submitted += njobs
        qondor.utils.create_directory(self.rundir)
        cluster.rundir = self.rundir
        # Possibly create tarballs out of required python packages
        self.handle_python_package_tarballs(cluster)
        # Possibly create tarball for the seutils cache and include it in the job
        if seutils.USE_CACHE:
            cluster.transfer_files["seutils-cache"] = self.dump_seutils_cache()
        # Dump cluster contents to file and build scope
        cluster.runcode_to_file()
        cluster.sh_entrypoint_to_file()
        cluster.scope_to_file()
        # Compile the submission dict
        # Base off of global settings only for python-binding mode
        # For the condor_submit cli method, it's better to just write the global keys once at the top of the file
        # Little unsafe, if you modiy these 'global' settings in one job they propegate to all the next ones,
        # but otherwise submission of many jobs becomes very slow
        sub = {
            "output": "out_{}_$(Cluster)_$(Process).txt".format(cluster.name),
            "error": "err_{}_$(Cluster)_$(Process).txt".format(cluster.name),
            "log": "log_{}_$(Cluster)_$(Process).txt".format(cluster.name),
        }
        sub["executable"] = osp.abspath(cluster.sh_entrypoint_filename)
        sub["environment"] = {}
        sub["environment"]["QONDORICLUSTER"] = str(cluster.i_cluster)
        sub["environment"]["QONDORCLUSTERNAME"] = str(cluster.name)
        sub["environment"].update(cluster.env)
        # Overwrite htcondor keys defined in the preprocessing
        sub.update(cluster.htcondor)
        # Flatten files into a string, excluding files on storage elements
        transfer_files = self.transfer_files + [
            f for f in cluster.transfer_files.values() if not seutils.has_protocol(f)
        ]
        if len(transfer_files) > 0:
            sub["transfer_input_files"] = ",".join(transfer_files)
        sub = update_sub(sub, cluster.htcondor)
        # Plugin the global and cmsconnect settings in now
        self.fix_cmsconnect_specific_settings_once(cli)
        sub = update_sub(self.htcondor_settings, sub)
        logger.info(
            "Prepared submission dict for cluster %s:\n%s",
            cluster.i_cluster,
            pprint.pformat(sub),
        )
        # Add it to the submittables
        self.submittables.append((sub, njobs))

    def submit_pythonbindings(self, njobsmax=None):
        qondor.utils.check_proxy()
        if not self.submittables:
            return
        import htcondor

        if njobsmax is None:
            njobsmax = 1e7
        n_jobs_summed = sum([njobs for _, njobs in self.submittables])
        n_jobs_total = min(n_jobs_summed, njobsmax)
        logger.info("Submitting all jobs; %s out of %s", n_jobs_total, n_jobs_summed)
        schedd = qondor.schedd.get_best_schedd()
        n_jobs_todo = n_jobs_total
        ads = []
        with qondor.utils.switchdir(self.rundir):
            with qondor.schedd._transaction(schedd) as transaction:
                submit_object = htcondor.Submit()
                for sub_orig, njobs in self.submittables:
                    sub = (
                        sub_orig.copy()
                    )  # Keep original dict intact? Global settings already contained
                    sub["environment"] = qondor.schedd.format_env_htcondor(
                        sub["environment"]
                    )
                    njobs = min(njobs, n_jobs_todo)
                    n_jobs_todo -= njobs
                    # Load the dict into the submit object
                    for key in sub:
                        submit_object[key] = sub[key]
                    new_ads = []
                    cluster_id = (
                        int(submit_object.queue(transaction, njobs, new_ads))
                        if not qondor.DRYMODE
                        else 0
                    )
                    logger.warning(
                        "Submitted %s jobs for i_cluster %s (%s) to htcondor cluster %s",
                        len(new_ads) if not qondor.DRYMODE else njobs,
                        sub_orig["environment"]["QONDORICLUSTER"],
                        sub_orig["environment"]["QONDORCLUSTERNAME"],
                        cluster_id,
                    )
                    ads.extend(new_ads)
                    self._submitted.append(
                        (
                            sub_orig,
                            cluster_id,
                            len(new_ads),
                            [ad["ProcId"] for ad in new_ads],
                        )
                    )
                    if n_jobs_todo == 0:
                        break
        logger.info(
            "Summary: Submitted %s jobs to cluster %s", n_jobs_total, cluster_id
        )

    def submit_cli(self, njobsmax=None):
        qondor.utils.check_proxy()
        if not self.submittables:
            return
        if njobsmax is None:
            njobsmax = 1e7
        n_jobs_summed = sum([njobs for _, njobs in self.submittables])
        n_jobs_total = min(n_jobs_summed, njobsmax)
        logger.info("Submitting all jobs; %s out of %s", n_jobs_total, n_jobs_summed)
        n_jobs_todo = n_jobs_total
        # Compile the .jdl file

        def get_cluster_nr(sub):
            return sub["environment"]["QONDORICLUSTER"]

        jdl_file = osp.join(
            self.rundir,
            "qondor_{}-{}.jdl".format(
                get_cluster_nr(self.submittables[0][0]),
                get_cluster_nr(self.submittables[-1][0]),
            ),
        )
        jdl_contents = []
        # Write the settings per job
        for sub, njobs in self.submittables:
            njobs = min(njobs, n_jobs_todo)
            n_jobs_todo -= njobs
            jdl_contents.append(
                "# Cluster {}".format(sub["environment"]["QONDORICLUSTER"])
            )
            # Dump the submission to a jdl file
            for key in sub.keys():
                val = sub[key]
                if key.lower() == "environment":
                    val = qondor.schedd.format_env_htcondor(val)
                jdl_contents.append("{} = {}".format(key, val))
            jdl_contents.append("queue {}\n".format(njobs))
            if n_jobs_todo == 0:
                break
        # Dump to file
        jdl_contents = "\n".join(jdl_contents)
        with qondor.utils.openfile(jdl_file, "w") as jdl:
            jdl.write(jdl_contents)
        logger.info("Compiled %s:\n%s", jdl_file, jdl_contents)
        # Run the actual submit command
        with qondor.utils.switchdir(self.rundir):
            output = qondor.utils.run_command(["condor_submit", osp.basename(jdl_file)])
        # Get some info from the condor_submit command, if not dry mode
        if qondor.DRYMODE:
            logger.warning("Summary: Submitted %s jobs to cluster 0", n_jobs_total)
        else:
            matches = re.findall(
                r"(\d+) job\(s\) submitted to cluster (\d+)", "\n".join(output)
            )
            if not len(matches):
                logger.error(
                    "condor_submit exited ok but could not determine where and how many jobs were submitted"
                )
            # Unfortunately we have to match the number of submitted jobs back to the submission dicts
            # with a while-loop and careful counting
            submittables = iter(self.submittables)
            for njobs_submitted, cluster_id in matches:
                njobs_submitted = int(njobs_submitted)
                logger.warning(
                    "Submitted %s jobs to cluster_id %s", njobs_submitted, cluster_id
                )
                njobs_assigned = 0
                while njobs_assigned < njobs_submitted:
                    sub, njobs_thissub = next(submittables)  # Get the next sub
                    proc_ids = list(
                        range(njobs_assigned, njobs_assigned + njobs_thissub)
                    )  # Build the list of proc_ids
                    njobs_assigned += njobs_thissub
                    self._submitted.append((sub, cluster_id, njobs_assigned, proc_ids))

    def submit(self, cli, *args, **kwargs):
        """
        Wrapper that just picks the specific submit method
        """
        if len(self.submittables) == 0:
            logger.warning("No jobs to be submitted")
            return
        qondor.utils.create_directory(self.rundir)
        # Run the submission code
        self.submit_cli(*args, **kwargs) if cli else self.submit_pythonbindings(
            *args, **kwargs
        )
        if not qondor.DRYMODE:
            # Dump some submission information to a .json file for possible resubmission later
            njobsmax = kwargs.get("njobsmax", None)
            submission_timestamp = qondor.get_submission_timestamp()
            submission = {
                "submission_timestamp": submission_timestamp,
                # 'submittables' : self.submittables,
                "njobsmax": njobsmax,
                "cli": cli,
                "submitted": self._submitted,
            }
            submission_jsonfile = osp.join(
                self.rundir, "submission_{}.json".format(submission_timestamp)
            )
            with open(submission_jsonfile, "w") as f:
                json.dump(submission, f)
            # Also copy this file to submission_latest.json for easier retrieval
            shutil.copyfile(
                submission_jsonfile, osp.join(self.rundir, "submission_latest.json")
            )
        # Clear the submittables
        self.submittables = []
        self._submitted = []
