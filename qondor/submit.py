# -*- coding: utf-8 -*-
import json
import logging
import os
import os.path as osp
import pprint
import re
import shutil
from datetime import datetime

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


# _____________________________________________________________________
# Submission interface:
# Deals with building submission dicts and submitting them to htcondor

# Run environments: Currently just two options (sl6 and sl7)
RUN_ENVS = {
    "sl7-py27": [
        'export pipdir="/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-centos7-gcc7-opt"',
        'export SCRAM_ARCH="slc7_amd64_gcc820"',
        "source /cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh",
        "source /cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-centos7-gcc7-opt/ROOT-env.sh",
        'export PATH="${pipdir}/bin:${PATH}"',
        'export PYTHONPATH="${pipdir}/lib/python2.7/site-packages/:${PYTHONPATH}"',
        'pip(){ ${pipdir}/bin/pip "$@"; }  # To avoid any local pip installations',
    ],
    "sl6-py27": [
        'export pipdir="/cvmfs/sft.cern.ch/lcg/releases/pip/19.0.3-06476/x86_64-slc6-gcc7-opt"',
        'export SCRAM_ARCH="slc6_amd64_gcc700"',
        "source /cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-slc6-gcc7-opt/setup.sh",
        "source /cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-slc6-gcc7-opt/ROOT-env.sh",
        'export PATH="${pipdir}/bin:${PATH}"',
        'export PYTHONPATH="${pipdir}/lib/python2.7/site-packages/:${PYTHONPATH}"',
        'pip(){ ${pipdir}/bin/pip "$@"; }  # To avoid any local pip installations',
    ],
    "centos7-py36": [
        "source /cvmfs/sft.cern.ch/lcg/views/LCG_96python3/x86_64-centos7-gcc8-opt/setup.sh",
    ],
}


def get_default_sub(submission_time=None):
    """
    Returns the default submission dict (the equivalent of a .jdl file)
    to be used by the submitter.
    """
    if submission_time is None:
        submission_time = datetime.now()
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
            "CLUSTER_SUBMISSION_TIMESTAMP": submission_time.strftime(
                qondor.TIMESTAMP_FMT
            ),
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


def update_sub(sub, other):
    """
    Merges a submission dict, attempting not to overwrite some keys but rather append them
    """
    # First copy all
    r = dict(sub, **other)
    # Merge some things more carefully:
    # Environment
    r["environment"] = dict(sub.get("environment", {}), **other.get("environment", {}))
    # Input files
    files = []
    if "transfer_input_files" in sub:
        files += sub["transfer_input_files"].split(",")
    if "transfer_input_files" in other:
        files += other["transfer_input_files"].split(",")
    if files:
        r["transfer_input_files"] = ",".join(files)
    return r


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
        self._created_python_module_tarballs = {}
        self._i_seutils_tarball = 0
        self.submittables = []
        self._njobs_submitted = 0
        self.htcondor_settings = get_default_sub(self.submission_time)
        self.htcondor_settings["+QondorRundir"] = '"' + self.rundir + '"'
        self._fixed_cmsconnect_specific_settings = False
        # Storage for submitted jobs
        self.submitted = []

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
            cmsconnect_settings(
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
                    self.submitted.append(
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
                    self.submitted.append((sub, cluster_id, njobs_assigned, proc_ids))

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
                "submitted": self.submitted,
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
        self.submitted = []


class Cluster(object):

    ICLUSTER = 0
    NAMES = set()

    def __init__(
        self,
        runcode,
        scope=None,
        env=None,
        pips=None,
        htcondor=None,
        run_args=None,
        run_env="sl7-py27",
        rundir=".",
        session=None,
        transfer_files=None,
        **kwargs
    ):
        self.i_cluster = self.__class__.ICLUSTER
        self.__class__.ICLUSTER += 1
        self.rundir = rundir
        self.runcode = runcode
        self.env = {} if env is None else env
        self.scope = {} if scope is None else scope
        self.htcondor = {} if htcondor is None else htcondor
        self.run_args = run_args
        if qondor.utils.is_string(run_env):
            run_env = RUN_ENVS[run_env]
        self.run_env = run_env
        self.transfer_files = {}
        if transfer_files:
            for f in transfer_files:
                self._add_transfer_file_str_or_tuple(f)
        self.session = session
        # Try to use a more human-readable name if it's given
        if "name" not in kwargs:
            self.name = "cluster{}".format(self.i_cluster)
        else:
            # If the name was already used before, append a number to it until it's unique
            name = kwargs["name"]
            if name in self.NAMES:
                name += "{}"
                i_attempt = 0
                while name.format(i_attempt) in self.NAMES:
                    i_attempt += 1
                name = name.format(i_attempt)
            self.__class__.NAMES.add(name)
            self.name = name
        logger.info("Using name %s", self.name)
        # Base filenames needed for the job
        self.runcode_filename = "{}.py".format(self.name)
        self.sh_entrypoint_filename = "{}.sh".format(self.name)
        self.scope_filename = "{}.json".format(self.name)
        # Process pip packages
        self.pips = []
        pips = [] if pips is None else pips

        def check_if_in_pips(package):
            """Checks if a package is already in the list"""
            package, _ = qondor.utils.pip_split_version(package)
            for pip in pips:
                if not qondor.utils.is_string(pip):
                    pip = pip[0]
                pip, _ = qondor.utils.pip_split_version(pip)
                if pip == package:
                    return True
            return False

        if not check_if_in_pips("qondor"):
            pips.append(("qondor", "auto"))
        if not check_if_in_pips("seutils"):
            pips.append(("seutils", "auto"))
        for pip in pips:
            if qondor.utils.is_string(pip):
                self.pips.append((pip, "auto"))
            else:
                self.pips.append((pip[0], pip[1]))
        # Add addtional keywords to the scope
        self.scope.update(kwargs)

    def _add_transfer_file_str_or_tuple(self, t):
        if qondor.utils.is_string(t):
            self.add_transfer_file(t)
        else:
            key, value = t
            self.add_transfer_file(value, key=key)

    def add_transfer_file(self, filename, key=None):
        """
        Adds a file to the self.transfer_files dict in order for it to be transferred
        """
        filename = osp.abspath(osp.expanduser(filename))
        if key is None:
            key = osp.basename(filename)
            # Make a unique key
            if key in self.transfer_files:
                key += "_{0}"
                for i in range(100):
                    if not key.format(i) in self.transfer_files:
                        key = key.format(i)
                        break
                else:
                    raise Exception("Could not make a unique key")
        self.transfer_files[key] = filename

    def runcode_to_file(self):
        self.runcode_filename = osp.join(self.rundir, self.runcode_filename)
        if osp.isfile(self.runcode_filename):
            raise OSError("{} exists".format(self.runcode_filename))
        self.transfer_files["runcode"] = self.runcode_filename
        logger.info(
            "Dumping python code for cluster %s to %s",
            self.i_cluster,
            self.runcode_filename,
        )
        if not (qondor.DRYMODE):
            with open(self.runcode_filename, "w") as f:
                f.write(self.runcode)

    def sh_entrypoint_to_file(self):
        self.sh_entrypoint_filename = osp.join(self.rundir, self.sh_entrypoint_filename)
        if osp.isfile(self.sh_entrypoint_filename):
            raise OSError("{} exists".format(self.sh_entrypoint_filename))
        sh = self.parse_sh_entrypoint()
        self.transfer_files["sh_entrypoint"] = self.sh_entrypoint_filename
        logger.info(
            "Dumping .sh entrypoint for cluster %s to %s",
            self.i_cluster,
            self.sh_entrypoint_filename,
        )
        if not (qondor.DRYMODE):
            with open(self.sh_entrypoint_filename, "w") as f:
                f.write(sh)

    def scope_to_file(self):
        self.scope_filename = osp.join(self.rundir, self.scope_filename)
        if osp.isfile(self.scope_filename):
            raise OSError("{} exists".format(self.scope_filename))
        self.transfer_files["scope"] = self.scope_filename
        self.env["QONDORSCOPEFILE"] = osp.basename(self.scope_filename)
        # Some last-minute additions before sending to a file
        self.scope["transfer_files"] = self.transfer_files
        self.scope["pips"] = self.pips
        logger.info(
            "Dumping the following scope for cluster %s to %s:\n%s",
            self.i_cluster,
            self.scope_filename,
            pprint.pformat(self.scope),
        )
        if not (qondor.DRYMODE):
            with open(self.scope_filename, "w") as f:
                json.dump(self.scope, f)

    def parse_sh_entrypoint(self):
        # Basic setup: Divert almost all output to the stderr, and setup cms scripts
        sh = [
            "#!/bin/bash",
            "set -e",
            'echo "hostname: $(hostname)"',
            'echo "date:     $(date)"',
            'echo "pwd:      $(pwd)"',
            'echo "ls -al:"',
            "ls -al",
            'echo "Redirecting all output to stderr from here on out"',
            "exec 1>&2",
            "",
            "export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/",
            "source /cvmfs/cms.cern.ch/cmsset_default.sh",
            "env > bare_env.txt",  # Save the environment before doing any other environment setup
            "",
        ]
        # Set the runtime environment (typically sourcing scripts to get the right python/gcc/ROOT/etc.)
        sh += self.run_env + [""]
        # Set up a directory to install python packages in, and put on the path
        # Currently requires $pipdir to be defined... might want to figure out something more clever
        sh += [
            "set -uxoE pipefail",
            'echo "Setting up custom pip install dir"',
            'HOME="$(pwd)"',
            'export pip_install_dir="$(pwd)/install"',
            'mkdir -p "${pip_install_dir}/bin"',
            'mkdir -p "${pip_install_dir}/lib/python2.7/site-packages"',
            'export PATH="${pip_install_dir}/bin:${PATH}"',
            "export PYTHONVERSION=$(python -c \"import sys; print('{}.{}'.format(sys.version_info.major, sys.version_info.minor))\")",
            'export PYTHONPATH="${pip_install_dir}/lib/python${PYTHONVERSION}/site-packages:${PYTHONPATH}"',
            "",
            "pip -V",
            "which pip",
            "",
        ]
        # `pip install` the required pip packages for the job
        for package, install_instruction in self.pips:
            package_name, version_stuff = qondor.utils.pip_split_version(
                package.rstrip("/")
            )
            package_name = package_name.replace(".", "-")
            package = package_name + version_stuff
            if version_stuff:
                install_instruction = (
                    "pypi"  # Force download from pypi for a specific version
                )
            if install_instruction == "auto":
                install_instruction = (
                    "editable" if qondor.utils.dist_is_editable(package) else "pypi"
                )
            if install_instruction == "editable":
                # Editable install: Manually give tarball, extract, and install
                sh.extend(
                    [
                        "mkdir {0}".format(package),
                        "tar xf {0}.tar -C {0}".format(package),
                        'pip install --install-option="--prefix=${{pip_install_dir}}" --no-cache-dir -e {0}/'.format(
                            package
                        ),
                    ]
                )
            else:
                # Non-editable install from pypi
                sh.append(
                    'pip install --install-option="--prefix=${{pip_install_dir}}" --no-cache-dir {0}'.format(
                        package
                    )
                )
        # Make the actual python call to run the required job code
        # Also echo the exitcode of the python command to a file, to easily check whether jobs succeeded
        # First compile the command - which might take some command line arguments
        python_cmd = "python {0}".format(osp.basename(self.runcode_filename))
        if self.run_args:
            # Add any arguments for the python script to this line
            try:  # py3
                from shlex import quote
            except ImportError:  # py2
                from pipes import quote
            python_cmd += " " + " ".join([quote(s) for s in self.run_args])
        sh += [
            python_cmd,
            'echo "$?" > exitcode_${QONDORCLUSTERNAME}_${CONDOR_CLUSTER_NUMBER}_${CONDOR_PROCESS_ID}.txt',  # Store the python exit code in a file
            "",
        ]
        sh = "\n".join(sh)
        logger.info(
            "Parsed the following .sh entrypoint for cluster %s:\n%s",
            self.i_cluster,
            sh,
        )
        return sh

    def submit(self, *args, **kwargs):
        """
        Like Session.submit(cluster, *args, **kwargs), but then initiated from the cluster object.
        If session is not provided as a keyword argument, a clean session is started.
        """
        session = kwargs.pop(
            "session", Session()
        )  # Make new session if not set by keyword
        session.submit(self, *args, **kwargs)


def cmsconnect_get_all_sites():
    """
    Reads the central config for cmsconnect to determine the list of all available sites
    """
    try:
        from configparser import RawConfigParser  # python 3
    except ImportError:
        import ConfigParser  # python 2

        RawConfigParser = ConfigParser.RawConfigParser
    cfg = RawConfigParser()
    cfg.read("/etc/ciconnect/config.ini")
    sites = cfg.get("submit", "DefaultSites").split(",")
    all_sites = set(sites)
    return all_sites


def cmsconnect_settings(sub, blacklist=None, whitelist=None, cli=False):
    """
    Adds special cmsconnect settings to submission dict in order to submit
    on cmsconnect via the python bindings, or in case of submitting by the
    command line, to set the DESIRED_Sites key.
    Modifies the dict in place.
    """
    all_sites = cmsconnect_get_all_sites()

    # Check whether the user whitelisted or blacklisted some sites
    desired_sites = None
    if blacklist or whitelist:
        import fnmatch

        blacklisted = []
        whitelisted = []
        # Build the blacklist
        if blacklist:
            for blacksite_pattern in blacklist:
                for site in all_sites:
                    if fnmatch.fnmatch(site, blacksite_pattern):
                        blacklisted.append(site)
        # Build the whitelist
        if whitelist:
            for whitesite_pattern in whitelist:
                for site in all_sites:
                    if fnmatch.fnmatch(site, whitesite_pattern):
                        whitelisted.append(site)
        # Convert to list and sort
        blacklisted = list(set(blacklisted))
        blacklisted.sort()
        whitelisted = list(set(whitelisted))
        whitelisted.sort()
        logger.info("Blacklisting: %s", ",".join(blacklisted))
        logger.info("Whitelisting: %s", ",".join(whitelisted))
        desired_sites = list(
            (set(all_sites) - set(blacklisted)).union(set(whitelisted))
        )
        desired_sites.sort()

    # Add a plus only if submitting via .jdl file
    def addplus(key):
        return "+" + key if cli else key

    if desired_sites:
        logger.info("Submitting to desired sites: %s", ",".join(desired_sites))
        sub[addplus("DESIRED_Sites")] = '"' + ",".join(desired_sites) + '"'
    else:
        logger.info("Submitting to all sites: %s", ",".join(all_sites))
    if not cli:
        sub[addplus("ConnectWrapper")] = '"2.0"'
        sub[addplus("CMSGroups")] = '"/cms,T3_US_FNALLPC"'
        sub[addplus("MaxWallTimeMins")] = "500"
        sub[addplus("ProjectName")] = '"cms.org.fnal"'
        sub[addplus("SubmitFile")] = '"irrelevant.jdl"'
        sub[addplus("AccountingGroup")] = '"analysis.{0}"'.format(os.environ["USER"])
        logger.warning(
            "FIXME: CMS Connect settings currently hard-coded for a FNAL user"
        )
