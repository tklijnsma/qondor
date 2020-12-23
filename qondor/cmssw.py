import glob
import logging
import os
import os.path as osp
import shutil
import time

import seutils

import qondor

logger = logging.getLogger("qondor")


class CMSSW(object):
    """docstring for CMSSW"""

    default_local_rundir = "/tmp/qondor"
    default_remote_rundir = "."

    @staticmethod
    def extract_tarball(tarball, outdir=None):
        if outdir is None:
            outdir = (
                CMSSW.default_remote_rundir
                if qondor.BATCHMODE
                else CMSSW.default_local_rundir
            )
            logger.warning("Will extract %s to default: %s", tarball, outdir)
            try:
                qondor.utils.create_directory(outdir)
            except Exception:
                logger.warning("Failed to create %s; attempting here (.)", outdir)
                outdir = "."
                qondor.utils.create_directory(outdir)
        else:
            qondor.utils.create_directory(outdir)
        cmssw_dir = qondor.utils.extract_tarball_cmssw(tarball, outdir=outdir)
        cmssw_src = osp.abspath(osp.join(cmssw_dir, "src"))
        return cmssw_src

    @classmethod
    def from_tarball(cls, tarball, scram_arch=None, outdir=None):
        if seutils.has_protocol(tarball):
            logger.info("Tarball %s seems to be located remote; copying", tarball)
            dst = osp.abspath(osp.basename(tarball))
            if osp.isfile(dst):
                logger.warning("Using pre-existing tarball %s", dst)
            else:
                seutils.cp(tarball, dst)
            tarball = dst
        cmssw_src = cls.extract_tarball(tarball, outdir)
        # See if the tarball was already compiled with some scram_arch, if so use it
        if scram_arch is None:
            compiled_arches = glob.glob(osp.join(cmssw_src, "../bin/slc*"))
            if compiled_arches:
                scram_arch = osp.basename(compiled_arches[0])
                logger.warning(
                    "Detected tarball was compiled with arch %s, using it", scram_arch
                )
        return cls(cmssw_src, scram_arch)

    @classmethod
    def for_release(cls, release, scram_arch=None, outdir=".", renew=True):
        logger.info("Setting up CMSSW %s, arch %s in %s", release, scram_arch, outdir)
        # Make sure there is a directory in which to create the new CMSSW
        qondor.utils.create_directory(outdir)
        with qondor.utils.switchdir(outdir):
            # Check if there is an existing CMSSW
            existing = glob.glob("*{0}*".format(release))
            if existing:
                existing = existing[0]
                if renew:
                    logger.warning(
                        "Detected existing CMSSW %s; deleting it", osp.abspath(existing)
                    )
                    shutil.rmtree(existing)
                else:
                    logger.warning(
                        "Detected existing CMSSW %s; returning existing instead",
                        osp.abspath(existing),
                    )
                    return cls(osp.join(existing, "src"), scram_arch)
            # Make fresh release
            qondor.utils.run_multiple_commands(
                [
                    "shopt -s expand_aliases",
                    "source /cvmfs/cms.cern.ch/cmsset_default.sh",
                    "cmsrel {0}".format(release),
                ],
                env=qondor.utils.get_clean_env(),
            )
            # Get the src of the newly setup CMSSW
            cmssw_src = osp.abspath(glob.glob("*{0}*/src".format(release))[0])
        cmssw = cls(cmssw_src, scram_arch)
        # A freshly setup CMSSW won't need to be renamed
        cmssw._is_renamed = True
        return cmssw

    def __init__(self, cmssw_path, scram_arch=None):
        super(CMSSW, self).__init__()
        cmssw_path = cmssw_path.rstrip("/")
        if cmssw_path.endswith("src"):
            cmssw_path = osp.dirname(cmssw_path)
        self.cmssw_path = osp.abspath(cmssw_path)
        self.cmssw_src = osp.join(self.cmssw_path, "src")
        if scram_arch is None:
            compiled_arches = glob.glob(osp.join(self.cmssw_src, "../bin/slc*"))
            if compiled_arches:
                self.scram_arch = osp.basename(compiled_arches[0])
                logger.warning(
                    "Detected CMSSW was compiled with arch %s, using it",
                    self.scram_arch,
                )
            else:
                try:
                    logger.warning("Attempting to find scram_arch...")
                    print(osp.basename(osp.normpath(osp.join(self.cmssw_src, ".."))))
                    self.scram_arch = qondor.get_arch(
                        osp.basename(osp.normpath(osp.join(self.cmssw_src, "..")))
                    )
                except RuntimeError:
                    logger.warning(
                        "Taking SCRAM_ARCH from environment; may mismatch with"
                        " CMSSW version of %s",
                        self.cmssw_src,
                    )
                    self.scram_arch = os.environ["SCRAM_ARCH"]
        else:
            self.scram_arch = scram_arch
        self._is_renamed = False
        self._is_externallinks = False

    def rename_project(self):
        if self._is_renamed:
            return
        self._is_renamed = True
        logger.info("Renaming project %s", self.cmssw_src)
        self.run_commands_nocmsenv(["scram b ProjectRename"])

    def scramb_externallinks(self):
        if self._is_externallinks:
            return
        self._is_externallinks = True
        logger.info("Doing scram b ExternalLinks %s", self.cmssw_src)
        self.run_commands_nocmsenv(["scram b ExternalLinks"])

    def cmsrun(self, cfg_file, **kwargs):
        """
        Specifically runs a cmsRun command. Expects a python file, and other
        keyword arguments are parsed as "keyword=value".
        The key "inputFiles" is treated differently: A list is expected,
        and every item is added as "inputFiles=item".
        The key "outputFile" is also treated differently: The passed string
        is formatted with keys "proc_id", "cluster_id", and "datestr" for
        convenience.
        """
        cmd = ["cmsRun", cfg_file]
        if "inputFiles" in kwargs:
            inputFiles = kwargs.pop("inputFiles")
            if qondor.utils.is_string(inputFiles):
                inputFiles = [inputFiles]
            for inputFile in inputFiles:
                cmd.append("inputFiles=" + inputFile)
        if "outputFile" in kwargs:
            outputFile = kwargs.pop("outputFile")
            cmd.append(
                "outputFile="
                + outputFile.format(
                    proc_id=qondor.get_proc_id(),
                    cluster_id=qondor.get_cluster_id(),
                    datestr=time.strftime("%b%d"),
                )
            )
        cmd.extend(["{0}={1}".format(k, v) for k, v in kwargs.items()])
        self.run_command(cmd)

    def run_command(self, cmd):
        """
        Mostly legacy; use run_commands instead
        """
        return self.run_commands([cmd])

    def run_commands(self, cmds):
        """
        Main/Public method: Much like run_commands_nocmsenv, but includes cmsenv
        This is intended to be called with cmsRun/cmsDriver.py commands
        """
        return self.run_commands_nocmsenv(["cmsenv"] + cmds)

    def run_commands_nocmsenv(self, cmds):
        """
        Preprends the basic CMSSW environment setup, and executes a set of
        commands in a clean environment
        """
        self.rename_project()
        with qondor.utils.switchdir(self.cmssw_src):
            return qondor.utils.run_multiple_commands(
                [
                    "shopt -s expand_aliases",
                    "source /cvmfs/cms.cern.ch/cmsset_default.sh",
                    "export SCRAM_ARCH={0}".format(self.scram_arch),
                ]
                + cmds,
                env=qondor.utils.get_clean_env(),
            )

    def make_tarball(
        self,
        outdir=".",
        tag=None,
        renew=True,
        exclude=None,
        exclude_vcs=True,
        exclude_caches_all=True,
        include=None,
    ):
        """
        Makes a tarball out of the CMSSW distribution of this class
        """
        cmssw_path = osp.abspath(osp.join(self.cmssw_src, ".."))
        # Determine location of the output tarball
        dst = osp.join(
            osp.abspath(outdir),
            (
                osp.basename(cmssw_path).strip("/")
                + ("" if tag is None else "_" + tag)
                + ".tar.gz"
            ),
        )
        if osp.isfile(dst):
            if renew:
                logger.warning("Removing existsing %s", dst)
                os.remove(dst)
            else:
                raise OSError("{0} already exists".format(dst))
        logger.warning("Tarballing {0} ==> {1}".format(cmssw_path, dst))
        if exclude is None:
            exclude = []
        exclude.append("tmp")
        with qondor.utils.switchdir(osp.dirname(cmssw_path)):
            cmd = [
                "tar",
                "-zcvf",
                dst,
            ]
            if include:
                if qondor.utils.is_string(include):
                    include = [include]
                cmd.extend(include)
            if exclude_vcs:
                cmd.append(
                    "--exclude-vcs",
                )
            if exclude_caches_all:
                cmd.append(
                    "--exclude-caches-all",
                )
            cmd.extend(["--exclude={0}".format(e) for e in exclude])
            cmd.append(osp.basename(cmssw_path))
            qondor.utils.run_command(cmd)
        return dst

    def pip(self, *args):
        """
        Installs a pypi package in this CMSSW environment using the scram-pip script.
        Any arguments are used for the pip command
        """
        scrampip = osp.join(qondor.INCLUDE_DIR, "scram-pip")
        cmd = [scrampip] + list(args)
        self.run_commands([cmd, "cmsenv"])

    def make_chunk_rootfile(self, src, first, last, tree="auto", dst=None):
        """
        Round-about way of calling seutils.make_chunk_rootfile in this CMSSW environment.
        First installs seutils in this CMSSW using scram pip, then calls seu-takechunkroot
        to perform the actual splitting.
        This is a rather hacky solution.
        """
        self.pip("seutils", '-p="--ignore-installed"')
        cmd = "seu-takechunkroot {src} -f {first} -l {last} -t {tree}".format(
            src=src, first=first, last=last, tree=tree
        )
        if dst:
            cmd += " -d " + dst
        self.run_command(cmd)

    def get_chunk(self, chunk, dst, tree="auto"):
        """
        Roundabout way of calling seutils.hadd_chunk_entries in this CMSSW environment.
        """
        return seutils.hadd_chunk_entries(
            chunk, dst, tree=tree, file_split_fn=self.make_chunk_rootfile
        )
