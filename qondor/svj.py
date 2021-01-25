"""
Module specific for the (boosted) svj analysis
"""

import logging
import os
import os.path as osp
import pprint
import subprocess

import seutils

import qondor

logger = logging.getLogger("qondor")

MG_TARBALL_PATHS = [
    "root://cmseos.fnal.gov//store/user/lpcsusyhad/SVJ2017/boosted/mg_tarballs",
    "root://cmseos.fnal.gov//store/user/lpcsusyhad/SVJ2017/boosted/mg_tarballs_2021",
]


class Physics(dict):
    """Holds the desired physics"""

    def __init__(self, *args, **kwargs):
        # Set some default values
        self["mz"] = 150.0
        self["rinv"] = 0.3
        self["boost"] = 0.0
        self["mdark"] = 20.0
        self["alpha"] = "peak"
        self["max_events"] = None
        super(Physics, self).__init__(*args, **kwargs)

    def boost_str(self):
        """Format string if boost > 0"""
        return "_HT{0:.0f}".format(self["boost"]) if self["boost"] > 0.0 else ""

    def max_events_str(self):
        return (
            ""
            if self.get("max_events", None) is None
            else "_n-{0}".format(self["max_events"])
        )

    def __repr__(self):
        return pprint.pformat(dict(self))


def svj_filename(step, physics):
    """
    Returns a basename for a root file that is input or output of a given step for given physics
    """
    rootfile = (
        "{step}_s-channel_mMed-{mz:.0f}_mDark-{mdark:.0f}_rinv-{rinv}_"
        "alpha-{alpha}{boost_str}_13TeV-madgraphMLM-pythia8{max_events_str}.root".format(
            step=step,
            boost_str=physics.boost_str(),
            max_events_str=physics.max_events_str(),
            **physics
        )
    )
    if physics.get("part", None):
        rootfile = rootfile.replace(".root", "_part-{}.root".format(physics["part"]))
    return rootfile


def madgraph_tarball_filename(physics):
    """Returns the basename of a MadGraph tarball for the given physics"""
    # Madgraph tarball filenames do not have a part number associated with them; overwrite it
    return svj_filename("step0_GRIDPACK", Physics(physics, part=None)).replace(
        ".root", ".tar.xz"
    )


def download_madgraph_tarball(physics, dst=None):
    """Downloads tarball from the storage element"""
    dst = osp.join(
        os.getcwd() if dst is None else dst, madgraph_tarball_filename(physics)
    )
    if osp.isfile(dst):
        logger.info("File %s already exists", dst)
    else:
        for mg_tarball_path in MG_TARBALL_PATHS:
            # Tarballs on SE will not have the boost tag and have postfix "_n-1"
            src = osp.join(
                mg_tarball_path,
                madgraph_tarball_filename(
                    Physics(physics, boost=0.0, max_events=1, part=None)
                ),
            )
            if seutils.isfile(src):
                logger.info("Downloading %s --> %s", src, dst)
                seutils.cp(src, dst)
                break
        else:
            raise Exception(
                "Cannot download tarball {0} for Physics {1}".format(
                    madgraph_tarball_filename(physics), physics
                )
            )


def step_cmd(inpre, outpre, physics):
    cmd = (
        "cmsRun runSVJ.py"
        " year={year}"
        " madgraph=1"
        " channel=s"
        " outpre={outpre}"
        " config={outpre}"
        " part={part}"
        " mMediator={mz:.0f}"
        " mDark={mdark:.0f}"
        " rinv={rinv}"
        " inpre={inpre}".format(inpre=inpre, outpre=outpre, **physics)
    )
    if "mingenjetpt" in physics:
        cmd += " mingenjetpt={0:.1f}".format(physics["mingenjetpt"])
    if "boost" in physics:
        cmd += " boost={0:.0f}".format(physics["boost"])
    if "max_events" in physics:
        cmd += " maxEvents={0}".format(physics["max_events"])
    return cmd


def gridpack_cmd(physics, nogridpack=False):
    cmd = (
        "python runMG.py"
        " year={year}"
        " madgraph=1"
        " channel=s"
        " outpre=step0_GRIDPACK"
        " mMediator={mz:.0f}"
        " mDark={mdark:.0f}"
        " rinv={rinv}".format(**physics)
    )
    if physics["boost"] > 0.0:
        cmd += " boost={}".format(physics["boost"])
    if physics["max_events"] > 0.0:
        cmd += " maxEvents={}".format(physics["max_events"])
    if nogridpack:
        cmd += " nogridpack=1"
    return cmd


class CMSSW(qondor.cmssw.CMSSW):
    """Subclass of the main CMSSW class for SVJ"""

    def __init__(self, *args, **kwargs):
        super(CMSSW, self).__init__(*args, **kwargs)
        self.svj_path = osp.join(self.cmssw_src, "SVJ/Production/test")

    def download_madgraph_tarball(self, physics):
        download_madgraph_tarball(physics, dst=self.svj_path)

    def _run_step(self, inpre, outpre, physics):
        """Runs the runSVJ script for 1 step"""
        expected_infile = osp.join(
            self.svj_path,
            madgraph_tarball_filename(physics)
            if inpre.startswith("step0")
            else svj_filename(inpre, physics),
        )
        expected_outfile = osp.join(self.svj_path, svj_filename(outpre, physics))
        if not osp.isfile(expected_infile):
            raise RuntimeError(
                "Expected input file {0} should exist now for step {1} -> {2}".format(
                    expected_infile, inpre, outpre
                )
            )
        self.run_commands(
            ["cd {0}".format(self.svj_path), step_cmd(inpre, outpre, physics)]
        )
        return expected_outfile

    def run_step(self, inpre, outpre, physics, n_attempts=1):
        """Wrapper around self._run_step with an n_attempts option"""
        i_attempt = 1
        while True:
            try:
                logger.info(
                    "Doing step %s -> %s (attempt %s/%s)",
                    inpre,
                    outpre,
                    i_attempt,
                    n_attempts,
                )
                expected_outfile = self._run_step(inpre, outpre, physics)
                return expected_outfile
            except subprocess.CalledProcessError:
                logger.error(
                    "Caught exception on step {0} -> {1}".format(inpre, outpre)
                )
                if i_attempt == n_attempts:
                    logger.error(
                        "step {0} -> {1} permanently failed".format(inpre, outpre)
                    )
                    raise
                else:
                    logger.error(
                        "This was attempt %s; Sleeping 60s and retrying", i_attempt
                    )
                    from time import sleep

                    sleep(60)
                    i_attempt += 1

    def run_chain(self, chain, physics, rootfile=None, move=False):
        """
        Runs a chain of steps. The first step is considered the input step (it is not ran).
        If rootfile is specified, it is copied into the svj_path. If move=True, it is moved instead (only
        possible for local filenames).
        """
        inpres = chain[:-1]
        outpres = chain[1:]
        # Copy/move the input rootfile if it's given
        if rootfile:
            expected_infile = osp.join(self.svj_path, svj_filename(inpres[0], physics))
            if move:
                logger.info("Moving %s -> %s", rootfile, expected_infile)
                os.rename(rootfile, expected_infile)
            else:
                seutils.cp(rootfile, expected_infile)
        # Run steps
        for inpre, outpre in zip(inpres, outpres):
            expected_outfile = self.run_step(
                inpre,
                outpre,
                physics,
                n_attempts=3 if ("RECO" in outpre or "DIGI" in outpre) else 1,
            )
        return expected_outfile

    def run_gridpack(self, physics, nogridpack=False):
        """
        Creates a MadGraph gridpack for the given physics
        """
        return self.run_commands(
            [
                "cd {0}".format(self.svj_path),
                gridpack_cmd(physics, nogridpack=nogridpack),
            ]
        )

    def make_madgraph_tarball(self, physics, max_events=1):
        """
        Runs the python to make the tarball
        """
        cmd = (
            "python runMG.py"
            " year={year}"
            " madgraph=1"
            " channel=s"
            " outpre=step0_GRIDPACK"
            " mMediator={mz:.0f}"
            " mDark={mdark:.0f}"
            " rinv={rinv}".format(**physics)
        )
        self.run_commands(["cd {0}".format(self.svj_path), cmd])
        return osp.join(
            self.svj_path,
            qondor.svj.madgraph_tarball_filename(
                Physics(physics, max_events=max_events)
            ),
        )


class TreeMakerCMSSW(qondor.cmssw.CMSSW):
    """Subclass of the main CMSSW class for SVJ"""

    _readfiles_cache = {}

    def __init__(self, *args, **kwargs):
        super(TreeMakerCMSSW, self).__init__(*args, **kwargs)
        self.treemaker_path = osp.join(self.cmssw_src, "TreeMaker/Production/test")

    @classmethod
    def get_readfiles(cls, bkg):
        """
        Hacky: gets the readFiles from the main TreeMaker repo.
        Caches results in class variable so subsequent calls just use the cache
        """
        if bkg in cls._readfiles_cache:
            return cls._readfiles_cache[bkg]
        import re

        scenario, bkg_string = bkg.split(".", 1)
        url = "https://raw.githubusercontent.com/TreeMaker/TreeMaker/Run2_2017/Production/python/{}/{}_cff.py".format(
            scenario, bkg_string
        )
        text = qondor.utils.strip_comments(qondor.utils.download_url_to_str(url))
        rootfiles = re.findall(r"/store/mc.*?root", text)
        cls._readfiles_cache[
            bkg
        ] = rootfiles  # Cache result so we can safely call the method again
        return rootfiles

    @classmethod
    def count_readfiles(cls, bkg):
        return len(cls.get_readfiles(bkg))

    def command_bkg(self, bkg, i_file, n_events=None):
        scenario = bkg.split(".")[0]
        cmd = (
            "cmsRun runMakeTreeFromMiniAOD_cfg.py"
            " outfile=outfile"
            " scenario={}"
            " inputFilesConfig={}"
            " lostlepton=0"
            " doZinv=0"
            " systematics=0"
            " deepAK8=0"
            " deepDoubleB=0"
            " doPDFs=0"
            " nestedVectors=False"
            " splitLevel=99"
            " nstart={}"
            " nfiles=1".format(scenario, bkg, i_file)
        )
        if n_events:
            cmd += " numevents={}".format(n_events)
        return cmd

    def run_bkg(self, *args, **kwargs):
        self.run_commands(
            ["cd {0}".format(self.treemaker_path), self.command_bkg(*args, **kwargs)]
        )
        expected_outfile = osp.join(self.treemaker_path, "outfile_RA2AnalysisTree.root")
        return expected_outfile


def init_cmssw(
    tarball_key="cmssw_tarball", scram_arch=None, outdir=None, init_class=CMSSW
):
    """
    Like the main qondor.init_cmssw, but for qondor.svj.CMSSW
    """
    if osp.isfile(tarball_key):
        # A path to a local tarball was given
        cmssw_tarball = tarball_key
    elif seutils.has_protocol(tarball_key):
        # A path to a tarball on a storage element was given
        cmssw_tarball = tarball_key
    else:
        # A key to a file in the preprocessing was given
        cmssw_tarball = qondor.get_preproc().files[tarball_key]
    cmssw = init_class.from_tarball(cmssw_tarball, scram_arch, outdir=outdir)
    return cmssw


def init_cmssw_treemaker(*args, **kwargs):
    return init_cmssw(init_class=TreeMakerCMSSW, *args, **kwargs)
