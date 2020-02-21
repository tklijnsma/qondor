import os
import os.path as osp
import qondor
import logging
logger = logging.getLogger('qondor')


class CMSSW(object):
    """docstring for CMSSW"""

    default_local_rundir = '/tmp/qondor'
    default_remote_rundir = '.'

    @staticmethod
    def extract_tarball(tarball, outdir=None):
        if outdir is None:
            outdir = CMSSW.default_remote_rundir if qondor.BATCHMODE else CMSSW.default_local_rundir
            logger.warning(
                'Will extract %s to default: %s',
                tarball, outdir
                )
        qondor.utils.create_directory(outdir)
        cmssw_dir = qondor.utils.extract_tarball_cmssw(tarball, outdir=outdir)
        cmssw_src = osp.abspath(osp.join(cmssw_dir, 'src'))
        return cmssw_src

    @classmethod
    def from_tarball(cls, tarball, scram_arch=None, outdir=None):
        cmssw_src = cls.extract_tarball(tarball, outdir)
        return cls(cmssw_src, scram_arch)

    def __init__(self, cmssw_src, scram_arch=None):
        super(CMSSW, self).__init__()
        self.cmssw_src = cmssw_src
        if scram_arch is None:
            logger.warning(
                'Taking SCRAM_ARCH from environment; may mismatch with'
                ' CMSSW version of %s', self.cmssw_src
                )
            self.scram_arch = os.environ['SCRAM_ARCH']
        else:
            self.scram_arch = scram_arch
        self._is_renamed = False
        self._is_externallinks = False

    def rename_project(self):
        if self._is_renamed: return
        self._is_renamed = True
        logger.info('Renaming project %s', self.cmssw_src)
        self.run_commands_nocmsenv(['scram b ProjectRename'])

    def scramb_externallinks(self):
        if self._is_externallinks: return
        self._is_externallinks = True
        logger.info('Doing scram b ExternalLinks %s', self.cmssw_src)
        self.run_commands_nocmsenv(['scram b ExternalLinks'])

    def run_command(self, cmd):
        """
        Mostly legacy; use run_commands instead
        """
        self.run_commands([cmd])

    def run_commands(self, cmds):
        """
        Main/Public method: Much like run_commands_nocmsenv, but includes cmsenv
        This is intended to be called with cmsRun/cmsDriver.py commands
        """
        self.run_commands_nocmsenv(['cmsenv'] + cmds)

    def run_commands_nocmsenv(self, cmds):
        """
        Preprends the basic CMSSW environment setup, and executes a set of
        commands in a clean environment
        """
        self.rename_project()
        with qondor.utils.switchdir(self.cmssw_src):
            qondor.utils.run_multiple_commands(
                [
                    'shopt -s expand_aliases',
                    'source /cvmfs/cms.cern.ch/cmsset_default.sh',
                    'export SCRAM_ARCH={0}'.format(self.scram_arch),
                    ] + cmds,
                env = qondor.utils.get_clean_env()
                )
