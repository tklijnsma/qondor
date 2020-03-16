import os, os.path as osp, logging, glob, shutil
import qondor
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

    @classmethod
    def for_release(cls, release, scram_arch=None, outdir='.', renew=True):
        logger.info(
            'Setting up CMSSW %s, arch %s in %s',
            release, scram_arch, outdir
            )
        # Make sure there is a directory in which to create the new CMSSW
        qondor.utils.create_directory(outdir)
        with qondor.utils.switchdir(outdir):
            # Check if there is an existing CMSSW
            existing = glob.glob('*{0}*'.format(release))
            if existing:
                existing = existing[0]
                if renew:
                    logger.warning(
                        'Detected existing CMSSW %s; deleting it',
                        osp.abspath(existing)
                        )
                    shutil.rmtree(existing)
                else:
                    logger.warning(
                        'Detected existing CMSSW %s; returning existing instead',
                        osp.abspath(existing)
                        )
                    return cls(osp.join(existing, 'src'), scram_arch)
            # Make fresh release
            qondor.utils.run_multiple_commands(
                [
                    'shopt -s expand_aliases',
                    'source /cvmfs/cms.cern.ch/cmsset_default.sh',
                    'cmsrel {0}'.format(release)
                    ],
                env = qondor.utils.get_clean_env()
                )
            # Get the src of the newly setup CMSSW
            cmssw_src = osp.abspath(
                glob.glob('*{0}*/src'.format(release))[0]
                )
        cmssw = cls(cmssw_src, scram_arch)
        # A freshly setup CMSSW won't need to be renamed
        cmssw._is_renamed = True
        return cmssw

    def __init__(self, cmssw_src, scram_arch=None):
        super(CMSSW, self).__init__()
        self.cmssw_src = osp.abspath(cmssw_src)
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
        return self.run_commands([cmd])

    def run_commands(self, cmds):
        """
        Main/Public method: Much like run_commands_nocmsenv, but includes cmsenv
        This is intended to be called with cmsRun/cmsDriver.py commands
        """
        return self.run_commands_nocmsenv(['cmsenv'] + cmds)

    def run_commands_nocmsenv(self, cmds):
        """
        Preprends the basic CMSSW environment setup, and executes a set of
        commands in a clean environment
        """
        self.rename_project()
        with qondor.utils.switchdir(self.cmssw_src):
            return qondor.utils.run_multiple_commands(
                [
                    'shopt -s expand_aliases',
                    'source /cvmfs/cms.cern.ch/cmsset_default.sh',
                    'export SCRAM_ARCH={0}'.format(self.scram_arch),
                    ] + cmds,
                env = qondor.utils.get_clean_env()
                )

    def make_tarball(self, outdir='.', tag=None, renew=True):
        """
        Makes a tarball out of the CMSSW distribution of this class
        """
        cmssw_path = osp.abspath(osp.join(self.cmssw_src, '..'))
        # Determine location of the output tarball
        dst = osp.join(
            osp.abspath(outdir),
            (
                osp.basename(cmssw_path).strip('/')
                + ('' if tag is None else '_' + tag)
                + '.tar.gz'
                )
            )
        if osp.isfile(dst):
            if renew:
                logger.warning('Removing existsing %s', dst)
                os.remove(dst)
            else:
                raise OSError('{0} already exists'.format(dst))
        logger.warning(
            'Tarballing {0} ==> {1}'
            .format(cmssw_path, dst)
            )
        with qondor.utils.switchdir(osp.dirname(cmssw_path)):
            cmd = [
                'tar',
                '--exclude-caches-all',
                '--exclude-vcs',
                '-zcvf',
                dst,
                osp.basename(cmssw_path),
                '--exclude=tmp',
                ]
            qondor.utils.run_command(cmd)
