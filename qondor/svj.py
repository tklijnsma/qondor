'''
Module specific for the (boosted) svj analysis
'''

import os, os.path as osp, logging, glob, shutil, time, subprocess
import qondor, seutils
logger = logging.getLogger('qondor')

MG_TARBALL_PATH = 'root://cmseos.fnal.gov//store/user/lpcsusyhad/SVJ2017/boosted/mg_tarballs'

class Physics(dict):
    '''Holds the desired physics'''
    def __init__(self, *args, **kwargs):
        # Set some default values
        self['mz'] = 150.
        self['rinv'] = 0.3
        self['boost'] = 0.
        self['mdark'] = 20.
        self['alpha'] = 'peak'
        self['max_events'] = None
        super(Physics, self).__init__(*args, **kwargs)

    def boost_str(self):
        '''Format string if boost > 0'''
        return '_HT{0:.0f}'.format(self['boost']) if self['boost'] > 0. else ''

    def max_events_str(self):
        return '' if self.get('max_events', None) is None else '_n-{0}'.format(self['max_events'])


def svj_filename(step, physics):
    '''
    Returns a basename for a root file that is input or output of a given step for given physics
    '''
    rootfile = (
        '{step}_s-channel_mMed-{mz:.0f}_mDark-{mdark:.0f}_rinv-{rinv}_'
        'alpha-{alpha}{boost_str}_13TeV-madgraphMLM-pythia8{max_events_str}.root'
        .format(step=step, boost_str=physics.boost_str(), max_events_str=physics.max_events_str(), **physics)
        )
    if physics.get('part', None): rootfile = rootfile.replace('.root', '_part-{}.root'.format(physics['part']))
    return rootfile


def madgraph_tarball_filename(physics):
    '''Returns the basename of a MadGraph tarball for the given physics'''
    return svj_filename('step0_GRIDPACK', Physics(physics, part=None)).replace('.root', '.tar.xz')


def download_madgraph_tarball(physics, dst=None):
    '''Downloads tarball from the storage element'''
    dst = osp.join(
        os.getcwd() if dst is None else dst,
        madgraph_tarball_filename(physics)
        )
    # Tarballs on SE will not have the boost tag and have postfix "_n-1"
    src = osp.join(MG_TARBALL_PATH, madgraph_tarball_filename(Physics(physics, boost=0., max_events=1)))
    if osp.isfile(dst):
        logger.info('File %s already exists', dst)
    else:
        logger.info('Downloading %s --> %s', src, dst)
        seutils.cp(src, dst)


def step_cmd(inpre, outpre, physics):
    cmd = (
        'cmsRun runSVJ.py'
        ' year={year}'
        ' madgraph=1'
        ' channel=s'
        ' outpre={outpre}'
        ' config={outpre}'
        ' part={part}'
        ' mMediator={mz:.0f}'
        ' mDark={mdark:.0f}'
        ' rinv={rinv}'
        ' inpre={inpre}'
        .format(inpre=inpre, outpre=outpre, **physics)
        )
    if 'boost' in physics: cmd += ' boost={0:.0f}'.format(physics['boost'])
    if 'max_events' in physics: cmd += ' maxEvents={0}'.format(physics['max_events'])
    return cmd


def init_cmssw(tarball_key='cmssw_tarball', scram_arch=None, outdir=None):
    """
    Like the main qondor.init_cmssw, but for qondor.svj.CMSSW
    """
    cmssw_tarball = qondor.get_preproc().files[tarball_key]
    cmssw = CMSSW.from_tarball(cmssw_tarball, scram_arch, outdir=outdir)
    return cmssw


class CMSSW(qondor.CMSSW):
    '''Subclass of the main CMSSW class for SVJ'''
    def __init__(self, *args, **kwargs):
        super(CMSSW, self).__init__(*args, **kwargs)
        self.svj_path = osp.join(self.cmssw_src, 'SVJ/Production/test')

    def download_madgraph_tarball(self, physics):
        download_madgraph_tarball(physics, dst=self.svj_path)

    def _run_step(self, inpre, outpre, physics):
        '''Runs the runSVJ script for 1 step'''
        expected_infile = osp.join(
            self.svj_path,
            madgraph_tarball_filename(physics) if inpre.startswith('step0') else svj_filename(inpre, physics)
            )
        expected_outfile = osp.join(self.svj_path, svj_filename(outpre, physics))
        if not osp.isfile(expected_infile):
            raise RuntimeError(
                'Expected input file {0} should exist now for step {1} -> {2}'
                .format(expected_infile, inpre, outpre)
                )
        self.run_commands([
            'cd {0}'.format(self.svj_path),
            step_cmd(inpre, outpre, physics)
            ])
        return expected_outfile

    def run_step(self, inpre, outpre, physics, n_attempts=1):
        '''Wrapper around self._run_step with an n_attempts option'''
        i_attempt = 1
        while(True):
            try:
                logger.info('Doing step %s -> %s (attempt %s/%s)', inpre, outpre, i_attempt, n_attempts)
                expected_outfile = self._run_step(inpre, outpre, physics)
                return expected_outfile
            except subprocess.CalledProcessError:
                logger.error('Caught exception on step {0} -> {1}'.format(inpre, outpre))
                if i_attempt == n_attempts:
                    logger.error('step {0} -> {1} permanently failed'.format(inpre, outpre))
                    raise
                else:
                    logger.error('This was attempt %s; Sleeping 60s and retrying', i_attempt)
                    from time import sleep
                    sleep(60)
                    i_attempt += 1

    def run_chain(self, chain, physics, rootfile=None, move=False):
        '''
        Runs a chain of steps. The first step is considered the input step (it is not ran).
        If rootfile is specified, it is copied into the svj_path. If move=True, it is moved instead (only
        possible for local filenames).
        '''
        inpres = chain[:-1]
        outpres = chain[1:]
        # Copy/move the input rootfile if it's given
        if rootfile:
            expected_infile = osp.join(self.svj_path, svj_filename(inpres[0], physics))
            if move:
                logger.info('Moving %s -> %s', rootfile, expected_infile)
                os.rename(rootfile, expected_infile)
            else:
                seutils.cp(rootfile, expected_infile)
        # Run steps
        for inpre, outpre in zip(inpres, outpres):
            expected_outfile = self.run_step(
                inpre, outpre, physics,
                n_attempts = 3 if 'RECO' in outpre else 1
                )
        return expected_outfile
    
