#$ file cmssw_tarball CMSSW_11_0_0_pre10_HGCALHistoryWithCaloPositions.tar.gz
import qondor
qondor.init_cmssw().cmsrun(
    'HGCALDev/PCaloHitWithPostionProducer/python/SingleMuPt_pythia8_cfi_GEN_SIM_PCaloHitWithPosition.py',
    outputFile = 'tmp/qondor/{cluster_id}/{proc_id}_{datestr}.root',
    maxEvents = 5
    )