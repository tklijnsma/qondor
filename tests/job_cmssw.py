# $ file some_tarball CMSSW_11_0_0_pre10_HGCALHistoryWithCaloPositions.tar.gz
import qondor

preprocessing = qondor.preprocessing(__file__)
cmssw = qondor.CMSSW.from_tarball(preprocessing.files["some_tarball"])
cmssw.run_command(
    [
        "cmsRun",
        "HGCALDev/PCaloHitWithPostionProducer/python/SingleMuPt_pythia8_cfi_GEN_SIM_PCaloHitWithPosition.py",
        "outputFile=root://cmseos.fnal.gov//store/user/klijnsma/qondor_testing/test.root",
        "maxEvents=5",
    ]
)
