"""# submit
# Make an example tarball - in a real job you would premake this
cmssw = qondor.CMSSW.for_release('CMSSW_11_2_0_pre10')
tarball_file = cmssw.make_tarball()

# Copy the tarball along with the job
submit(transfer_files=[tarball_file])
"""# endsubmit

import qondor
cmssw = qondor.CMSSW.from_tarball("CMSSW_11_2_0_pre10.tar.gz")
cmssw.run_commands([
    "echo $PWD",
    "which cmsRun",
    "cmsRun --help"
    ])