# qondor

qondor is a lightweight tool that hopes to make job submission on htcondor clusters a little
easier.

In order to scale out a physics application via htcondor, typically one must maintain three
code bases:

1. The actual physics application (the fun part)
2. The code to run the application as a job (for htcondor, some executable, typically a shell script `job.sh`)
3. The code to submit the 'job code' to the nodes (for htcondor, a .jdl file)

While shell scripts and .jdl files start out manageable enough, commonly physicists quickly
resort to their own .jdl-file generators, submission scripts with hard-to-decipher BASH functions, 
or other forms of abstraction.
And typically, all this abstraction work does not carry over
from one physics application to another;
When a new physics application is made, the process of custom .jdl-file
generators and bash scripts starts all over again.

qondor aims to make it easier to scale out high energy physics applications by doing bullets
2 and 3 in python.


## Installation

```
pip install qondor
```

Or from source:

```
git clone https://github.com/tklijnsma/qondor.git
pip install -e qondor/
```

qondor is supported for python 3 and (begrudgingly) down to python 2.6.


## Examples

The most basic example consists of a simple, ordinary python script.
Create a file with the following contents:

```
print('Hello world!')
```

and submit it with:

```
qondor-submit whatever_you_called_it.py
```

A directory called `qondor_whatever_you_called_it_YYYYMMDD_HHMMSS` will be created, a job will
be submitted that runs the code in `whatever_you_called_it.py`,
and relevant job files will be stored in the directory.
If you inspect the `stderr` of the job, you should see:

```
>>> Hello World!
```

being printed.

Note that you can run the 'job code' locally by just running the script directly with python:

```
python whatever_you_called_it.py
```

This is in general possible, also for more complicated jobs.
qondor is designed such that you can always test your job code locally before scaling out.

See also the files in the [examples](examples) directory.


### Flexible jobs

qondor allows you to quickly submit many customized jobs:

```
"""# submit
for some_str in [ "foo", "bar" ]:
    submit(my_str=some_str)
"""# endsubmit

import qondor

print("This is {0}".format(qondor.scope.my_str))
```

The code between the `"""# submit` and `"""# endsubmit` tags is the *submission code* -
the python code that is ran at submission time. The rest of the file is considered ordinary
python code.

The submission code has access to the `submit` function,
which launches jobs every time it is called.
Keyword arguments to the `submit` function are available in the job using the `qondor.scope`
object (with the limitation that values are JSON serializable).

Running the code above with `qondor-submit whatever_you_called_it.py` will launch two jobs,
each printing a string:

```
# In job 1
>>> This is foo
```

```
# In job 2
>>> This is bar
```

Running the above code with `python whatever_you_called_it.py` will run only the _first job_
locally, i.e. it will break after the first `submit` call. In this case, `python whatever_you_called_it.py`
will simply print `This is foo`.

Note this allows you to very easily submit 'x jobs per data file' - a feature that is hard
to achieve with default HTCondor:

```
"""# submit
from glob import glob

for rootfile in glob('/my/data/path/*.root'):
    submit(rootfile=rootfile)
"""# endsubmit

import qondor
print("Processing rootfile {0}".format(qondor.scope.rootfile))
```

(Also this example can be run locally with `python whatever_you_called_it.py` -
only the first rootfile in the `glob` pattern would be printed).


### CMSSW jobs

qondor runs CMSSW code in a subshell via `subprocess`. Consider the following example:

```
"""# submit
# Make an example tarball - in a real job you would use some prepared tarball
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
```

This first part just creates a standard CMSSW tarball from scratch - in a more
realistic scenario, a physicist would create a CMSSW tarball beforehand.
The tarball is copied to the worker node by specifying it in the `transfer_files` keyword
of the `submit` function.

Inside the job, the tarball is extracted, and one can easily interact with it by using
the [`qondor.CMSSW`](qondor/cmssw.py) class.
The CMSSW environment is set up in a subshell, so your main job environment is not polluted.
The example job above would print (along with other stuff) the following in the jobs stderr:

```
...
[qondor|    INFO|2020-12-23 00:19:11|utils]: Sending cmds:
['shopt -s expand_aliases',
 'source /cvmfs/cms.cern.ch/cmsset_default.sh',
 'export SCRAM_ARCH=slc7_amd64_gcc820',
 'cmsenv',
 'echo $PWD',
 'which cmsRun',
 'cmsRun --help']
[2020-12-23 00:19:12]: /storage/local/data1/condor/execute/dir_22757/CMSSW_11_2_0_pre10/src
[2020-12-23 00:19:12]: /cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw/CMSSW_11_2_0_pre10/bin/slc7_amd64_gcc820/cmsRun
[2020-12-23 00:19:18]: cmsRun [options] [--parameter-set] config_file 
[2020-12-23 00:19:18]: Allowed options:
[2020-12-23 00:19:18]:   -h [ --help ]                         produce help message
[2020-12-23 00:19:18]:   -p [ --parameter-set ] arg            configuration file
[2020-12-23 00:19:18]:   -j [ --jobreport ] arg                file name to use for a job report file:
[2020-12-23 00:19:18]:                                         default extension is .xml
[2020-12-23 00:19:18]:   -e [ --enablejobreport ]              enable job report files (if any) 
[2020-12-23 00:19:18]:                                         specified in configuration file
[2020-12-23 00:19:18]:   -m [ --mode ] arg                     Job Mode for MessageLogger defaults - 
[2020-12-23 00:19:18]:                                         default mode is grid
[2020-12-23 00:19:18]:   -n [ --numThreads ] arg               Number of threads to use in job (0 is 
[2020-12-23 00:19:18]:                                         use all CPUs)
[2020-12-23 00:19:18]:   -s [ --sizeOfStackForThreadsInKB ] arg
[2020-12-23 00:19:18]:                                         Size of stack in KB to use for extra 
[2020-12-23 00:19:18]:                                         threads (0 is use system default size)
[2020-12-23 00:19:18]:   --strict                              strict parsing
[2020-12-23 00:19:18]: 
[qondor|    INFO|2020-12-23 00:19:18|utils]: Command exited with status 0 - all good
...
```

Note also this script can be tested locally before submitting to HTCondor.

Copying big tarballs around via HTCondor is typically not recommended.
`qondor.CMSSW` can also be initialized from a tarball located remotely:

```
"""# submit
submit()
"""# endsubmit

import qondor
cmssw = qondor.CMSSW.from_tarball("root://cmseos.fnal.gov//store/user/klijnsma/my_CMSSW_tarball.tar.gz")
cmssw.run_commands([
    "echo $PWD",
    "which cmsRun",
    "cmsRun --help"
    ])
```

In this case, qondor will simply copy the tarball to the worker node first.
This workflow is recommended over copying tarballs via HTCondor.


### Command line arguments

qondor passes command line arguments to the job, provided they are placed _behind_
the python script.
Consider the following job code:

```
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("some_string", type=str)
args = parser.parse_args()
print(args.some_string)
```

Running `qondor-submit whatever_you_called_it.py foobar` will print `foobar`.

This allows you to easily modify via the command line what jobs should do.
You don't have to use `argparse`, you can use any command line interpreter -
go wild with configuration.


### Python packages

qondor has some special support for python packages.
If your job requires some non-standard python library, specify it as an installable
in the submit code:

```
"""# submit
pip('pathspec')
submit()
"""# endsubmit

import pathspec
print(pathspec)
```

The job should print something like `<module 'pathspec' from '/storage/local/data1/condor/execute/dir_20305/install/lib/python2.7/site-packages/pathspec/__init__.pyc'>`.
Note that running this locally with `python whatever_you_called_it.py` requires that the
package is already installed - qondor does not install packages except when ran as a job.

There is also support for locally installed 'editable' packages: 
`pip('mypackage')` will try to create a tarball out of your package and install it on the worker node.


### HTCondor settings

You can use the `htcondor` function in the submission code to pass along HTCondor settings:

```
"""# submit
htcondor('Hold', 'True')
submit()
"""# endsubmit

print('Hello world!')
```

Running `qondor-submit whatever_you_called_it.py` will submit one job in the `Held` state.
Note that HTCondor attributes that expect a string require double quotations:
`htcondor('Foo', '"Bar"')`.



## A note about submission

HTCondor has 
[official python bindings](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/),
but you are usually at the mercy of the system administrator to install these system-wide.

If the htcondor python bindings are not installed system-wide, you can try to install them in
a conda environment. First determine the version of HTCondor installed on your system
(using `condor_version`), and install the exact same version using pip:

```
pip install htcondor==X.Y.Z
```

This works for some setups, and fails for others.

By default, if qondor can find the htcondor python bindings on the path, it will try to use
them. You can suppress this behavior by using `qondor-submit --cli myjob.py`;
qondor will then parse a regular .jdl file first and use the `condor_submit` command line
tool to actually submit the job.

*It is common practice for sys admins to replace the default `condor_submit` with their own
python script*.
In this case, it is usually better to use the command line `condor_submit`, since it may be
hard to figure out what additional HTCondor settings the admins added in their modded
`condor_submit` script.
A notable example is CMS Connect, which adds a bunch of attributes to your job before actually
submitting it.
The exception to this is the Fermilab LPC: qondor has special settings for the LPC that
allow it to use the python bindings.
