# qondor

This is a lightweight tool to make htcondor submission a little easier.

It requires that the `htcondor` python bindings are installed.


## Installation

```
pip install qondor
```

Or from source:

```
git clone https://github.com/tklijnsma/qondor.git
pip install -e qondor/
```

## Examples


### Hello world!

Create a file with the following contents:

```
print 'Hello world!'
```

and submit it with:

```
qondor-submit whatever_you_called_it.py
```

A directory called `whatever_you_called_it_YYYYMMDD_HHMMSS` will be created, a job will be submitted that runs the code in `whatever_you_called_it.py`, and relevant job files will be stored in the directory.



### Hello world! multiple jobs

Create a file with the following contents:

```
#$ njobs 2
print 'Hello world!'
```

Running it locally will just print "Hello World!":

```
python whatever_you_called_it.py
>>> Hello world!
```

Submitting it with

```
qondor-submit whatever_you_called_it.py
```

will once again create a directory `whatever_you_called_it_YYYYMMDD_HHMMSS`.
This time, two jobs will be submitted.



### A CMSSW tarball

Create the following python file:

```
#$ file tarball path/to/CMSSW_X_Y_Z.tar.gz
#$ njobs 2

import qondor
preprocessing = qondor.preprocessing(__file__)
cmssw = qondor.CMSSW.from_tarball(preprocessing.files['tarball'])
cmssw.run_commands([
    'cd $CMSSW_BASE/src/Package/SubPackage/python',
    'cmsRun some_cfg.py',
    ])
```

Once again, running locally ignores any line that starts with `#`, allowing you to debug your code base:

```
python whatever_you_called_it.py
```

Submitting it to htcondor does a little more:

```
qondor-submit whatever_you_called_it.py
```

will copy the tarball to two worker nodes, extract the tarball, and run some commands in the CMSSW environment that was stored in the tarball (i.e. `cmsenv` is called).


### Multiple CMSSW tarball jobs that run at some specific time

Create the following python file:

```
#$ file tarball path/to/CMSSW_X_Y_Z.tar.gz
#$ njobs 2
#$ delay 30 m
#$ allowed_lateness 5 m

import qondor
preprocessing = qondor.preprocessing(__file__)
cmssw = qondor.CMSSW.from_tarball(preprocessing.files['tarball'])
cmssw.run_commands([
    'cd $CMSSW_BASE/src/Package/SubPackage/python',
    'cmsRun some_cfg.py',
    ])
```

Upon doing `qondor-submit whatever_you_called_it.py`, the scheduled run time will be set to 30 minutes in the future from now. `njobs` are submitted, and will sleep until the calculated scheduled run time. Jobs will still be allowed to start up to 5 minutes after the scheduled run time.


### More examples

Todo


## Full documentation

Todo
