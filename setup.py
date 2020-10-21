from setuptools import setup

setup(
    name          = 'qondor',
    version       = '1.6',
    license       = 'BSD 3-Clause License',
    description   = 'Description text',
    url           = 'https://github.com/tklijnsma/qondor.git',
    download_url  = 'https://github.com/tklijnsma/qondor/archive/v1_6.tar.gz',
    author        = 'Thomas Klijnsma',
    author_email  = 'tklijnsm@gmail.com',
    packages      = ['qondor'],
    zip_safe      = False,
    scripts       = [
        'bin/qondor-submit',
        'bin/qondor-make-cmssw-tarball'
        ],
    install_requires     = ['seutils'],
    include_package_data = True,
    )
