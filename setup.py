from setuptools import setup

with open("qondor/include/VERSION", "r") as f:
    version = f.read().strip()

setup(
    name="qondor",
    version=version,
    license="BSD 3-Clause License",
    description="Description text",
    url="https://github.com/tklijnsma/qondor.git",
    author="Thomas Klijnsma",
    author_email="tklijnsm@gmail.com",
    packages=["qondor"],
    zip_safe=False,
    scripts=[
        "bin/qondor-submit",
        "bin/qondor-resubmit",
        "bin/qondor-status",
        "bin/qondor-make-cmssw-tarball",
    ],
    install_requires=["seutils"],
    include_package_data=True,
)
