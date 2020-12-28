# Script that updates the version in setup.py.
from __future__ import print_function

import os.path as osp


def update_version():
    version_file = osp.join(osp.dirname(__file__), "qondor/include/VERSION")
    with open(version_file, "r") as f:
        version = f.read().strip()
    major, minor = version.rsplit(".", 1)
    minor = str(int(minor) + 1)
    updated_version = major + "." + minor
    print("Updating {0} from {1} to {2}".format(version_file, version, updated_version))
    with open(version_file, "w") as f:
        f.write(updated_version)


if __name__ == "__main__":
    update_version()
