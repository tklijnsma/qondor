# Script that updates the version in setup.py.
from __future__ import print_function

import os.path as osp
import re


def update_version_no(contents):
    pat = r"\s*version\s*=\s*\"([\d\.]+)\""
    match = re.search(pat, contents)
    if not match:
        raise RuntimeError("Cannot update version number")
    version_components = match.group(1).split(".")
    version_components[-1] = str(int(version_components[-1]) + 1)
    new_version_str = ".".join(version_components)
    begin, end = match.span(1)
    contents = contents[:begin] + str(new_version_str) + contents[end:]

    # Now the replacement in the download_url
    pat = r"\s*download_url\s*=\s*\"[\w\.\:\/\_]+v([\d\_]+)\.tar\.gz\""
    match = re.search(pat, contents)
    if not match:
        raise RuntimeError("Cannot update version number")
    version_components = match.group(1).split("_")
    version_components[-1] = str(int(version_components[-1]) + 1)
    new_version_str = "_".join(version_components)
    begin, end = match.span(1)
    contents = contents[:begin] + str(new_version_str) + contents[end:]
    return contents


setuppy = osp.join(osp.dirname(__file__), "setup.py")
with open(setuppy, "r") as f:
    contents = f.read()

print("> Updating contents from:")
print(contents)

contents = update_version_no(contents)

print("> To:")
print(contents)

with open(setuppy, "w") as f:
    f.write(contents)
