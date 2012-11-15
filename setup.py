#!/usr/bin/env python

try:
    from setuptools import setup, Extension
    setup, Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension
    setup, Extension

import os
import sys

if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()


desc = open("README.rst").read()
with open("requirements.txt") as f:
    required = f.readlines()

setup(
    name="casjobs",
    version="0.0.1",
    author="Daniel Foreman-Mackey",
    author_email="danfm@nyu.edu",
    url="https://github.com/dfm/casjobs",
    py_modules=["casjobs", ],
    install_requires=required,
    license="MIT",
    description="An interface to CasJobs for Humans.",
    long_description=desc,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
)
