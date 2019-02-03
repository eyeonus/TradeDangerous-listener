#!/usr/bin/env python3
# --------------------------------------------------------------------
# Copyright (C) Oliver 'kfsone' Smith 2014 <oliver@kfs.org>:
# Copyright (C) Bernd 'Gazelle' Gollesch 2016, 2017
# Copyright (C) Jonathan 'eyeonus' Jones 2018
#
# You are free to use, redistribute, or even print and eat a copy of
# this software so long as you include this copyright notice.
# I guarantee there is at least one bug neither of us knew about.
# --------------------------------------------------------------------
"""Setup for trade-dangerous eddblink_listener"""

from setuptools import setup, find_packages

package = "tdlistener"

exec(open("tdlistener/version.py").read()) #pylint: disable=W0122

setup(name=package,
        version=__version__, #pylint: disable=E0602
        install_requires=["tradedangerous", "pyzmq"],
        packages=find_packages(exclude=["tests"]),
        url="https://github.com/eyeonus/EDDBlink-listener",
        project_urls={
            "Bug Tracker": "https://github.com/eyeonus/EDDBlink-listener/issues",
            "Documentation": "https://github.com/eyeonus/EDDBlink-listener/wiki",
            "Source Code": "https://github.com/eyeonus/EDDBlink-listener",
        },
        author="eyeonus",
        author_email="eyeonus@gmail.com",
        description="An EDDN listener, designed to work in conjunction with the EDDBlink plugin for Trade Dangerous.",
        keywords=["trade", "elite", "elite-dangerous"],
        classifiers=[
            "Programming Language :: Python :: 3"
            "Programming Language :: Python :: 3.5"
            "Programming Language :: Python :: 3.6"
        ],
        license="MPL",
        entry_points={
            "console_scripts": [
                "tdlistener=tdlistener.eddblink_listener:main"
            ]
        },
        zip_safe=False
)
