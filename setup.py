#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='intake-latticejson',
    version='0.0.1',
    description='Test plugin: latticejson',
    url='',
    maintainer='Trainee',
    maintainer_email='',
    license='BSD',
    packages=find_packages(),
    entry_points= {
        'intake.drivers': [
            'latticejson =  intake_latticejson.Latticejson'
            ]
        },
    include_package_data=True,
    install_requires=['intake'],
    long_description="",
    zip_safe=False,
)


