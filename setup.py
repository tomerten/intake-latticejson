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
    #py_modules=['intake_latticejson'],
    #packages=['intake_latticejon'],
    #
    packages=find_packages(),
    entry_points= {
        'intake.drivers': [
            'lattice_json =  intake_latticejson.intake_latticejson:Latticejson',
            ]
        },
    include_package_data=True,
    install_requires=['intake'],
    long_description="",
    zip_safe=False,
)


