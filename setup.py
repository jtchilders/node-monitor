#!/usr/bin/env python3
"""
Setup script for node_monitor -- ALCF login-node observability.
"""

from setuptools import setup, find_packages
import os


def read_version():
   """Read version from package __init__.py"""
   with open('node_monitor/__init__.py', 'r') as f:
      for line in f:
         if line.startswith('__version__'):
            return line.split('=')[1].strip().strip('"\'')
   return '0.1.0'


def read_long_description():
   if os.path.exists('README.md'):
      with open('README.md', 'r', encoding='utf-8') as f:
         return f.read()
   return ''


def read_requirements():
   requirements = []
   if os.path.exists('requirements.txt'):
      with open('requirements.txt', 'r') as f:
         for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
               requirements.append(line)
   return requirements


setup(
   name='node-monitor',
   version=read_version(),
   description='Login-node observability for ALCF systems',
   long_description=read_long_description(),
   long_description_content_type='text/markdown',
   packages=find_packages(exclude=['tests', 'tests.*']),
   python_requires='>=3.9',
   install_requires=read_requirements(),
   entry_points={
      'console_scripts': [
         'node-monitor=node_monitor.cli.main:main',
      ],
   },
   include_package_data=True,
   zip_safe=False,
)
