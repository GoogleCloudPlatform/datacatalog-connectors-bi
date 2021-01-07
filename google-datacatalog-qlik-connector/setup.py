#!/usr/bin/python
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import setuptools

release_status = 'Development Status :: 4 - Beta'

with open('README.md') as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name='google-datacatalog-qlik-connector',
    version='0.1.1',
    author='Google LLC',
    description='Package for ingesting Qlik metadata'
    ' into Google Cloud Data Catalog',
    platforms='Posix; MacOS X; Windows',
    packages=setuptools.find_packages(where='./src'),
    namespace_packages=['google', 'google.datacatalog_connectors'],
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'google-datacatalog-qlik-connector = google.datacatalog_connectors.qlik:main',
        ],
    },
    include_package_data=True,
    install_requires=(
        'google-datacatalog-connectors-commons ~= 0.6.7',
        'jmespath ~= 0.10.0',
        'requests_ntlm ~= 1.1.0',
        'websockets ~= 8.1',
    ),
    setup_requires=('pytest-runner',),
    tests_require=('pytest-cov',),
    classifiers=[
        release_status,
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    long_description=readme,
    long_description_content_type='text/markdown',
)
