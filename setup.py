#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

version = '1.0.0'

setuptools.setup(
    name='pyfirecrest',
    version=version,
    author='CSCS Swiss National Supercomputing Center',
    description='pyFirecrest is a python wrapper for FirecREST',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ekouts/pyfirecrest',
    license='BSD 3-Clause',
    # packages=setuptools.find_packages(),
    packages=["firecrest"],
    # package_data={},
    include_package_data=True,
    classifiers=(
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'License :: OSI Approved :: BSD License',
        "Operating System :: OS Independent",
    ),
    python_requires='>=3.7',
    # FIXME PyJWT is only necessary until FirecREST has an appropriate endpoint
    install_requires=['requests>=2.14.0', 'PyJWT==1.7.1'],
)
