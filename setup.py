# Copyright International Business Machines Corp, 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages
import sys

if sys.version_info[0] == 2 or sys.version <'3':
    sys.exit("Python 2 is not supported.")

setup(
    name="lsf_faas",
    version="0.1",
    license="Apache License Version 2.0",
    description="Python package to send function calls as jobs to LSF",
    # To avoid the bug in setuptools: https://github.com/pypa/setuptools/issues/250
    # create a subdirectory lsf_faas in src and use the map:
    packages=['lsf_faas'],
    package_dir={'lsf_faas': 'src/lsf_faas'},
)
