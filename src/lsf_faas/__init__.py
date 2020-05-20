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

from lsf_faas.lsf import *
from functools import wraps

ipython = get_ipython()
if ipython is None:
    print('Import Failed. This tool can only be used in IPYTHON context.')
else:
    lsf= lsf()

def bsub(func):
    @wraps(func)
    def with_bsub( *arguments, files = None, asynchronous = False):
        print(func.__name__ + " was called")
        return lsf.sub(func,  *arguments, files = files, asynchronous = False)
    return with_bsub