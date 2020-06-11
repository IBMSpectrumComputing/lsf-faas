#!/usr/bin/env python3

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


from concurrent.futures import ThreadPoolExecutor
import datetime
import errno
import functools
from functools import wraps
import getpass
import inspect
from IPython import get_ipython
from lsf_faas.lsflib import *
import os
import shutil
import signal
import sys
import threading
import time
import uuid

class lsf(object):
    """
    This class allows you to send function calls(especially for time-consuming) as jobs to LSF without blocking.
    """

    interval = 5

    def __init__(self):
        self.__input_module_set=set()
        self.__func_d = {}
        if os.name == 'nt':
            self.work_dir = os.sep.join([os.environ['HOMEDRIVE'], os.environ['HOMEPATH'], WORK_DIR_NAME])
        else:
            self.work_dir = os.sep.join([os.environ['HOME'], WORK_DIR_NAME])

        is_exists = os.path.exists(self.work_dir)
        if is_exists:
            # delete sub-dir more than 30 days (modify date) in work_dir.
            father = list(os.listdir(self.work_dir))
            for i in range(len(father)):
                subidr = os.sep.join([self.work_dir , father[i]])
                if os.path.isdir(subidr):
                    path_date = os.path.getmtime(subidr)
                    current = time.time()
                    num = (current - path_date)/60/60/24
                    if num >= 30:
                        try:
                            shutil.rmtree(subidr)
                            fomrat_date = datetime.datetime.fromtimestamp(path_date).strftime('%Y-%m-%d')
                            print("Deleted %s: %s" % (subidr, fomrat_date))
                        except Exception as e:
                            print(e)
        else:
            os.makedirs(self.work_dir)

        ipython = get_ipython()
        ipython.events.register('post_run_cell', self.__postRunCell)

        success, output = verifyToken(self.work_dir)
        if success:
            self.__is_logged = True
        else:
            self.__is_logged = False

        self.__thread_pool = None

    def __postRunCell(self, result):
        try:
            lines = result.info.raw_cell.split('\n')
            input_module_list = []
            for line in lines:
                line.strip()
                # does not support other format yet
                if (line.startswith('import ') == True) or (line.startswith('from ') == True):
                    input_module_list.append(line)

            if len(input_module_list) > 0:
                # remove the failed import
                if result.error_in_exec:
                    missed_module = str(result.error_in_exec).split('\'')[1]

                    for line in input_module_list:
                        words = line.split(' ')
                        for word in words:
                            if word is not 'import' and word is not 'from' and (missed_module == word or missed_module in word):
                                return
                        self.__input_module_set.add(line)

                elif result.error_before_exec:
                    return
                else:
                    for line in input_module_list:
                        self.__input_module_set.add(line)

        except Exception as e:
            print('Failed to run post_run_cell, due to %s' %(e))
            return


    def __generateScript(self, script_name, func, *arguments):
        try:
            tmp_file = open(script_name, "a")
            # make sure we import the right modules
            for line in self.__input_module_set:
                if 'lsf_faas' in line:
                    pass
                else:
                    tmp_file.write(line + '\n')

            tmp_file.write('import os \n')
            tmp_file.write('import base64 \n')
            tmp_file.write('import dill \n')
            tmp_file.write('\n')
            # remove symbol of decorator
            lines = inspect.getsource(func).split('\n')
            output =''
            for line in lines:
                if (line.startswith('@')) == False:
                    output += line
                    output += '\n'
            tmp_file.write(output)

            tmp_file.write('\n')

            counts = 1
            args_strings = ''
            # the reason of serializable/deserialize:
            # 1. keep the orginal data type
            # 2. the generate script file will be transfered from/to socket, so must change the bytes to str
            for tmp in arguments:
                # serializable:
                # dill.dumps(): returns the encapsulated object(tmp) as a byte object,
                # base64.b64encode(): return the b'strings', since the characters in 3.x are unicode encodings and the arguments to the b64encode function are of type byte
                # str(): remove the prefix 'b' to return type str
                # write the str as the arg, example: arg1 = "YWJjcjM0cjM0NHI="
                # deserialize:
                # bytes(): change the str to type bytes
                # base64.b64decode()
                # dill.loads(): return object
                tmp_file.write('arg'+ str(counts) + ' = \"' + str(base64.b64encode(dill.dumps(tmp)),'utf-8') + '\" \n')
                args_strings = args_strings + 'dill.loads(base64.b64decode(bytes(arg' + str(counts) +', encoding = "utf8"))), '

                counts +=1

            tmp_file.write('result = ' + func.__name__ + '(' )
            # remove the last chars ","
            if len(args_strings) > 2:
                tmp_file.write(args_strings[:-2])
            else:
                tmp_file.write(args_strings)

            tmp_file.write(') \n')

            tmp_file.write('str = dill.dumps(result)\n')
            tmp_file.write('f = open("' + OUTPUT_FILE_NAME + '", "wb")\n')
            tmp_file.write('f.write(base64.b64encode(str))\n')
            tmp_file.write('f.close()\n')
            tmp_file.write('\n')

        except Exception as e:
            tmp_file.close()
            return False, 'Found error when generate data: %s' % e

        finally:
            tmp_file.close()
        return True, script_name


    def __checkMessage(self, message):
        if SESSION_LOGOUT in message:
            print(message)
            self.__is_logged = False
            removeToken(self.work_dir)
        elif CANNOT_CONNECT_SERVER == message:
            print(message +'The server may be terminated. Please make sure the IBM Spectrum Application Center is running and then logon again.')
            self.__is_logged = False
            removeToken(self.work_dir)
        elif TOKEN_IS_DELETED == message:
            print(message +'Please logon again.')
            self.__is_logged = False
        else:
            print(message)


    def __getDownloadResult(self, future, files, destination):

        success, content = future.result()
        if not success:
            self.__checkMessage(content)
        else:
            print('Success to download files: %s to directory: %s' %(files, destination))

        return


    def __getSubmitResult(self, future, func_id, cur_workdir):

        success, content = future.result()
        value = {}
        if success:
            jobid = int(content)
            value['jobid'] = jobid
            value['status'] = 'Send'
            value['output'] = None
            self.__func_d[func_id] = value
            return func_id
        else:
            self.__checkMessage(content)
            shutil.rmtree(cur_workdir)
            return None


    def __waitFinish(self, id, func_id, timeout, cur_workdir):
        is_interrupted = False
        output = {}
        output['jobid'] = id
        output['status'] = 'Send'
        print('Waiting...')

        # To reduce waiting error, use timestamp to calculate
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                success, content = getJobOutput(id, cur_workdir, self.work_dir)
                if success:
                    if content['status'] == 'Done':
                        print('Done.')
                        return content['output']
                    if content['status'] == 'Exit':
                        print('Exit.')
                        return content['message']

                else:
                    self.__checkMessage(content)
                    return None

                time.sleep(self.interval)

            except KeyboardInterrupt:
                is_interrupted = True
                break

        if is_interrupted:
            print('Interrupted. The task will be canceled.')
        else:
            print('Timeout. The task will be canceled.')

        self.__func_d[func_id] = output
        success, content = doAction(str(id), 'kill', self.work_dir)
        # if timeout or interrupted, should always return the func_id after kill it
        return func_id


    def __submit(self, func, *arguments, files = None, block = False, timeout = 60, asynchronous = False):
        if not self.__is_logged:
            print ('Please logon before using this function.')
            return None

        paths = None
        if files is not None:
            if files != '':
                success, content = prepareUpload(files)
                if success:
                    paths = content
                else:
                    print(paths)
                    return None

        func_id = str(uuid.uuid4())

        cur_workdir = os.sep.join([self.work_dir, func_id])
        os.makedirs(cur_workdir)

        script_name = os.sep.join([cur_workdir ,SCRIPT_FILE_NAME])

        success, content = self.__generateScript(script_name, func, *arguments)
        if not success:
            print(content)
            return None

        os.chmod(script_name, 0o744)
        value = {}
        # only for upload file
        if not block and paths is not None and asynchronous:
            if self.__thread_pool is None:
                self.__thread_pool = ThreadPoolExecutor(max_workers=5)
            future_task = self.__thread_pool.submit(submitJob, script_name, paths, self.work_dir, asynchronous)
            future_task.add_done_callback(functools.partial(self.__getSubmitResult, func_id = func_id, cur_workdir = cur_workdir))
            value['status'] = 'uploading'
            print('uploading')
            self.__func_d[func_id] = value
            return func_id

        success, content = submitJob(script_name, paths, self.work_dir, asynchronous)
        if success:
            jobid = int(content)
            if block:
                return self.__waitFinish(jobid, func_id, timeout, cur_workdir)
            else:
                value['jobid'] = jobid
                value['status'] = 'Send'
                value['output'] = None
                self.__func_d[func_id] = value
                return func_id
        else:
            self.__checkMessage(content)
            shutil.rmtree(cur_workdir)
            return None


    def logon(self, username = getpass.getuser(), password = '123456', host = 'localhost', port=8080, isHttps = False):
        """
        Use the specified username/password to log on the specified AC web server.

        Return True if success, otherwise return false.
        """
        # remove old token
        removeToken(self.work_dir)
        # always logon as server may be shutdown or terminated
        success, content = logonAC(username, password, host, port, isHttps, self.work_dir)
        if success:
            self.__is_logged = True
            return True
        else:
            print(content)
            self.__is_logged = False
            return False


    def logout(self):
        """
        Log out from AC web server.
        """
        if self.__is_logged:
            success, content = logoutAC(self.work_dir)
            # no matter success or not, also force logout
            self.__is_logged =False
            removeToken(self.work_dir)
            if self.__thread_pool is not None:
                self.__thread_pool.shutdown()
            if not success:
                print(content)
        else:
            print ('You are not logged yet.' )


    def get(self,id):
        """
        Get the output based on the specified function id (which returned by sub()).

        Return the return value(if any) of function if succeeds, or error string if error found,
          or 'None' if function is uploading/pending/running...

        As this routine returns an indefinite number of values. To avoid number of arguments does not match,
          please use an argument to receive the return value. If no error message is printed, then iterate the output on demand.
        """
        # assume the id is func_id by default
        if id is None:
            print('Input id is null.')
            return None
        try:
            value = self.__func_d[id]
            status = value['status']
            if status == 'Done':
                return  value['output']
            if status == 'Exit':
                print('Task status is %s' % status)
                return value['message']
            if status == 'uploading':
                 print('uploading...')
                 return None

            # if task is not finished, just receive status from the server
            jobid = value['jobid']
            cur_workdir = os.sep.join([self.work_dir , str(id)])
        except Exception as e:
            # no key exists: try to restore data from work_dir
            cur_workdir = os.sep.join([self.work_dir, str(id)])
            is_exists = os.path.exists(cur_workdir)
            if is_exists:
                value = {}
                for root,dirs,files in os.walk(cur_workdir):
                    for file in files:
                        if LSF_ERRPUT_FILE_NAME in file:
                            f = open(os.sep.join([cur_workdir , file]), "rb")
                            content = f.read().decode('utf-8')
                            f.close()
                            value['message'] =  content
                            if len(content) > 0:
                                print('Task status is Exit')
                                value['status'] = 'Exit'
                                self.__func_d[id] = value
                                return value['message']
                        if OUTPUT_FILE_NAME in file:
                            f = open(os.sep.join([cur_workdir , file]), "rb")
                            value['output'] =  dill.load(f)
                            f.close()
                            if len(content) > 0:
                                value['status'] = 'Done'
                                self.__func_d[id] = value
                                return value['output']

            # may be the id is job id.
            for value in self.__func_d.values():
                jobid = value['jobid']
                if id == jobid:
                    status = value['status']
                    if status == 'Done':
                        return  value['output']
                    if status == 'Exit':
                        print('Task status is %s' % status)
                        return value['message']


        # not found: we will send request to the server to recontruct the data
        if not self.__is_logged:
            print('Please logon before using this function.')
            return None

        if not isinstance(jobid,int):
            print('You must use job id when you want to reconstruct the data.')
            return None

        success, content = getJobOutput(jobid, cur_workdir, self.work_dir)
        if success :
            self.__func_d[id] = content
            status = content['status']
            if status == 'Done':
                return content['output']
            elif status == 'Exit':
                print('Task status is %s' % status)
                return content['message']
            else:
                return None
        else:
            self.__checkMessage(content)
            return None


    def download(self, id, files, destination = None, asynchronous = False):
        """
        Download function data files from AC server to the specified destination.
        Currently, only can download files that belong to the specified function id (the files must be the function input/output files).

        Return True if success, otherwise return false.

        Parameters:
        id: The function id.
        files: Only support relative path(eg: a.txt or ./a.txt). To specify multiple files, separate with a comma(,).
        destination: Specify the absolute path. If destination is None, download to work_dir/id.
        asynchronous: Whether downlaod the files your specified asynchronously. Only use with the 'files' parameter specified.

        Examples:
        # Download the fucntion' file a.txt to /tmp/
        >>> lsf.download(id,'a.txt','/tmp/')
        # Download the fucntion' file a.txt and b.txt to work_dir/id
        >>> lsf.download(id,'a.txt,b.txt')
        # Download the fucntion' file a.txt to /tmp/ asynchronously
        >>> lsf.download(id,'a.txt','/tmp/',asynchronous = True)
        # Download the fucntion' file a.txt and b.txt to work_dir/id asynchronously
        >>> lsf.download(id,'a.txt,b.txt',asynchronous = True)
        """
        if not self.__is_logged:
            print('Please logon before using this function.')
            return False

        # assume the id is func_id by default
        if id is None:
            print('Input id is null.')
            return False
        try:
            value = self.__func_d[id]
            jobid = value['jobid']
        except Exception as e:
            # rrror found, it may be jobid
            try:
                jobid = int(id)
            except Exception as e:
                print('Invalid id %s is specified, you can specify either funct_id returned by sub/exe or known job id' %id)
                return False


        paths = ''
        if files is None:
            print('Input files is null.')
            return True
        files = files.strip()
        if files == '':
            print('Input files is empty.')
            return True

        filelist = files.split(',')
        for f in filelist:
            f.strip()
            if f.startswith(os.sep):
                print('Invalid file name %s is specified, only support relative path(e.g. a.txt or ./a.txt).'% f)
                return False
            else:
                ff = f.split(os.sep)
                if len(ff) > 2:
                    print('Invalid file name %s is specified, only support relative path(e.g. a.txt or ./a.txt).'% f)
                    return False
                elif len(ff) == 2:
                    if f.startswith('./') or f.startswith('.\\'):
                        paths = paths +  f  + ','
                    else:
                        print('Invalid file name %s is specified, only support relative path(e.g. a.txt or ./a.txt).'% f)
                        return False

                else:
                    paths = paths +  f  + ','


        if len(paths) == '':
            print('Input files %s is null.'% files)
            return False
        else:
            paths = paths[:-1]

        if destination is None:
            destination = os.sep.join([self.work_dir , str(id)])
            if not os.path.exists(destination):
                os.makedirs(destination)
        else:
            if not os.path.exists(destination):
                print('No such file or diectory: %s.'% destination)
                return False
            if os.access(destination, os.W_OK) == 0:
                print('You do not have read permission on the directoy: %s' % destination)
                return False

        if asynchronous:
            if self.__thread_pool is None:
                self.__thread_pool = ThreadPoolExecutor(max_workers=5)

            future_task = self.__thread_pool.submit(downloadFiles, str(jobid), destination, paths, self.work_dir, asynchronous)
            future_task.add_done_callback(functools.partial(self.__getDownloadResult, files = files, destination =destination))
            print('Downloading...')
            return True

        success, content = downloadFiles(str(jobid), destination, paths, self.work_dir, asynchronous)
        if not success:
            self.__checkMessage(content)
            return False
        else:
            return True


    def sub(self, func, *arguments, files = None, asynchronous = False):
        """
        Send function calls(especially for time-consuming) with arguments as jobs to LSF without blocking.

        Return None if error found, otherwise return function id.

        Parameters:
        func: function name.
        arguments: Represents any number of unnamed parameters.
        files: If the function has some dependency files you can upload files by set to the file absolute path
          which will be uploaded from local to server. To specify multiple files, separate with a comma(,).
        asynchronous: Whether upload the files your specified asynchronously. Only use together with the 'files' parameter.

        Examples:
        >>>
        # Submit the 'myfun' function with two arguments 'arg1' and 'arg2' to LSF
        >>> id = lsf.sub(myfun, arg1, arg2)
        >>>
        # Submit the 'myfun' function without arguments but with dependency file '/tmp/a.txt' to LSF
        # then in 'myfun' you can use relative path(eg: a.txt or ./a.txt) to read/write the file
        >>> id = lsf.sub(myfun, files='/tmp/a.txt')
        >>>
        # Submit the 'myfun' function with two arguments 'arg1' and 'arg2' to LSF asynchronously
        >>> id = lsf.sub(myfun, arg1, arg2, asynchronous = True)
        >>>
        # Submit the 'myfun' function without arguments but with dependency file '/tmp/a.txt' to LSF asynchronously
        # then in 'myfun' you can use relative path(eg: a.txt or ./a.txt) to read/write the file
        >>> id = lsf.sub(myfun, files='/tmp/a.txt', asynchronous = True)
        >>>
        """
        return self.__submit(func, *arguments, files=files, block = False, asynchronous = asynchronous)


    def exe(self, func, *arguments, files= None, timeout = 60):
        """
        Send function calls(especially for time-consuming) with arguments as jobs on LSF.
        It will block until job finished/timeout/error found.

        Return the return value(if any) of function if succeeds, or error string if error found,
          or 'None' if function is uploading/pending/running...

        As this routine returns an indefinite number of values. To avoid number of arguments does not match,
          please use an argument to receive the return value. If no error message is printed, then iterate the output on demand.

        Parameters:
        func: function name.
        arguments: Represents any number of unnamed parameters.
        files: If the function has some dependency files you can set files to the file absolute path
                     which will be uploaded from local to server. To specify multiple files, separate with a comma(,).
        timeout(in seconds): If not specified, use timeout = 60. If timeout or press 'CTRL-C', the function will be canceled.

        Examples:
        >>>
        # Execute the 'myfun' function with two arguments 'arg1' and 'arg2'
        >>> output = lsf.exe(myfun, arg1, arg2)
        >>>
        # Execute the 'myfun' function without arguments but with dependency file '/tmp/a.txt'
        # then in 'myfun' you can use relative path(eg: a.txt or ./a.txt) to read/write the file
        >>> output = lsf.exe(myfun, files='/tmp/a.txt')
        >>>
        # Execute and wait 300 seconds
        >>> output = lsf.exe(myfun, arg1, arg2, timeout = 300)
        >>> output = lsf.exe(myfun, files='/tmp/a.txt', timeout = 300)
        """
        return self.__submit(func, *arguments, files=files, block = True, timeout = timeout)


    def cancel(self, id):
        """
        Cancel to the function based on specified function id.

        Return True if success, otherwise return False.
        """
        if not self.__is_logged:
            print('Please logon before using this function.')
            return False

        if id is None:
            print('Input id is null.')
            return False
        try:
            value = self.__func_d[id]
            jobid = value['jobid']
        except Exception as e:
            try:
                jobid = int(id)
            except Exception as e:
                print('Invalid id %s is specified, you can specify either funct_id returned by sub/exe or known job id' %id)
                return False

        success, content = doAction(str(jobid), 'kill', self.work_dir)
        if not success:
            self.__checkMessage(content)

        return success


    def printDict(self,id = None):
        """
        Print diretocy, it is used to debug. If id is not specified, print all.
        """
        if id is None:
            print(self.__func_d)
            return
        else:
            try:
                print(self.__func_d[id])
                return
            except Exception as e:
                # not found, may be jobid
                for value in self.__func_d.values():
                    jobid = value['jobid']
                    if id == jobid:
                        print(value)
                        return

        print('Not found dict for the specified function id %s' %str(id))
        return

if __name__ == "__main__":
    ipython = get_ipython()
    if ipython is None:
        sys.exit('Import Failed. This tool can only be used in IPYTHON context.')
    else:
        lsf = lsf()
        def bsub(func):
            @wraps(func)
            def with_bsub(*arguments, files = None, asynchronous = False):
                return lsf.sub(func,  *arguments, files = files, asynchronous = asynchronous)
            return with_bsub
        def bexe(func):
            @wraps(func)
            def with_bexe(*arguments, files = None, timeout = 60):
                return lsf.exe(func, *arguments, files = files, timeout = timeout)
            return with_bexe