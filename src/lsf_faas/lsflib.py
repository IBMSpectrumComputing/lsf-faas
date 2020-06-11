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

import base64
import dill
import getopt
import http.client as httplib
import httplib2
import locale
import os
import re
import sys
import urllib
import urllib.request as urllib2
from xml.dom import minidom
from xml.etree import ElementTree as ET
from xml.parsers.expat import ExpatError


TOKEN_FILE = '.lsfpass'
WORK_DIR_NAME = '.lsf_faas'
MULTIPLE_ACCEPT_TYPE = 'text/plain,application/xml,text/xml,multipart/mixed'
ERROR_STRING = 'errMsg'
ERROR_TAG = '<' + ERROR_STRING + '>'
ACTION_STRING = 'actionMsg'
ACTION_TAG = '<' + ACTION_STRING + '>'

SCRIPT_FILE_NAME = 'lsf_faas.py'
OUTPUT_FILE_NAME = 'output.out'
LSF_OUTPUT_FILE_NAME = 'lsf.output'
LSF_ERRPUT_FILE_NAME = 'lsf.errput'
SESSION_LOGOUT = 'Your current login session was logout'
CANNOT_CONNECT_SERVER = 'Cannot connect to the server.'
TOKEN_IS_DELETED = 'Your token is empty or was deleted.'



def checkField(field):
    if field != None:
        if field.text == None :
            field = ''
        else:
            field = field.text
    else:
        field='-'

    return field


def prepareUpload(upload_files):
    cwd = os.getcwd()
    files = upload_files.split(',')
    paths = ''
    p = re.compile('^[a-zA-Z]:[/\\][\w\W]+')

    totalSize = 0
    for f in files:
        f.strip()
        if len(f) > 0:
            if ((os.sep != f[0]) and (p.match(f.lower()) == None)):
                f = os.sep.join([cwd ,f])
            if not os.path.isfile(f):
                return False, 'The specified file does not exist: %s' % f
            elif os.access(f, os.R_OK) == 0:
                return False, 'No read permission for the file: %s' % f
            else:
                totalSize += os.path.getsize(f)
                paths = paths + f + ','

    if len(paths) <= 0:
        return False, 'The specified file does not exist: %s '% upload_files
    else:
        paths = paths[:-1]

    if (totalSize > 536870912):
        return False, 'Total file size is greater than 500MB. Files cannot be uploaded.'
    else:
        return True, paths


def getHttp(url, work_dir, timeout=5):

    is_https = False
    if ( (len(url) != 0) & ('https' in url.lower())):
        is_https = True

    if is_https == True:
        pem_file= os.sep.join([work_dir , 'cacert.pem'])
        if os.path.isfile(pem_file):
            if timeout is None:
                return httplib2.Http(ca_certs = pem_file)
            else:
                return httplib2.Http(ca_certs = pem_file, timeout = timeout)
        else:
            raise Exception('The https certificate \'cacert.pem\' is missing. Please copy the \'cacert.pem\' file from the GUI_CONFDIR/https/cacert.pem on the IBM Spectrum Application Center to %s.' % work_dir)

    if timeout is None:
        return  httplib2.Http()
    else:
        return  httplib2.Http(timeout = timeout)



def saveToken(url, token, jtoken, work_dir):

    if len(jtoken) > 0:
        token = token + ",JSESSIONID=" + jtoken[0].childNodes[0].nodeValue

    fpath = os.sep.join([work_dir , TOKEN_FILE])
    try:
        f = open(fpath, "w")
    except IOError as e:
        raise  Exception('Cannot open file "%s": %s' % (fpath, str(e)))
    else:
        f.write(url)
        f.write('\n')
        f.write(token)
        f.close()

def getToken(work_dir):
    token = ''
    url = ''
    fpath = os.sep.join([work_dir, TOKEN_FILE])

    try:
        f = open(fpath, "r")
        url_token = f.read().split('\n')
        f.close()
        url = url_token[0]
        token = url_token[1].replace('"', '#quote#')
        if len(token) <= 0:
            return url, ''
        else:
            return url, 'platform_token='+token
    except IOError:
        return url, token
    except Exception as e:
        return url, token


def removeToken(work_dir):

    fpath = os.sep.join([work_dir , TOKEN_FILE])

    if (os.path.exists(fpath)):
        os.remove(fpath)


def doAction(jobId, action, work_dir):

    url, token = getToken(work_dir)
    if token == '':
        return False, TOKEN_IS_DELETED

    try:
        http = getHttp(url, work_dir)
    except Exception as e:
        return False, str(e)

    headers = {'Content-Type': 'text/plain', 'Cookie': token, 'Accept': 'application/xml', 'Accept-Language': 'en-us'}
    try:
        response, content = http.request(url + 'webservice/pacclient/jobOperation/' + action +'/' + jobId, 'GET', headers=headers)
    except Exception as e:
        return False, CANNOT_CONNECT_SERVER

    try:
        content = content.decode('utf-8')
    except Exception as e:
        return False, 'Failed to decode content "%s": %s' % (content, str(e))

    try:
        if response['status'] == '200':
            xdoc = minidom.parseString(content)

            if ERROR_TAG in content:
                err_tag = xdoc.getElementsByTagName(ERROR_STRING)
                return False, err_tag[0].childNodes[0].nodeValue
            elif ACTION_TAG in content:
                action_tag = xdoc.getElementsByTagName(ACTION_STRING)
                return True, action_tag[0].childNodes[0].nodeValue
            else:
                return False, 'Failed to %s the task' % action
        else:
            return False, 'Failed to %s the task' % action
    except Exception as e:
        return False, 'Failed to parse content: %s' % str(e)


def downloadFiles(jobId, destination, files, work_dir, asynchronous = False):

    url,token = getToken(work_dir)
    if token == '':
        return False, TOKEN_IS_DELETED
    try:
        if asynchronous:
            http = getHttp(url, work_dir, timeout = None)
        else:
            http = getHttp(url, work_dir)
    except Exception as e:
        return False, str(e)

    body = os.path.basename(files)

    headers = {'Content-Type': 'text/plain', 'Cookie': token, 'Accept': MULTIPLE_ACCEPT_TYPE, 'Accept-Language': 'en-us'}
    try:
        response, content = http.request( url + 'webservice/pacclient/file/' + jobId, 'GET', body = body, headers = headers)
    except Exception as e:
        return False, CANNOT_CONNECT_SERVER

    if len(content) <= 0:
        if response['status'] == '404':
            return False, 'Failed to download the file. The specified file does not exist: ' + body
        # when SESSION_LOGOUT, AC also return error code 403. so here no way to get the real reason.
        # may be let user logout.
        elif response['status'] == '403':
            return False, 'Failed to download the file. Permmsin denied: ' + body
        else:
            return False, 'Failed to download the file: ' + body
    else:
        try:
            content = content.decode('utf-8')
        except Exception as e:
            try:
                # no need decode here
                parseDownloadContentBytes(destination, content)
                return True, ''
            except Exception as e:
                return False, 'Failed to parse downloaded content: %s' % str(e)
        try:
            parseDownloadContentString(destination, content)
            return True, ''
        except Exception as e:
            return False, 'Failed to parse downloaded content: %s' % str(e)

def parseDownloadContentBytes(destination, content):
    boundary = content.split(b"\n")[0].strip()
    if b'--' not in boundary:
        boundary = content.split(b"\n")[1].strip()

    file_sections = content.split(boundary)
    file_number = len(file_sections) - 1

    for sections in file_sections:
        # if has Content-ID in this section, it means a file
        if b'Content-ID:' in sections:
            # get the file name
            data_list = sections.split(b"Content-ID: ")
            filename = data_list[1][1:data_list[1].index(b">")]
            filename = os.path.basename(str(filename,'utf-8'))
            fname = os.sep.join([destination , filename])

            lengths = len(sections)
            start = sections.index(b">") + 5
            end = lengths
            if file_number > 1:
                end = lengths - 2
            data = sections[start : end]

            try:
                f = open(fname,'wb')
                f.write(data.decode('utf-8'))
                f.close()
            except Exception as e:
                f = open(fname,'wb')
                f.write(data)
                f.close()


def parseDownloadContentString(destination, content):
    boundary = content.split("\n")[0].strip()
    if '--' not in boundary:
        boundary = content.split("\n")[1].strip()

    file_sections = content.split(boundary)
    file_number = len(file_sections) - 1

    for sections in file_sections:
        # if has Content-ID in this section, it means a file
        if 'Content-ID:' in sections:
            # get the file name
            data_list = sections.split("Content-ID: ")
            filename = data_list[1][1:data_list[1].index(">")]
            filename = os.path.basename(filename)
            fname = os.sep.join([destination , filename])

            # get the file content
            lengths = len(sections)
            start = sections.index(">") + 5
            end = lengths
            if file_number > 1:
                end = lengths - 2
            data = sections[start : end]

            if OUTPUT_FILE_NAME in fname:
                f = open(fname,'wb')
                # encode data as it received bytes
                orig_bytes = data.encode('utf-8')
                f.write(base64.b64decode(orig_bytes))
            else:
                try:
                    f = open(fname,'wb')
                    f.write(data.encode('utf-8'))
                except Exception as e:
                    f = open(fname,'w')
                    f.write(data)

            f.close()


def logonAC(username, password, host, port, isHttps, work_dir):

    if isHttps:
        url='https://' + host + ':' + str(port) + '/platform/'
    else:
        url='http://' + host + ':' + str(port) + '/platform/'

    password = password.replace("&", "&amp;")
    password = password.replace("<", "&lt;")
    password = password.replace(">", "&gt;")

    try:
        http = getHttp(url, work_dir)
    except Exception as e:
        return False, str(e)

    url_check, token = getToken(work_dir)
    if ( (url_check != url) | (False == token.startswith("platform_token=" + username + "#quote#")) ):
        token = "platform_token="
    headers = {'Content-Type': 'application/xml', 'Cookie': token, 'Accept': MULTIPLE_ACCEPT_TYPE, 'Accept-Language': 'en-us'}
    body = '<User><name>%s</name> <pass>%s</pass> </User>' % (username, password)

    try:
        response, content = http.request(url + 'webservice/pacclient/logon/', 'GET', body=body, headers=headers)
    except Exception as e:
        return False, 'Failed to log on the server "%s": %s' % (host, str(e))
    try:
        content = content.decode('utf-8')
    except Exception as e:
        return False, 'Failed to decode the content "%s": %s' % (content, str(e))

    if response['status'] == '200':
        xdoc = minidom.parseString(content)
        tk = xdoc.getElementsByTagName("token")
        jtk = xdoc.getElementsByTagName("jtoken")

        if len(tk) > 0:
            #You have logged on to as: {0} username)
            try:
                saveToken(url, tk[0].childNodes[0].nodeValue,jtk, work_dir)
            except Exception as e:
                return False, str(e)

            return True, 'You have logged on.'
        else:
            err_tag = xdoc.getElementsByTagName("errMsg")
            return False, err_tag[0].childNodes[0].nodeValue

    else:
        return False, 'Failed to logon the server "%s".' % host


def logoutAC(work_dir):
    url, token = getToken(work_dir)
    if token == '':
        return False, TOKEN_IS_DELETED
    if (len(token) <= 0):
        return True,'You are not logged yet.'

    try:
        http = getHttp(url, work_dir)
    except Exception as e:
        return False, str(e)

    url_logout= url + 'webservice/pacclient/logout/'
    headers = {'Content-Type': 'text/plain', 'Cookie': token, 'Accept': MULTIPLE_ACCEPT_TYPE, 'Accept-Language': 'en-us'}
    try:
        response, content = http.request(url_logout, 'GET', headers = headers)

    except Exception as e:
        return False, CANNOT_CONNECT_SERVER
    try:
        content = content.decode('utf-8')
    except Exception as e:
        return False, 'Failed to decode content "%s": %s' % (content, str(e))

    if response['status'] == '200':
        if content == 'ok':
            return True, 'You have logout successfully.'
        else:
            return False, content
    else:
        return False, CANNOT_CONNECT_SERVER


def verifyToken(work_dir):
    return getJobs('', work_dir)


def getJobs(parameter, work_dir):
    url, token = getToken(work_dir)
    if token == '':
        return False, TOKEN_IS_DELETED
    try:
        http = getHttp(url, work_dir)
    except Exception as e:
        return False, str(e)

    headers = {'Content-Type': 'application/xml', 'Cookie': token, 'Accept': MULTIPLE_ACCEPT_TYPE, 'Accept-Language': 'en-us'}
    try:
        response, content = http.request(url + 'webservice/pacclient/jobs?' + parameter, 'GET', headers = headers)
    except Exception as e:
        return False, CANNOT_CONNECT_SERVER

    try:
        content = content.decode('utf-8')
    except Exception as e:
        return False, 'Failed to decode content "%s": %s' % (content, str(e))

    if response['status'] == '200':
        xdoc = ET.fromstring(content)
        if ERROR_TAG in content:
            tree = xdoc.getiterator("Jobs")
            for xdoc in tree:
                error = xdoc.find(ERROR_STRING)
            return False, checkField(error)
        elif 'note' in content:
            tree = xdoc.getiterator("Jobs")
            for xdoc in tree:
                note=xdoc.find('note')
            return False, checkField(note)
        else:
            return True, content
    else:
        return False, CANNOT_CONNECT_SERVER


def submitJob(scriptname, files, work_dir, asynchronous):
    params = {}
    params['COMMANDTORUN'] = 'python3 ' + SCRIPT_FILE_NAME
    params['ERROR_FILE'] = './' + LSF_ERRPUT_FILE_NAME
    params['OUTPUT_FILE'] = './' + LSF_OUTPUT_FILE_NAME

    input_files={}
    input_files['INPUT_FILE'] = scriptname + ',upload'

    if files is not None:
        paths = files.split(',')
        i = 0
        for path in paths:
            input_files[ str(i) + 'INPUT_FILE']= path + ',upload'
            i += 1

    url, token = getToken(work_dir)
    if token == '':
        return False, TOKEN_IS_DELETED

    boundary = '_lsf_faas_boundary'
    try:
        if asynchronous:
            http = getHttp(url, work_dir, timeout = None)
        else:
            http = getHttp(url, work_dir)
        body = encodeBody(boundary, 'generic', params, input_files)
    except Exception as e:
        return False, str(e)

    headers = {'Content-Type': 'multipart/mixed; boundary='+boundary,
                   'Accept': 'text/xml,application/xml;', 'Cookie': token,
                   'Content-Length': str(len(body)), 'Accept-Language': 'en-us'}

    try:
        response, content = http.request(url + 'webservice/pacclient/submitapp', 'POST', body = body, headers = headers)
    except Exception as e:
        return False, CANNOT_CONNECT_SERVER
    try:
        content = content.decode('utf-8')
    except Exception as e:
        return False, 'Failed to decode content "%s": %s' % (content, str(e))

    if response['status'] == '200':
        xdoc = minidom.parseString(content)
        if ERROR_TAG not in content:
            id_tag = xdoc.getElementsByTagName("id")
            return True, id_tag[0].childNodes[0].nodeValue
        else:
            err_tag = xdoc.getElementsByTagName(ERROR_STRING)
            return False, err_tag[0].childNodes[0].nodeValue
    else:
        return False, CANNOT_CONNECT_SERVER


# in python-3.x:
# str.joinReturn a string which is the concatenation of the strings in the iterable iterable.
# a TypeError will be raised if there are any non-string values in iterable, including bytes objects.
def encodeBody(boundary, appName, params, input_files):

    boundary2 = '_lsf_faas_file_boundary'
    def encodeAppname():
        return (  ('--' + boundary).encode('utf-8'),
        'Content-Disposition: form-data; name="AppName"'.encode('utf-8'),
        'Content-ID: <AppName>'.encode('utf-8'),
        ''.encode('utf-8'),
        appName.encode('utf-8'))

    def encodeHead():
        return( ('--' + boundary).encode('utf-8'),
        'Content-Disposition: form-data; name="data"'.encode('utf-8'),
        ('Content-Type: multipart/mixed; boundary='+ boundary2).encode('utf-8'),
        ('Accept-Language: en-us').encode('utf-8'),
        'Content-ID: <data>'.encode('utf-8'), ''.encode('utf-8'))

    def encodeParam(param_name):
        return( ('--' + boundary2).encode('utf-8'),
        ('Content-Disposition: form-data; name="%s"' % param_name).encode('utf-8'),
        'Content-Type: application/xml; charset=UTF-8'.encode('utf-8'),
        'Content-Transfer-Encoding: 8bit'.encode('utf-8'),
        ('Accept-Language: en-us').encode('utf-8'),
        ''.encode('utf-8'),
        ('<AppParam><id>%s</id><value>%s</value><type></type></AppParam>' %(param_name, params[param_name])).encode('utf-8') )

    def encodeFileParam(param_name, param_value):
        return( ('--' + boundary2).encode('utf-8'),
        ('Content-Disposition: form-data; name="%s"' % param_name).encode('utf-8'),
        'Content-Type: application/xml; charset=UTF-8'.encode('utf-8'),
        'Content-Transfer-Encoding: 8bit'.encode('utf-8'),
        ('Accept-Language: en-us').encode('utf-8'),
        ''.encode('utf-8'),
        ('<AppParam><id>%s</id><value>%s</value><type>file</type></AppParam>' %(param_name, param_value)).encode('utf-8'))

    def encodeFile(file_path, filename):
        f= open(file_path, 'rb')
        content = f.read ()
        f.close()
        return ( ('--' + boundary).encode('utf-8'),
            ('Content-Disposition: form-data; name="%s"; filename="%s"' %(filename, filename)).encode('utf-8'),
            'Content-Type: application/octet-stream'.encode('utf-8'),
            'Content-Transfer-Encoding: binary'.encode('utf-8'),
            ('Accept-Language: en-us').encode('utf-8'),
            ('Content-ID: <%s>' % urllib2.quote(filename)).encode('utf-8'),
            ''.encode('utf-8'),
            content )

    lines = []
    upload_type = ''
    flag = False
    lines.extend(encodeAppname())
    lines.extend(encodeHead())
    for name in params:
        lines.extend (encodeParam(name))

    file_number = 0
    for name in input_files:
        value = input_files[name]
        value_string = input_files[name]
        # specify multiple files
        value_list = value_string.split('#')
        filepathList = ''
        for value in value_list:
            if ',' in value:
                try:
                    upload_type = value.split(',')[1]
                    if (upload_type == 'link') | (upload_type == 'copy') | (upload_type == 'path'):
                        if upload_type == 'copy':
                            pass
                    else:
                        flag = True
                        value = value.replace('\\', '/').split('/').pop()
                        file_number = file_number + 1
                    if filepathList == '':
                        filepathList = value
                    else:
                        filepathList += ';' + value

                except IndexError:
                    return
            else:
                return
        lines.extend (encodeFileParam(name, filepathList))


    lines.extend (( ('--%s--' % boundary2).encode('utf-8'), ''.encode('utf-8')))

    if flag:
        for name in input_files:
            value_string = input_files[name]
            # specify multiple files
            value_list = value_string.split('#')
            for value in value_list:
                if ',' in value:
                    upload_type = value.split(',')[1]
                    file_path = value.split(',')[0]
                    if upload_type == 'upload':
                        filename = file_path.replace('\\', '/').split('/').pop()
                        try:
                            lines.extend(encodeFile(file_path, filename))
                        except IOError:
                            raise Exception('Submit job failed, No such file or directory: %s' % file_path)

    lines.extend (( ('--%s--' % boundary).encode('utf-8'), ''.encode('utf-8')))
    return b'\r\n'.join (lines)


def getJobOutput(id, cur_work_dir, work_dir):
    value = {}

    try:
        if not os.path.exists(cur_work_dir):
            os.makedirs(cur_work_dir)

        value['jobid'] = id
        value['output'] = ''
        value['message'] = ''
        success, content = getJobs('id=' +str(id), work_dir)
        if success:
            tree = ET.fromstring(content)
            jobs = tree.getiterator("Job")
            for xdoc in jobs:
                status = checkField(xdoc.find('status'))
                value['status'] = status
                if status == 'Done' or status == 'Exit':
                    # assume the output of the task is not too big, so download files synchronously
                    success, content = downloadFiles(str(id), cur_work_dir, OUTPUT_FILE_NAME + ',' + LSF_ERRPUT_FILE_NAME, work_dir)
                    if not success:
                        return False, content
                else:
                    pass

                for root,dirs,files in os.walk(cur_work_dir):
                    for file in files:
                        if LSF_ERRPUT_FILE_NAME in file:
                            f = open(os.sep.join([cur_work_dir, file]), "rb")
                            content = f.read().decode('utf-8')
                            f.close()
                            value['message'] =  content
                        if OUTPUT_FILE_NAME in file:
                            f = open(os.sep.join([cur_work_dir, file]), "rb")
                            value['output'] = dill.load(f)
                            f.close()

            return True, value
        else:
            return False, content
    except Exception as e:
        return False, str(e)

