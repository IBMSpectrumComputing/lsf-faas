#!/bin/sh

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


IPYTHON_STARTUP_PATH="$HOME/.ipython/profile_default/startup/"
LSF_FAAS_FILE_NAME="lsf.py"
LSF_FAAS_LIB_FILE_NAME="lsflib.py"

SCRIPT_FILE_PATH=""

usage ()
{
    cat << SETUP_HLP
Usage:  $0 path
        $0 -r
        $0 -h
path    Copy the lsf_faas script files:$LSF_FAAS_FILE_NAME,$LSF_FAAS_LIB_FILE_NAME from the specified path to $IPYTHON_STARTUP_PATH so that 
        this module can be automatically introduced at startup by ipython. Please do NOT copy these files to $IPYTHON_STARTUP_PATH by yourself. 
        Example: $0 src/lsf_faas/
-r      Remove the lsf_faas script files:$LSF_FAAS_FILE_NAME,$LSF_FAAS_LIB_FILE_NAME from $IPYTHON_STARTUP_PATH.
-h      Outputs command usage and exits.

SETUP_HLP

} # usage

if [ $# -ne 1 ]; then
    usage
    exit 0
fi

rFLAG=false

if [ ! -d $IPYTHON_STARTUP_PATH ]; then
    echo "No such file or directory: $IPYTHON_STARTUP_PATH."
    exit 0
fi

while getopts ":hr" opt
do
    case $opt in
        r)
            if [ "$ACTION" != "" ] ; then
                usage
                exit 1
            fi
            rFLAG=true
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            usage
            exit 1
            ;;
    esac
done

shift $[$OPTIND-1]
if [ $# -eq 1 ]; then
    SCRIPT_FILE_PATH=$1

    if [ ! -d $SCRIPT_FILE_PATH ]; then
        echo "No such file or directory: $SCRIPT_FILE_PATH"
        usage
        exit 1
    fi
    FULL_LSF_FAAS_FILE_NAME="$SCRIPT_FILE_PATH/$LSF_FAAS_FILE_NAME"
    FULL_LSF_LIB_FAAS_FILE_NAME="$SCRIPT_FILE_PATH/$LSF_FAAS_LIB_FILE_NAME"
    if [ ! -f $FULL_LSF_FAAS_FILE_NAME ]; then
        echo "No such file or directory:$FULL_LSF_FAAS_FILE_NAME"
        exit 0
    fi
    if [ ! -f $FULL_LSF_LIB_FAAS_FILE_NAME ]; then
        echo "No such file or directory:$FULL_LSF_LIB_FAAS_FILE_NAME"
        exit 0
    fi

    echo "Move $FULL_LSF_FAAS_FILE_NAME and $FULL_LSF_LIB_FAAS_FILE_NAME to $IPYTHON_STARTUP_PATH"
    cp -rf $FULL_LSF_FAAS_FILE_NAME $IPYTHON_STARTUP_PATH
    cp -rf $FULL_LSF_LIB_FAAS_FILE_NAME $IPYTHON_STARTUP_PATH

    echo "Modify $IPYTHON_STARTUP_PATH$LSF_FAAS_FILE_NAME"
    sed -i 's/lsf_faas\.//g' $IPYTHON_STARTUP_PATH$LSF_FAAS_FILE_NAME
    echo "Success"
    exit 0
fi

if [ $rFLAG = true ] ; then
    echo "Remove file: $IPYTHON_STARTUP_PATH$LSF_FAAS_FILE_NAME"
    rm -rf $IPYTHON_STARTUP_PATH$LSF_FAAS_FILE_NAME
    echo "Remove file: $IPYTHON_STARTUP_PATH$LSF_FAAS_LIB_FILE_NAME"
    rm -rf $IPYTHON_STARTUP_PATH$LSF_FAAS_LIB_FILE_NAME
    echo "Success."
    exit 0

else
    usage
    exit 1
fi

