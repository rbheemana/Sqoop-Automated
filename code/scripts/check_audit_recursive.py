#!/usr/bin/python

# Purpose: This Job is the starting point of all jobs. 
#          Looks up jobnames.list and calls the script

import sys, os, commands, envvars, datetime, time ,getpass, errno
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE


def main():
    global return_code, msck_count, msck_command
    return_code = 0
    msck_count = 0
    home = '/data/'
    path = os.path.dirname(os.path.realpath(__file__))
    root = path.split('src/scripts')[0]

    env = 'p'
    env_ver = '01'
    usage = "usage: check_recursive.py grp_name app sub_app jobnames.list"
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()
    if len(args) < 3:
        parser.error("Arguments - group_job_name and app name are required.")
    global app, sub_app
    grp_name = args[0]
    app = args[1]
    sub_app = args[2]
    jobnames = "jobnames.list"
    as_of_date = str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S.%f'))
    recursive = "python /cloudera_nfs1/code/scripts/run_recursive.py "+grp_name+ " " +app + " "+sub_app
    command = "ps -ef|grep '"+recursive+"'"    
    rc, process = commands.getstatusoutput(command)
    print (process+ "\nLength:"+str(len(process.split('\n'))))
    if len(process.split('\n')) < 3:
       print "Process is not running, re-triggering the command"
       os.system(recursive+"&")
    else:
       print "Process is running"

if __name__ == "__main__":
    main()
