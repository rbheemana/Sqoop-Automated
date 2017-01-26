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
    usage = "usage: run_recursive.py grp_name app sub_app jobnames.list"
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
    grp_names = grp_name.split(",")
    command = ""
    for grp_name in grp_names:
        command = " ".join([command,"python /cloudera_nfs1/code/scripts/run_job.py " + grp_name + " " +app+" "+sub_app+" " + as_of_date+" &"])
    command = command + " wait "
    #print command                   
    rc, user_name = commands.getstatusoutput(command) 

if __name__ == "__main__":
    main()
