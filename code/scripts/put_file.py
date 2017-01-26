#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, fileinput, errno, datetime, commands, re, string, envvars, time,getpass
import shutil
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE
from os import listdir
from os.path import isfile, join

def main():
    global return_code
    return_code = 0
    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("put_file.py             -> Started    : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    table,field,field_type,field_rdbms_format,field_hadoop_format,file_reg,upper_bound,common_properties,app,sub_app,env,env_ver,group,ingest_type = arg_handle()
    print "field_type=" , field_type
    

    # Get envvars from oozie_common_properties file
    envvars.populate(env,env_ver,app,sub_app)   
    
    #Get Final Properties final name and path from variables
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_' + app.replace("/","_") + '_' + table + '.properties'
    

    # Remove if the file exists
    silentremove(final_properties)
    
    # open the final properties file in write append mode
    properties_file = open(final_properties, 'wb')
    if 'uzip' in ingest_type:
       unzip_cmd = 'unzip  "'+envvars.list['lfs_app_data'] + '/'+file_reg+'" -d "'+envvars.list['lfs_app_data'] + '/'+file_reg.rsplit('/',1)[0]+'/"'
       print("put_file.py             -> Command : "+unzip_cmd)
       sys.stdout.flush()
       rc, out = commands.getstatusoutput(unzip_cmd)
       if rc == 0:
          if rc == 0:
             print("put_file.py             -> GZIP command successful ")
             rm_cmd = 'rm '+envvars.list['lfs_app_data'] + '/'+file_reg
             rc, out = commands.getstatusoutput(rm_cmd)
             if rc!=0:
               print("put_file.py             -> File remove command failed" + str(out))
               sys.exit(1)
             else:
               print("put_file.py             -> Removed Files :"+envvars.list['lfs_app_data'] + '/'+file_reg)
          else:
             print("put_file.py             -> GZIP command failed" + str(out))
             sys.exit(1)
       elif rc == 2304:
         print("put_file.py             -> No Files to be processed RC:" + str(rc))
         print(str(out))
       else:
         print("put_file.py             -> "+unzip_cmd+" command failed RC:" + str(rc))
         print(str(out))
         sys.exit(1)
    if 'rmfl' in ingest_type:
       rmfl_cmd = 'rm  '+envvars.list['lfs_app_data'] + '/'+file_reg
       print("put_file.py             -> Command : "+rmfl_cmd)
       sys.stdout.flush()
       rc, out = commands.getstatusoutput(rmfl_cmd)
       if rc == 0:
          if rc == 0:
             print("put_file.py             -> GZIP command successful ")
          else:
             print("put_file.py             -> GZIP command failed" + str(out))
             sys.exit(1)
       elif rc == 256:
         print("put_file.py             -> No Files to be processed RC:" + str(rc))
         print(str(out))
       else:
         print("put_file.py             -> "+rmfl_cmd+" command failed RC:" + str(rc))
         print(str(out))
         sys.exit(1)
    sys.stdout.flush()
    if 'gzip' in ingest_type:
       gz_cmd = "gzip  "+envvars.list['lfs_app_data'] + '/'+file_reg
       rc, out = commands.getstatusoutput(gz_cmd)
       if rc == 0:
          if rc == 0:
             print("put_file.py             -> GZIP command successful ")
             #Removing txt is not necessary as GZIPping will remove them
             #rm_cmd = 'rm '+envvars.list['lfs_app_data'] + '/'+file_reg
             #rc, out = commands.getstatusoutput(rm_cmd)
             #if rc!=0:
             #  print("put_file.py             -> File remove command failed" + str(out))
             #  sys.exit(1)
             #else:
             #  print("put_file.py             -> Removed Files :"+envvars.list['lfs_app_data'] + '/'+file_reg)
          else:
             print("put_file.py             -> GZIP command failed" + str(out))
             sys.exit(1)
       elif rc == 256:
         print("put_file.py             -> No Files to be processed RC:" + str(rc))
         print(str(out))
       else:
         print("put_file.py             -> "+gz_cmd+" command failed RC:" + str(rc))
         print(str(out))
         sys.exit(1)
    if 'bzip' in ingest_type:
       bz_cmd = "bzip2  "+envvars.list['lfs_app_data'] + '/'+file_reg
       rc, out = commands.getstatusoutput(bz_cmd)
       if rc == 0:
          if rc == 0:
             print("put_file.py             -> GZIP command successful ")
             #Removing txt is not necessary as GZIPping will remove them
             #rm_cmd = 'rm '+envvars.list['lfs_app_data'] + '/'+file_reg
             #rc, out = commands.getstatusoutput(rm_cmd)
             #if rc!=0:
             #  print("put_file.py             -> File remove command failed" + str(out))
             #  sys.exit(1)
             #else:
             #  print("put_file.py             -> Removed Files :"+envvars.list['lfs_app_data'] + '/'+file_reg)
          else:
             print("put_file.py             -> GZIP command failed" + str(out))
             sys.exit(1)
       elif rc == 256:
         print("put_file.py             -> No Files to be processed RC:" + str(rc))
         print(str(out))
       else:
         print("put_file.py             -> "+bz_cmd+" command failed RC:" + str(rc))
         print(str(out))
         sys.exit(1)
    sys.stdout.flush()
    if 'rplc' in ingest_type:
       hdfs_loc = envvars.list['hdfs_str_raw'] + '/'+envvars.list['hv_db_'+app+'_'+sub_app+'_stage']+'/'+table+'/'
       put_cmd = 'hdfs dfs -put -f  '+envvars.list['lfs_app_data'] + '/'+file_reg+' '+ hdfs_loc
       rc, out = commands.getstatusoutput(put_cmd)
       if rc == 0:
          print("put_file.py             -> Command Sucessful: "+put_cmd)
          rm_cmd = 'rm '+envvars.list['lfs_app_data'] + '/'+file_reg
          rc, out = commands.getstatusoutput(rm_cmd)
          if rc!=0:
            print("put_file.py             -> File remove command failed" + str(out))
            sys.exit(1)
          else:
            print("put_file.py             -> Removed File :"+envvars.list['lfs_app_data'] + '/'+file_reg)
       elif rc == 256:
         print("put_file.py             -> No Files to be processed RC:" + str(rc))
         print(str(out))
       else:
         print("put_file.py             -> HDFS PUT command failed RC:" + str(rc))
         print(str(out))
         sys.exit(1)
       sys.stdout.flush()
    if 'extp' in ingest_type:
        try:
           quarter_offset = file_reg.replace('{mm}','mm').replace('{YYYY}','YYYY').index('{q}')
        except ValueError:
           quarter_offset = -1
        try:
           year_offset   = file_reg.replace('{mm}','mm').replace('{q}','q').index('{YYYY}')
        except ValueError:
           year_offset = -1
        try:
           month_offset   = file_reg.replace('{YYYY}','YYYY').replace('{q}','q').index('{mm}')
        except ValueError:
           month_offset = -1
        file_reg = file_reg.replace('.','\.')
        file_reg = file_reg.replace('{q}','[1-4]{1}')
        file_reg = file_reg.replace('{YYYY}','[0-9]{4}')
        file_reg = file_reg.replace('{mm}','[0-9]{2}')
        prog = re.compile(file_reg)
        lfs_data = envvars.list['lfs_app_data'] + '/'+table+'/'
        hdfs_data = envvars.list['hdfs_str_raw'] + '/'+table+'/'
        year = ""
        month = ""
        quarter = ""
        for f in listdir(lfs_data):
           if isfile(join(lfs_data, f)):
              if prog.match(f):
                 hdfs_path = hdfs_data
                 if year_offset != -1:
                    year = f[year_offset:4]
                    hdfs_path = hdfs_path + "year="+str(f)[year_offset:year_offset+4]+"/"
                 if month_offset != -1:
                    month = f[month_offset:2]
                    hdfs_path = hdfs_path +"month="+str(f)[month_offset:month_offset+2]+"/"
                 if quarter_offset != -1:
                    quarter = f[quarter_offset:1]
                    hdfs_path = hdfs_path + "quarter="+str(f)[quarter_offset:quarter_offset+1]+"/" 
                 put_command = "hdfs dfs -put -f "+ join(lfs_data, f) + " " + hdfs_path
                 print(put_command)
                 rc, out = commands.getstatusoutput(put_command)
                 if rc == 0:
                  rm_command = "rm " + join(lfs_data, f)
                  #rc, out = commands.getstatusoutput(rm_command)
                 else:
                   print("hdfs command failed" + str(out))
                   sys.exit(1)

    sys.stdout.flush()                     

    print("put_file.py             -> Ended      : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    print "put_file.py             -> Return-Code:" + str(return_code)
    print start_line
    sys.exit(return_code)



def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured


def arg_handle():
    usage = "usage: put_file.py   [options]"
    parser = OptionParser(usage)
    parser.add_option("-i", "--op0",dest="ingest_type",
                      help="ingest type")
    parser.add_option("-t", "--op1", dest="table",
              help="environment name")
    parser.add_option("-f", "--op2",dest="field",
                      help="increment field")
    parser.add_option("-l", "--op3",dest="file_reg",
                      help="increment field min bound")
    parser.add_option("-u", "--op4",dest="upper_bound",
                      help="increment field max bound") 
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("--cmmn_dt", dest="common_date",
                  help="application name")
    parser.add_option("-s", "--subapp", dest="sub_app",
                  help="application name")
    parser.add_option("-e", "--env", dest="env",
              help="environment name")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-g", "--group", dest="group",
              help="environment name")

    (options, args) = parser.parse_args()
    print("put_file.py             -> Input      : " + str(options)) 
    if  options.table == "":
        parser.error("Argument, table_name, is required.")
        return_code = 10
        sys.exit(return_code)
    table = options.table.lower()
    field = options.field
    field_name_type_fmt = None
    field_name = None
    field_type = None
    field_rdbms_format=None
    field_hadoop_format=None
    field_delimiter = "#"
    if field is not None:
       field = field.replace('\s',' ')
       field_name_type_fmt=field.split(field_delimiter)
       field_name=field_name_type_fmt[0]
       field_type=""
       if len(field_name_type_fmt) >=2:
          field_type=field_name_type_fmt[1]
       if len(field_name_type_fmt) >=3:
          field_rdbms_format=field_name_type_fmt[2]
       if len(field_name_type_fmt) >=4:
          field_hadoop_format=field_name_type_fmt[3]
    source = '/cloudera_nfs1/config/oozie_global.properties'
    upper_bound = options.upper_bound
    if upper_bound is not None:
       upper_bound = upper_bound.replace('\s',' ')
    group = options.group
    abc_line = "|".join([group,"put_file.py  ","python","run_job.py",str(table),str(options),"STARTED",
                         getpass.getuser(),"run_ingest started..",str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush()
    return table,field_name,field_type,field_rdbms_format,field_hadoop_format,options.file_reg.strip(),upper_bound,source,options.app,options.sub_app,options.env,options.env_ver,group, options.ingest_type.lower()

if __name__ == "__main__":
    main()
