#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, fileinput, errno, commands, re, string, envvars, time,getpass
import shutil
from datetime import datetime
from datetime import timedelta
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE

def main():
    global return_code
    return_code = 0
    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("run_hive_query.py           -> Started    : " + datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    options = arg_handle()
    table   = options.table.strip().lower()
    app     = options.app.lower()
    env     = options.env.lower()
    env_ver = options.env_ver.lower()
    sub_app = options.sub_app.lower()
    query   = ""
    query_type = options.query_type.strip().lower()
    if query_type == "cstm":
       query = options.query.strip()
    group   = options.group
    common_properties = '/cloudera_nfs1/config/oozie_global.properties'
    # Get envvars from oozie_common_properties file
    envvars.populate(env,env_ver,app,sub_app)   
    hdfs_load_ts = "'" + str(options.common_date).replace("_"," ") +"'"
    common_date_tfmt = datetime.strptime(options.common_date,'%Y-%m-%d_%H:%M:%S.%f')
    log_time = common_date_tfmt.strftime('%Y-%m-%d_%H-%M-%S')
    log_date = common_date_tfmt.strftime('%Y-%m-%d')
    log_folder = envvars.list['lfs_app_logs'] + "/"+log_date
    log_file = log_folder +"/run_job-" + group + '_' + log_time  + '.log'
    #Get Final Properties final name and path from variables
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_hive_query_' + app.replace("/","_") + '_' + table + '.properties'
    

    # Remove if the file exists
    silentremove(final_properties)
    
    # open the final properties file in write append mode
    properties_file = open(final_properties, 'wb')

    # Build the table properties file name and path from variables run_ingest only calls wf_db_ingest workflow
    table_properties = envvars.list['lfs_app_workflows'] + '/wf_db_ingest/' + table + '.properties'
    # get time stamp to load the table
    hdfs_load_ts = "'" + str(datetime.now()) +"'"
    envvars.list['hdfs_load_ts'] = hdfs_load_ts

    #load evironment variables for app specific
    if os.path.isfile(table_properties):
       envvars.load_file(table_properties) 
      
    #  Concatenate global properties file and table properties file
    shutil.copyfileobj(open(common_properties, 'rb'), properties_file)
    if os.path.isfile(table_properties):
       shutil.copyfileobj(open(table_properties, 'rb'), properties_file)
       #Get Databese name from environment variables
       db = envvars.list['hv_db']
       table = envvars.list['hv_table']
    else:
       db = envvars.list['hv_db_'+app+'_'+sub_app]
       table = table_properties

    sys.stdout.flush()

    curr_date = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d") 
    prev_load_date = (datetime.fromtimestamp(time.time()) + timedelta(days=-1)).strftime("%Y-%m-%d")
    
    dynamic_properties = '\n'.join(['\nenv=' + env,
                                'app=' + app,
                                'sub_app=' +sub_app,
                                'group=' +group,
                                'prev_load_date=' +prev_load_date,
                                'curr_date='+curr_date,
                                'log_file='+log_file,
                                'happ=' + envvars.list['happ']])
    if query_type == 'mgfl':
        print("run_hive_query.py           -> HiveQueryTyp: Merge small files ")
        if not envvars.list['partition_column'] == '':
           if not (options.ldate is None or options.ldate.strip() == ""):
              partition_clause = "PARTITION ("+envvars.list['partition_column']+"='"+options.ldate.strip()+"')"
              where_clause = " where "+envvars.list['partition_column']+"='"+options.ldate.strip()+"'"
           else:
              partition_clause = "PARTITION ("+envvars.list['partition_column']+"='"+prev_load_date+"')"
              where_clause = " where "+envvars.list['partition_column']+"='"+prev_load_date+"'"
           dynamic_properties = '\n'.join([dynamic_properties,
                                        'hive_query=merge_smfl_table.hql',
                                        'python_script=invalidate_metadata.py',
                                        'partition_clause='+partition_clause,
                                        'where_clause='+where_clause])
           abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table +","
        else:
           print("run_hive_query.py           -> Partition column must be present to merge small files ")
           sys.exit()
    elif query_type == 'cstm':
        print("run_hive_query.py           -> HiveQueryTyp: custom query ")
        queries = query
        query = queries.split(",")[0]
        rm_ctlM = "sed -i -e 's/\r$//' "+envvars.list['lfs_app_workflows'] + '/wf_hive_query/'+query
        rc,status = commands.getstatusoutput(rm_ctlM)
        print("run_hive_query.py           -> removing ^M characters in file: "+rm_ctlM+" Status:"+str(rc))
        hdfs_put = "hdfs dfs -put -f "+envvars.list['lfs_app_workflows'] + '/wf_hive_query/'+query+ " "+ envvars.list['hdfs_app_workflows'] + '/wf_hive_query/'
        rc,status = commands.getstatusoutput(hdfs_put)
        print("run_hive_query.py           -> copying file: "+hdfs_put+" Status:"+str(rc))
        
        if len(queries.split(","))==2:
           py_query = queries.split(",")[1]
           hdfs_put = "hdfs dfs -put -f "+envvars.list['lfs_app_workflows'] + '/wf_hive_query/'+py_query+ " "+ envvars.list['hdfs_app_workflows'] + '/wf_hive_query/'
           rc,status = commands.getstatusoutput(hdfs_put)
           print("run_hive_query.py           -> copying file: "+hdfs_put+" Status:"+str(rc))
        
        else:
           py_query = 'invalidate_metadata.py'
        dynamic_properties = '\n'.join([dynamic_properties,
                                        'hive_query='+query,
                                        'python_script='+py_query])
        abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table +","            

    properties_file.write(dynamic_properties)
    properties_file.close()
    print("run_hive_query.py           -> CommnPrpty : " + common_properties) 
    print("run_hive_query.py           -> TablePrpty : " + table_properties)
    print("run_hive_query.py           -> DynmcPrpty : " + dynamic_properties.replace("\n",", ")) 
    print("run_hive_query.py           -> FinalPrpty : " + final_properties) 
    sys.stdout.flush()
     # ABC Logging Started
    parameter_string=""
    comments =  "Properties file name :" +final_properties
    abc_line = "|".join([group,"run_hive_query.py","python","run_job.py",str(table),parameter_string,"RUNNING",
                         getpass.getuser(),comments,str(datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+",run_hive_query.py"  
    sys.stdout.flush()
    rc = runoozieworkflow(final_properties,abc_parameter)
    print "Return-Code:" + str(rc)
    if rc > return_code:
       return_code = rc
    abc_line = "|".join([group,"run_hive_query.py","python","run_job.py",str(table),parameter_string,"ENDED",
                         getpass.getuser(),"return-code:"+str(return_code),str(datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush()                     

    print("run_hive_query.py           -> Ended      : " + datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    print start_line
    print "Return-Code:" + str(return_code)
    sys.exit(return_code)
  
def runoozieworkflow(final_properties,abc_parameter):    
    #command to trigger oozie script
    workflow = envvars.list['hdfs_app_workflows'] + '/wf_hive_query'
    oozie_wf_script = "python " + envvars.list['lfs_global_scripts'] + "/run_oozie_workflow.py " + workflow + ' ' + final_properties +' '+abc_parameter

    print("run_hive_query.py           -> Invoked   : " + oozie_wf_script) 
    sys.stdout.flush()
    #rc,status = commands.getstatusoutput(oozie_wf_script)
    #print(status)
    call = subprocess.Popen(oozie_wf_script.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    while True:
       line = call.stdout.readline()
       if not line:
           break
       print line.strip()
       sys.stdout.flush()
    call.communicate()
    print "call returned"+str(call.returncode) 
    return call.returncode  


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

def determine_default_field_format(field_format):
    #print "field_format = ", field_format
    if field_format==None or field_format.strip()=="":
       field_format="%Y-%m-%d"
    return field_format

def arg_handle():
    usage = "usage: run_hive_query.py [options]"
    parser = OptionParser(usage)
    parser.add_option("-i", "--op0",dest="query_type",
                      help="ingest type")
    parser.add_option("-t", "--op1", dest="table",
              help="environment name")
    parser.add_option("-f", "--op2",dest="query",
                      help="increment field")
    parser.add_option("--op3",dest="ldate",
                      help="load_Date field")
    parser.add_option("--cmmn_dt", dest="common_date",
                  help="application name") 
    parser.add_option("-a", "--app", dest="app",
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
    print("run_hive_query.py           -> Input      : " + str(options)) 
    if  options.table == "":
        parser.error("Argument, table_name, is required.")
        return_code = 10
        sys.exit(return_code)
    table = options.table.lower()
    group = options.group
    abc_line = "|".join([group,"run_hive_query.py","python","run_job.py",str(table),str(options),"STARTED",
                         getpass.getuser(),"run_ingest started..",str(datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush()
    return options




if __name__ == "__main__":
    main()
