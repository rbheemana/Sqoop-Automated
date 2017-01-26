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
    print("run_solr_index.py           -> Started    : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    options = arg_handle()
    env = options.env.strip()
    env_ver = options.env_ver.strip()
    app = options.app.strip()
    sub_app = options.sub_app.strip()
    group = options.group.strip()
    # Get envvars from oozie_common_properties file
    envvars.populate(env,env_ver,app,sub_app)
    collection_name = options.collection_name.strip()
    actions = options.actions.strip()
    try:
      script_name = options.script_name.strip()
    except AttributeError as e:
      script_name = ""
    curr_date = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d") 
    prev_load_date = (datetime.datetime.fromtimestamp(time.time()) + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    try:
      lower_bound = options.min_bound.strip()
    except AttributeError as e:
      lower_bound = prev_load_date
    try:
      upper_bound = options.max_bound.strip()
    except AttributeError as e:
      upper_bound = prev_load_date

    if collection_name is None or collection_name == "":
       print("run_solr_index.py           -> Error    : Collection name is required ")
       sys.exit(8)
    if actions is None or actions == "":
       print("run_solr_index.py           -> Error    : ACtions are required ex:insert,delete,create ")
       sys.exit(8)
    collection_folder = envvars.list['lfs_solr'] + '/'+collection_name
    if not os.path.isdir(collection_folder):
       print("run_solr_index.py           -> Error    : Collection folder "+collection_folder+" does not exists..")
       sys.exit(8)
    if not (os.path.exists(collection_folder+"/conf") and os.path.exists(collection_folder+"/conf/schema.xml")):
       print("run_solr_index.py           -> Error    : Configuration folder is corrupt..")
       sys.exit(8)

    title_trans=''.join(chr(c) if (chr(c).isupper() or chr(c).islower() or str(c).isdigit()) and chr(c) != '-' else '_' for c in range(256))
    solr_table_name = 'solr_' + collection_name+"_"+lower_bound
    solr_table_name = solr_table_name.translate(title_trans)


    if "delete" in actions.lower():
       solr_cmd = " ".join(["solrctl --solr",
                            envvars.list['solr_server'],
                            "--zk",envvars.list['zookeeper_ensemble'],
                            "collection --delete ",
                            collection_name])
       rc, out = commands.getstatusoutput(solr_cmd)
       if rc != 0:
          print("run_solr_index.py           -> Failed      : "+solr_cmd+";RC : "+str(rc))
          print out
          #sys.exit(10)
       print("run_solr_index.py           -> Command  : "+solr_cmd+";RC : "+str(rc))
       solr_cmd = " ".join(["solrctl --solr",
                            envvars.list['solr_server'],
                            "--zk",envvars.list['zookeeper_ensemble'],
                            "instancedir --delete ",
                            collection_name])
       rc, out = commands.getstatusoutput(solr_cmd)
       if rc != 0:
          print out
          print("run_solr_index.py           -> Failed      : "+solr_cmd+";RC : "+str(rc))
          #sys.exit(10)
       print("run_solr_index.py           -> Command  : "+solr_cmd+";RC : "+str(rc))

    if "create" in actions.lower():
       solr_cmd = " ".join(["solrctl --solr",
                            envvars.list['solr_server'],
                            "--zk",envvars.list['zookeeper_ensemble'],
                            "instancedir --create ",
                            collection_name,collection_folder
                            ])
       rc, out = commands.getstatusoutput(solr_cmd)
       if rc != 0:
          print out
          print("run_solr_index.py           -> Failed      : "+solr_cmd+";RC : "+str(rc))
          sys.exit(10)
       print("run_solr_index.py           -> Command  : "+solr_cmd+";RC : "+str(rc))
       solr_cmd = " ".join(["solrctl --solr",
                            envvars.list['solr_server'],
                            "--zk",envvars.list['zookeeper_ensemble'],
                            "collection --create ",
                            collection_name,"-s 1 -r 1 -m 1"
                            ])
       rc, out = commands.getstatusoutput(solr_cmd)
       if rc != 0:
          print out
          print("run_solr_index.py           -> Failed      : "+solr_cmd+";RC : "+str(rc))
          sys.exit(10)
       print("run_solr_index.py           -> Command  : "+solr_cmd+";RC : "+str(rc))
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_' + app.replace("/","_") + '_'+solr_table_name + '.properties'
    # Remove if the file exists
    silentremove(final_properties)
    
    

    if "query" in actions.lower():
       # open the final properties file in write append mode
       properties_file = open(final_properties, 'wb')
       shutil.copyfileobj(open('/cloudera_nfs1/config/oozie_global.properties', 'rb'), properties_file)
       dynamic_properties = ""
       if script_name == "":
          print("run_solr_index.py           -> Error    : Script name required for insert option")
          sys.exit(8)
       else:
          script_name = options.script_name.strip()
          script_file = collection_folder+"/"+script_name
          hdfs_cmd = "hdfs dfs -put -f "+script_file+" "+envvars.list['hdfs_app_workflows'] + '/wf_hive_query/'
          rc, out = commands.getstatusoutput(hdfs_cmd)
          if rc != 0:
             print("run_solr_index.py           -> Failed      : "+hdfs_cmd+";RC : "+str(rc))
             print out
             #sys.exit(10)
          
          dynamic_properties = '\n'.join(['\nenv=' + env,
                                'app=' + app,
                                'sub_app=' +sub_app,
                                'group=' +group,
                                'happ=' + envvars.list['happ'],
                                'hv_db=' +envvars.list['hv_db_'+app+'_'+sub_app+'_stage'],
                                'hv_db_stage=' +envvars.list['hv_db_'+app+'_'+sub_app+'_stage'],
                                'hv_table=' + solr_table_name, 
                                'table=' + solr_table_name,
                                'stage_table=' + solr_table_name,
                                'hdfs_location='+envvars.list['hdfs_str_raw']+"/solr/"+solr_table_name,
                                'hdfs_tmp_dir='+envvars.list['hdfs_str_raw']+"/tmp_"+ solr_table_name,
                                'prev_load_date='+prev_load_date,
                                'hive_query='+script_name,
                                'curr_date='+curr_date,
                                'min_bound=' + lower_bound,
                                'max_bound=' + upper_bound])  
       properties_file.write(dynamic_properties)
       properties_file.close()
       print("run_solr_index.py           -> DynmcPrpty : " + dynamic_properties.replace("\n",", ")) 
       print("run_solr_index.py           -> FinalPrpty : " + final_properties)
       sys.stdout.flush()
       abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+",run_solr_index.py" 
       rc = runoozieworkflow(final_properties,abc_parameter)
       if rc > 0:
          sys.exit(rc)
    if "insert" in actions.lower():
       morphline_file_name = collection_name+".conf"
       morphline_file_path = collection_folder+"/"+collection_name+".conf"
       if not (os.path.exists(morphline_file_path)):
             print("run_solr_index.py           -> Error    : Morphline conf file doesnot exist.."+morphline_file_path)
             sys.exit(8)
       solr_mr_cmd = "|".join(["hadoop|jar",
                            envvars.list['solr_home']+"/contrib/mr/search-mr-1.0.0-cdh5.5.4-job.jar|org.apache.solr.hadoop.MapReduceIndexerTool",
                            "-D|'mapred.child.java.opts=-Xmx4G'",
                            "-D|'mapreduce.reduce.memory.mb=8192'",
                            "--morphline-file",morphline_file_path,
                            "--output-dir",envvars.list['hdfs_str_raw']+"/tmp_"+ solr_table_name,
                            "--verbose|--go-live|--zk-host",envvars.list['zookeeper_ensemble'],
                            "--collection", collection_name,
                            envvars.list['hdfs_str_raw']+"/solr/"+solr_table_name
                            ])
       env_var_cmd = ";".join(["export myDriverJarDir=/opt/cloudera/parcels/CDH/lib/solr/contrib/crunch", 
                               "export myDependencyJarDir=/opt/cloudera/parcels/CDH/lib/search/lib/search-crunch", 
                               "export myDriverJar=$(find $myDriverJarDir -maxdepth 1 -name 'search-crunch-*.jar' ! -name '*-job.jar' ! -name '*-sources.jar')", 
                               "export myDependencyJarFiles=$(find $myDependencyJarDir -name '*.jar' | sort | tr '\n' ',' | head -c -1)",
                               "export myDependencyJarFiles=$myDependencyJarFiles,$(find /opt/cloudera/parcels/CDH/jars -name 'snappy-java-*.jar')",
                               "export myDependencyJarPaths=$(find $myDependencyJarDir -name '*.jar' | sort | tr '\n' ':' | head -c -1)",
                               'export myJVMOptions="-DmaxConnectionsPerHost=10000 -DmaxConnections=10000"',
                               "export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark",
                               "export SPARK_SUBMIT_CLASSPATH=/opt/cloudera/parcels/CDH/lib/search/lib/search-crunch/commons-codec-*.jar:$SPARK_HOME/assembly/lib/*:/opt/cloudera/parcels/CDH/lib/search/lib/search-crunch/*"])
       #rc, export = commands.getstatusoutput(env_var_cmd)
       #if rc != 0:         
       #   print("run_solr_index.py           -> Failed      : "+env_var_cmd+";RC : "+str(rc))
       #   print out         
       #   sys.exit(10)
       rc, dependency_jars = commands.getstatusoutput(env_var_cmd+';echo "$myDependencyJarFiles"')
       if rc != 0:         
          print("run_solr_index.py           -> Failed      : "+'echo "$myDependencyJarFiles"'+";RC : "+str(rc))
          print dependency_jars         
          sys.exit(10)         
       print("run_solr_index.py           -> Dependency Jars : "+dependency_jars)
       rc, driver_jar = commands.getstatusoutput(env_var_cmd+';echo "$myDriverJar"')
       if rc != 0:         
          print("run_solr_index.py           -> Failed      : "+'echo "$myDriverJar"'+";RC : "+str(rc))
          print driver_jar         
          sys.exit(10)
       print("run_solr_index.py           -> driver_jar : "+driver_jar)
       rc, jvm_options = commands.getstatusoutput(env_var_cmd+';echo "$myJVMOptions"')
       if rc != 0:         
          print("run_solr_index.py           -> Failed      : "+'echo "$myJVMOptions"'+";RC : "+str(rc))
          print jvm_options         
          sys.exit(10)
       print("run_solr_index.py           -> jvm_options : "+jvm_options)
       rc, user_name = commands.getstatusoutput("echo $USER")
       token_file_name = env + '_' + app.replace("/","_") + '_solr_'+user_name.lower()+'_'+options.group.lower()+'_'+collection_name + '.token'
       token_file_path = envvars.list['lfs_app_wrk'] + '/'+token_file_name
       tokenCmd = " curl --negotiate -u: '"+envvars.list['solr_server']+"/?op=GETDELEGATIONTOKEN' > "+token_file_path
       rc, token_txt = commands.getstatusoutput(tokenCmd)
       if rc != 0:         
          print("run_solr_index.py           -> Failed      : "+tokenCmd+";RC : "+str(rc))
          print token_txt         
          sys.exit(10)
       log4jCmd = "echo $(ls /opt/cloudera/parcels/CDH/share/doc/search-*/search-crunch/log4j.properties)"
       rc, log4j_path = commands.getstatusoutput(log4jCmd)
       if rc != 0:         
          print("run_solr_index.py           -> Failed      : "+log4jCmd+";RC : "+str(rc))
          print token_txt         
          sys.exit(10)
       solr_spark_cmd = "|".join(["spark-submit",
                                  "--master","yarn","--deploy-mode","cluster",
                                  "--jars", dependency_jars,
                                  '--executor-memory','6G',
                                  '--conf','"spark.executor.extraJavaOptions='+jvm_options+'"',
                                  '--conf','"spark.driver.extraJavaOptions='+jvm_options+'"',
                                  '--class','org.apache.solr.crunch.CrunchIndexerTool',
                                  "--files",token_file_path+","+morphline_file_path+","+log4j_path,
                                  driver_jar,
                                  "-Dhadoop.tmp.dir=/tmp",
                                  "-DmorphlineVariable.ZK_HOST="+envvars.list['zookeeper_ensemble'],
                                  "-DtokenFile="+token_file_name,
                                  "--morphline-file",morphline_file_name,
                                  "--log4j|log4j.properties",
                                  "--pipeline-type|spark|--chatty",
                                  envvars.list['hdfs_str_raw']+"/solr/"+solr_table_name])
       solr_cmd = solr_spark_cmd
       call = subprocess.Popen(solr_cmd.split('|'),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
       while True:
          line = call.stdout.readline()
          if not line:
              break
          print line.strip()
          sys.stdout.flush()
       call.communicate()
       
       if call.returncode != 0:
          print "run_solr_index.py           -> Failed      : "+solr_cmd+";RC : "+str(call.returncode)
          sys.exit(10)
       print("run_solr_index.py           -> Command  : "+solr_cmd+";RC : "+str(call.returncode))

    sys.stdout.flush()                     

    print("fi_getfreddiemac.py           -> Ended      : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    print start_line
    print "Return-Code:" + str(return_code)
    sys.exit(return_code)
def runoozieworkflow(final_properties,abc_parameter):    
    #command to trigger oozie script
    workflow = envvars.list['hdfs_app_workflows'] + '/wf_hive_query'
    oozie_wf_script = "python " + envvars.list['lfs_global_scripts'] + "/run_oozie_workflow.py " + workflow + ' ' + final_properties +' '+abc_parameter

    print("run_ingest.py           -> Invoked   : " + oozie_wf_script) 
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
def silentRemoveDirContents(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)

def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured
def silentremovedir(dirname):
    try:
        shutil.rmtree(dirname)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

def arg_handle():
    usage = "usage: run_solr_index.py [options]"
    parser = OptionParser(usage)
    parser.add_option("-i", "--op0",dest="collection_name",
                      help="collection_name")
    parser.add_option("-t", "--op1", dest="actions",
              help="actions")
    parser.add_option("-f", "--op2",dest="script_name",
                      help="script_name")
    parser.add_option("-l", "--op3",dest="min_bound",
                      help="increment field min bound")
    parser.add_option("-u", "--op4",dest="max_bound",
                      help="increment field max bound") 
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("--cmmn_dt", dest="comon_date",
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
    print("run_solr_index.py           -> Input      : " + str(options)) 
    sys.stdout.flush()
    return options

if __name__ == "__main__":
    main()
