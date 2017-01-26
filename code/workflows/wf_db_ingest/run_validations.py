#!/opt/cloudera/parcels/Anaconda/bin/python

# Purpose: This Job is the starting point of all jobs. 
#          Looks up jobnames.list and calls the script

import sys, os, commands, datetime, time ,getpass, errno
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE

def arg_handle():
    usage = "usage: run_validations.py [options]"
    parser = OptionParser(usage)
    parser.add_option("-a", "--user",dest="user_name",
                      help="increment field max bound") 
    parser.add_option("-b", "--domain", dest="domain",
                  help="application name")
    parser.add_option("-c", "--hv_db", dest="hv_db",
                  help="application name")
    parser.add_option("-d", "--hv_table", dest="hv_table",
                  help="application name")
    parser.add_option("-e", "--hv_db_stage", dest="hv_db_stage",
                  help="application name")
    parser.add_option("--validation_type", dest="validation_type",
                  help="validation type")
    parser.add_option("-f", "--stage_table", dest="stage_table",
                  help="application name")
    parser.add_option("--where_hadoop", dest="where_hadoop",
                  help="hive table where clause")
    parser.add_option("--jdbc_connect", dest="jdbc_connect",
                  help="jdbc_connect")
    parser.add_option("--table", dest="table",
                  help="table")
    parser.add_option("--where", dest="where",
                  help="where")
    parser.add_option("--username", dest="username",
                  help="username")
    parser.add_option("--datasource", dest="datasource",
                  help="datasource")
    parser.add_option("-g", "--keystore", dest="keystore",
                  help="application name")
    parser.add_option("-i", "--impalaConnect", dest="impalaConnect",
                  help="application name")

    (options, args) = parser.parse_args()
    print("run_validations.py  -> Input      : " + str(options))
    return options

def main():
	print("run_validations.py  -> Started Run_validations.py")
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python27.zip')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/plat-linux2')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/lib-tk')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/lib-old')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/lib-dynload')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/site-packages')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/site-packages/Sphinx-1.3.5-py2.7.egg')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/site-packages/cryptography-1.0.2-py2.7-linux-x86_64.egg')
	sys.path.append('/opt/cloudera/parcels/Anaconda/lib/python2.7/site-packages/setuptools-25.1.6-py2.7.egg')
	rc, out = commands.getstatusoutput("echo $PATH")
	print("run_validations.py  -> PATH Variable: "+out)
	sys.stdout.flush()
	options = arg_handle()
	#remove keytab file if exists
	rm = "rm "+options.user_name.lower()+".keytab " 
	rc, out = commands.getstatusoutput(rm)
	#get latest keytab file from hdfs
	hget = "hdfs dfs -get "+options.keystore+"/"+options.user_name+".keytab "
	rc, out = commands.getstatusoutput(hget)
	print("run_validations.py  -> Command    : "+hget+"\n"+out+"\n RC:"+str(rc))
	sys.stdout.flush()
	#Authenticate using keytab file
	kerb = "kinit -k -t "+options.user_name+".keytab "+options.user_name+options.domain
	rc, out = commands.getstatusoutput(kerb)
	if rc == 0:
		print("run_validations.py  -> Authenticated: "+kerb+" RC:"+str(rc))
	else:
		print("run_validations.py  -> ERROR    : Authentication failed"+kerb+"\n"+out+"\n RC:"+str(rc))
	sys.stdout.flush()
	impalaCmd=options.impalaConnect+'" refresh '+options.hv_db+"."+options.hv_table+";refresh "+options.hv_db_stage+"."+options.stage_table+';"'
	print "run_validations.py  ->      " + impalaCmd
	sys.stdout.flush()
	rc, out = commands.getstatusoutput(impalaCmd)
	if rc !=0:
		print("run_validations.py  -> ERROR    : Impala command failed"+str(out)+" RC:"+str(rc))
		sys.exit(1)
	print("run_validations.py  -> SUCCESS   : Tables refereshed")
	sys.stdout.flush()
	if 'default' in options.validation_type.lower():
		print("run_validations.py  -> OPTION    : Validate RAW Table and FINAL Table")
		validate_stg_final_tables(options)
	if 'full' in options.validation_type.lower() and options.datasource.strip().lower() == 'oracle':
		print("run_validations.py  -> OPTION    : Validate Total counts between RDBMS Source Table and HIVE FINAL Table")
		validate_full_rdbms_final_tables(options)
	sys.stdout.flush()
	if 'delta' in options.validation_type.lower() and options.datasource.strip().lower() == 'oracle':
		print("run_validations.py  -> OPTION    : Validate counts between RDBMS Source Table and HIVE RAW Table for rows we are importing")
		validate_delta_rdbms_final_tables(options)
	sys.stdout.flush()

def validate_stg_final_tables(options):
	impalaCmd=options.impalaConnect+'"select count(*) from '+options.hv_db+"."+options.hv_table+" where " +options.where_hadoop+' ; "'
	print "run_validations.py  -> COMMAND   : " + impalaCmd
	rc, out = commands.getstatusoutput(impalaCmd)
	if rc !=0:
		print("run_validations.py  -> ERROR    : Impala command failed"+str(out)+" RC:"+str(rc))
		sys.exit(1)
	outputlist = out.split('\n')
	hadoop_final_count = str(outputlist[-1]).strip()
	print("run_validations.py  -> RESULT    : Count from Hive Table "+options.hv_db+"."+options.hv_table+":"+str(hadoop_final_count))
	sys.stdout.flush()
	impalaCmd=options.impalaConnect+"' refresh "+options.hv_db_stage+"."+options.stage_table+"; select count(*) from "+options.hv_db_stage+"."+options.stage_table+"; '"
	print "run_validations.py  -> COMMAND   : " + impalaCmd
	sys.stdout.flush()
	rc, out = commands.getstatusoutput(impalaCmd)
	if rc !=0:
		print("run_validations.py  -> ERROR    : Impala command failed"+str(out)+" RC:"+str(rc))
		sys.exit(1)
	outputlist = out.split('\n')
	hadoop_stage_count = str(outputlist[-1]).strip()
	print("run_validations.py  -> RESULT   : Count from Hive Stage Table "+options.hv_db_stage+"."+options.stage_table+":"+str(hadoop_stage_count))
	sys.stdout.flush()
	if(hadoop_stage_count != hadoop_final_count):
		print("run_validations.py  -> ERROR: Counts of Hive Stage Table and Hive Final Table do not match")
		sys.exit(4)
	print("run_validations.py  -> SUCCESS  : Hurray!! Counts of Hive Stage Table and Hive Final Table match..")
	sys.stdout.flush()

def validate_delta_rdbms_final_tables(options):
	import cx_Oracle
	impalaCmd=options.impalaConnect+'"select count(*) from '+options.hv_db_stage+"."+options.stage_table+'; "'
	print "run_validations.py  -> COMMAND   : " + impalaCmd
	rc, out = commands.getstatusoutput(impalaCmd)
	if rc !=0:
		print("run_validations.py  -> ERROR    : Impala command failed"+str(out)+" RC:"+str(rc))
		sys.exit(1)
	outputlist = out.split('\n')
	hadoop_stage_count = str(outputlist[-1]).strip()
	print("run_validations.py  -> RESULT    : Count from Hive Table "+options.hv_db_stage+"."+options.stage_table+":"+str(hadoop_stage_count))
	rdbms_connect = options.jdbc_connect.strip().split('@')[1]
	db = cx_Oracle.connect(options.username.strip()+'/Hadoop#123@'+rdbms_connect)
	cursor = db.cursor()
	rdbms_query = "select count(*) from "+options.table+" where "+options.where
	print("run_validations.py  -> COMMAND   : "+rdbms_query)
	cursor.execute(rdbms_query)
	oracle_results = []
	for row in cursor:
		oracle_results.append(row[0])
	rdbms_oracle_count = str(oracle_results[0]).strip()
	print("run_validations.py  -> RESULT    : Count from RDBMS Table "+str(oracle_results[0]))
	if(rdbms_oracle_count != hadoop_stage_count):
		print("run_validations.py  -> ERROR: Counts of RDBMS Table and Hive Final Table do not match")
		sys.exit(4)
	print("run_validations.py  -> SUCCESS   : Hurray!! Counts of RDBMS Table and Hive RAW Table match..")

def validate_full_rdbms_final_tables(options):
	import cx_Oracle
	impalaCmd=options.impalaConnect+'"select count(*) from '+options.hv_db+"."+options.hv_table+'; "'
	print "run_validations.py  -> COMMAND   : " + impalaCmd
	rc, out = commands.getstatusoutput(impalaCmd)
	if rc !=0:
		print("run_validations.py  -> ERROR    : Impala command failed"+str(out)+" RC:"+str(rc))
		sys.exit(1)
	outputlist = out.split('\n')
	hadoop_final_count = str(outputlist[-1]).strip()
	print("run_validations.py  -> RESULT    : Count from Hive Table "+options.hv_db+"."+options.hv_table+":"+str(hadoop_final_count))
	rdbms_connect = options.jdbc_connect.strip().split('@')[1]
	db = cx_Oracle.connect(options.username.strip()+'/Hadoop#123@'+rdbms_connect)
	cursor = db.cursor()
	rdbms_query = "select count(*) from "+options.table
	print("run_validations.py  -> COMMAND   : "+rdbms_query)
	cursor.execute(rdbms_query)
	oracle_results = []
	for row in cursor:
		oracle_results.append(row[0])
	rdbms_oracle_count = str(oracle_results[0]).strip()
	print("run_validations.py  -> RESULT    : Count from RDBMS Table "+str(oracle_results[0]))
	if(rdbms_oracle_count != hadoop_final_count):
		print("run_validations.py  -> ERROR: Counts of RDBMS Table and Hive Final Table do not match")
		sys.exit(4)
	print("run_validations.py  -> SUCCESS   : Hurray!! Counts of RDBMS Table and Hive Final Table match..")
if __name__ == "__main__":
    main()
