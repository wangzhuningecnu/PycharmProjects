import datetime
import calendar
import commands
from subprocess import Popen, PIPE

# subprocess.call("ls -l", shell=True)
db_name='DW_FltDB'
table_name='fact_ti_order'
tbl_hdfs_file_path="/user/biuser/warehouse/etl/"+db_name+".db/"+table_name

sql = """
  set hive.cli.print.header=false;set hive.exec.parallel=true;
  select count(1) from %s.%s where d='%s'
  union all select count(1) from %s.%s where d='%s'
  union all select count(1) from %s.%s where d='%s'
  union all select count(1) from %s.%s where d='%s'
;
""" % ('DW_FltDB','fact_ti_order','2018-07-17','DW_FltDB','fact_ti_order','2018-07-16','DW_FltDB','fact_ti_order','2018-07-10','DW_FltDB','fact_ti_order','2018-06-17')
cmd = 'hive -e \"'+sql.replace('"', "\'")+'\"'
print cmd

p = Popen(cmd, shell=True,stdout=PIPE)
out,temp = p.communicate()
print out


#title=commands.getoutput('hadoop fs -du -h '+ tbl_hdfs_file_path + '| grep -i d=2018-07-15 | awk \'{print $1}\'')

