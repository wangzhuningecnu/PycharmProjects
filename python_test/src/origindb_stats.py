# python2.7.13
# -*- coding: UTF-8 -*-
import datetime
import calendar
import commands
from subprocess import Popen, PIPE



"""处理月份，实现月份加减"""
def add_months(dt, months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    day = min(dt.day,calendar.monthrange(year,month)[1])
    return dt.replace(year=year, month=month, day=day)


"""
要查询的日期，返回字符串
today：要查询的当天的日期，如 2018-07-12
yestoday：昨天，如2018-07-11
lastweek：上周的同星期：如2018-07-12这周五，2018-07-05上周五
lastmonth：有两个可选，根据数据情况选择，2018-06-12上个月同日/2018-6-14 减4周 
"""
def data_selection(dt):
    date_query = {}
    dt_today = dt.strftime("%Y-%m-%d")
    dt_yestoday = ( dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    dt_lastweek = ( dt - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    # 2018-07-12 2018-06-14
    # lastmonth = ( dt - datetime.timedelta(weeks= 4)).strftime("%Y-%m-%d")
    dt_lastmonth = add_months( dt, -1).strftime("%Y-%m-%d")

    date_query['today'] = dt_today
    date_query['yestoday'] = dt_yestoday
    date_query['lastweek'] = dt_lastweek
    date_query['lastmonth'] = dt_lastmonth
    return date_query



"""计算表的 size"""
def size_of_table(path, dates_query):
    sizes_query = {}

    try:
        sizes_query['today'] = float(commands.getoutput(
            'hadoop fs -du -h ' + path + '| grep -i d=' + dates_query['today'] + '| awk \'{print $1}\''))
        sizes_query['yestoday'] = float(commands.getoutput(
            'hadoop fs -du -h ' + path + '| grep -i d=' + dates_query['yestoday'] + '| awk \'{print $1}\''))
        sizes_query['lastweek'] = float(commands.getoutput(
            'hadoop fs -du -h ' + path + '| grep -i d=' + dates_query['lastweek'] + '| awk \'{print $1}\''))
        sizes_query['lastmonth'] = float(commands.getoutput(
            'hadoop fs -du -h ' + path + '| grep -i d=' + dates_query['lastmonth'] + '| awk \'{print $1}\''))


        '''计算当天表 size 日同比新增数据量'''
        sizes_query['day_size_incre'] = sizes_query['today'] - sizes_query['yestoday']

        '''计算当天表 size 周同比新增数据量'''
        sizes_query['week_size_incre'] = sizes_query['today'] - sizes_query['lastweek']

        '''计算当天表 size 月同比新增数据量'''
        sizes_query['month_size_incre'] = sizes_query['today'] - sizes_query['lastmonth']

    except:

        sizes_query = {}

    return sizes_query


'''计算表的 总量'''
def count_of_table(db_name_query, table_name_query, dates_query):
    counts_query = {}
    try:
        output1, param1 = Popen(['hive', '-e',
                                 'set hive.cli.print.header=false;select count(1) from ' + db_name_query + '.' + table_name_query + ' where d=\'' + dates_query['today'] + '\';'],
                                stdout=PIPE).communicate()
        counts_query['today'] = int(output1)

        output2, param2 = Popen(['hive', '-e',
                                 'set hive.cli.print.header=false;select count(1) from ' + db_name_query + '.' + table_name_query + ' where d=\'' + dates_query['yestoday'] + '\';'],
                                stdout=PIPE).communicate()
        counts_query['yestoday'] = int(output2)

        output3, param3 = Popen(['hive', '-e',
                                 'set hive.cli.print.header=false;select count(1) from ' + db_name_query + '.' + table_name_query + ' where d=\'' + dates_query['lastweek'] + '\';'],
                                stdout=PIPE).communicate()
        counts_query['lastweek'] = int(output3)

        output4, param4 = Popen(['hive', '-e',
                                 'set hive.cli.print.header=false;select count(1) from ' + db_name_query + '.' + table_name_query + ' where d=\'' + dates_query['lastmonth'] + '\';'],
                                stdout=PIPE).communicate()
        counts_query['lastmonth'] = int(output4)

        '''计算当天表 总量 日同比新增数据量'''
        counts_query['day_count_incre'] = counts_query['today'] - counts_query['yestoday']

        '''计算当天表 总量 周同比新增数据量'''
        counts_query['week_count_incre'] = counts_query['today'] - counts_query['lastweek']

        '''计算当天表 总量 月同比新增数据量'''
        counts_query['month_count_incre'] = counts_query['today'] - counts_query['lastmonth']

    except:
        counts_query = {}
    return counts_query


'''计算表关联的文件数'''
def numOfFiles_relate_table(path, dates_query):

    numOfFiles = -1
    try:
        numOfFiles = int(commands.getoutput(
            'hadoop fs -ls ' + path + '| grep -i d=' + dates_query + '| wc -l'))
    except:
        pass

    return numOfFiles


if __name__ == '__main__':
    import sys

    """得到命令行参数：仓库名，表名，表路径，要查询的日期（2018-07-16）"""
    try:
        db_name, table_name, tbl_hdfs_file_path, query_date = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
        query_date = datetime.datetime.strptime(query_date, '%Y-%m-%d')
    except IndexError:
        # TODO 默认值
        db_name = 'aaa'
        table_name = 'aaa'
        tbl_hdfs_file_path = "/user/biuser/warehouse/etl/" + db_name + ".db/" + table_name
        query_date = datetime.datetime(2018, 07, 15)

    date = data_selection(query_date)
    print date
    size = size_of_table(tbl_hdfs_file_path, date)
    print size
    count = count_of_table(db_name,table_name,date)
    print count

    numOfFiles = numOfFiles_relate_table(tbl_hdfs_file_path,date['today'])
    print numOfFiles




