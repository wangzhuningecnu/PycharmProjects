import datetime
import calendar
import time

def add_months(dt,months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    day = min(dt.day,calendar.monthrange(year,month)[1])
    return dt.replace(year=year, month=month, day=day)

def data_selection():
    now = datetime.datetime.now()
    today = ( now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    yestoday = ( now - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    lastweek = ( now - datetime.timedelta(days=8)).strftime("%Y-%m-%d")
    # 2018-07-12 2018-06-15
    # lastmonth = ( now - datetime.timedelta(weeks= 4)).strftime("%Y-%m-%d")
    lastmonth = add_months( now - datetime.timedelta(days=1), -1).strftime("%Y-%m-%d")
    return today,yestoday,lastweek,lastmonth


