#-*- coding:utf-8 -*-
import os, datetime
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    endTime = datetime.datetime.strptime('2018-08-29 14:48:50', '%Y-%m-%d %H:%M:%S')
    # crontab.every('day').at(hour=12, minute=19).end(endTime).execute(script)
    crontab.every('second').interval(5).end(endTime).execute(script)
    #crontab.every('month').at(day=-1).execute(script)
    crontab_run(True)