#-*- coding:utf-8 -*-
import os, datetime
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    crontab.every('day').at(hour=17, minute=30).add(script)
    crontab.every('second').interval(5).end(datetime.datetime(year=2018, month=7, day=31, hour=17, minute=24, second=50)).add(script)
    crontab_run(True)