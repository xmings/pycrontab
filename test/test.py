#-*- coding:utf-8 -*-
import os, datetime
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    crontab.every('day').at(hour=17, minute=30).add(script)
    crontab.every('minute').interval(5).begin(datetime.datetime.now()).add(script)
    crontab_run(True)