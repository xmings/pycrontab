#-*- coding:utf-8 -*-
import os, datetime
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    crontab.every('day').at(hour=12, minute=19).execute(script)
    #crontab.every('second').interval(5).begin(datetime.datetime.now()).execute(script)
    #crontab.every('month').at(day=-1).execute(script)
    crontab_run(True)