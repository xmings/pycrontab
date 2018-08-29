#-*- coding:utf-8 -*-
import os, datetime
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    script1 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    script2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scrapy_news.py')
    endTime = datetime.datetime.strptime('2018-08-29 14:48:50', '%Y-%m-%d %H:%M:%S')
    crontab.every('second').interval(5).execute(script2)
    crontab.every('second').interval(1).execute(script1)
    #crontab.every('month').at(day=-1).execute(script)
    crontab_run(True)