#-*- coding:utf-8 -*-
import os
from pycrontab import crontab, crontab_run

if __name__ == '__main__':
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    crontab.every().second(5).add(script)
    crontab.every().second(5).add(script)
    crontab_run()