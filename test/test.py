#-*- coding:utf-8 -*-
import os
from pycrontab import crontab, crontab_run, freeze_support,Queue

queue = Queue()
if __name__ == '__main__':
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calculator.py')
    # crontab.every().second(5).add(script)
    crontab.every().second(2).add(script)

    #crontab.at().hour(17).minute(30).second(0).add(script)
    crontab_run(True)