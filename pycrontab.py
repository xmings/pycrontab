#-*- coding:utf8 -*-
import os, time, uuid
import logging
from multiprocessing import Process
from datetime import date, datetime, timedelta
from subprocess import Popen, PIPE


path = os.path.abspath(__file__)
        


########################################################################
class Job(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, script, crontab, *args):
        """Constructor"""
        self.script = script
        self.crontab = crontab
        self.start_time = datetime.now()
        self.run_batch_id = None
        self.next_time = None
        self.log_file = None
        self.log_file_timestamp = '%Y%m%d%H%M%S'
        self.log_file_suffix = '-{timestamp}-{sequence}.log'
        self.log_file_sequence = 1
        self.gen_next_time()
        self.log()
        
    #----------------------------------------------------------------------
    def gen_next_time(self):
        """"""
        month = self.crontab._month if self.crontab._month else 0
        day = self.crontab._month if self.crontab._day else 0
        week = self.crontab._month if self.crontab._week else 0
        if (month > 0 or day > 0) and week > 0:
            raise Exception('week month or day')
        hour = self.crontab._month if self.crontab._hour else 0
        minute = self.crontab._month if self.crontab._minute else 0
        second = self.crontab._month if self.crontab._second else 0
        self.next_time = self.next_time if self.next_time else self.start_time
        
        if self.crontab._method == 'every':
            self.next_time = self.next_time \
                + timedelta(weeks = self.crontab._weeks) \
                + timedelta(days = self.crontab._days) \
                + timedelta(hours = self.crontab._hours) \
                + timedelta(minutes = self.crontab._minutes) \
                + timedelta(seconds = self.crontab._seconds)
            
            if self.start_time.month == 12:            
                self.next_time.replace(year = self.start_time.year + 1, month = 1)
            else:
                self.next_time.replace(month = self.start_time.month + 1)
        else:
            if month < self.next_time.month:
                self.next_time.replace(year = self.next_time.year + 1)
            if month == self.next_time.month and day < self.next_time.day:
                self.next_time.replace(month = self.next_time.month + 1 if self.next_time.month < 12 else 1)
            if month == self.next_time.month and day == self.next_time.day and hour < self.next_time.hour:
                self.next_time += timedelta(days = 1)
            if month == self.next_time.month and day == self.next_time.day and hour == self.next_time.hour \
                 and minute < self.next_time.minute:
                self.next_time += timedelta(hour = 1)
            if month == self.next_time.month and day == self.next_time.day and hour == self.next_time.hour \
                 and minute == self.next_time.minute and second < self.next_time.second:
                self.next_time += timedelta(minute = 1)
        
    #----------------------------------------------------------------------
    def log(self, path = None, prefix = None, size = None):
        """"""
        if path:
            self.log_path = path
        else:
            self.log_path = os.path.join(path, 'log')
            
            
        if prefix:
            self.log_file = os.path.join(self.log_path, str(prefix) + self.log_file_suffix)
        else:
            self.log_file = os.path.join(self.log_path,
                                         os.path.splitext(os.path.basename(script))[0] + self.log_file_suffix)               
            
        if size:
            self.log_size = size
        else:
            self.log_size = 10
            
            
    #----------------------------------------------------------------------
    def __gt__(self, other):
        """"""
        if self.next_time > other.next_time:
            return True
        return False
    
    
    #----------------------------------------------------------------------
    def _logger(self):
        """"""
        self.logger = logging.getLogger(__file__)
        self.logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            filehandler = logging.FileHandler(self.log_file)
            filehandler.setLevel(logging.DEBUG)
    
            consolehandler = logging.StreamHandler()
            consolehandler.setLevel(logging.ERROR)

            formatter = logging.Formatter("%(asctime)s - %(filename)s - %(module)s - %(levelname)s - %(message)s")
    
            filehandler.setFormatter(formatter)
            consolehandler.setFormatter(formatter)
    
            logger.addHandler(filehandler)
            logger.addHandler(consolehandler)
            

    #----------------------------------------------------------------------
    def run(self):
        """"""
        now_str = datetime.now().strftime(self.log_file_timestamp)
        log_file = self.log_file.format(now_str, self.log_file_sequence)
        
        self.logger = self._logger()
        self.logger.info('begin running script: {}'.format(script))
        try:
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
            out, err = p.communicate()
            if err and p.returncode != 0:
                raise Exception(err)
        except Exception as e:
            err = '\n'.join(e.args)
            self.logger.error("The command finished with error:" + err)
        else:
            self.logger.info("The result of run the command: " + str(out) + str(err))
        finally:
            self.logger.info('finish running script: {}'.format(script))
        
        
            
        
        
        
########################################################################
class Crontab(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self._method = 'every'
        self._month = None
        self._day =  None
        self._weeek = None
        self._hour = None
        self._minute = None
        self._second = None
        self._jobs = []
        
    #----------------------------------------------------------------------
    def every(self):
        """"""
        return self
    
    #----------------------------------------------------------------------
    def at(self):
        """"""
        return self
    
    #----------------------------------------------------------------------
    def month(self, m):
        """"""
        self._month = m
        return self
    
    #----------------------------------------------------------------------
    def day(self, d):
        """"""
        self._day = d
        return self
    
    #----------------------------------------------------------------------
    def week(self, w):
        """"""
        self._week = w
        return self
        
    #----------------------------------------------------------------------
    def hour(self, h):
        """"""
        self._hour = h
        return self
    
    #----------------------------------------------------------------------
    def minute(self, m):
        """"""
        self._minute = m
        return self

    #----------------------------------------------------------------------
    def second(self, s):
        """"""
        self._second = s
        return self
    
    #----------------------------------------------------------------------
    def add(self, script):
        """"""
        j = Job(script, self)
        self._jobs.append(j)
        
    
    #----------------------------------------------------------------------
    def loop(self):
        """
        需要开启队列，由另外的进程运行任务
        """
        while True:
            self.run_batch_id = uuid.uuid1().hex
            now = datetime.now().replace(microsecond=0)
            for j in sorted(self._jobs):
                if j.next_time == now:
                    p = Process(target=j.run)
                    p.start()
                    j.run_batch_id = self.run_batch_id
            p.join()
            time.sleep(1)

                

    
