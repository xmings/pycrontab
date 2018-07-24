#-*- coding:utf-8 -*-
import os, time, uuid, platform
import logging
from multiprocessing import Process, Queue, freeze_support, Manager
from datetime import date, datetime, timedelta
from subprocess import Popen, PIPE


current_path = os.path.dirname(os.path.abspath(__file__))

decode = 'gb2312' if platform.system() == 'Windows' else 'utf-8'

########################################################################
class Job(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, script, crontab, *args):
        """Constructor"""
        self.script = script
        self.crontab = crontab
        self.start_time = datetime.now().replace(microsecond=0)
        self.run_batch_id = None
        self.next_time = None
        self.log_file = None
        self.log_file_timestamp = '%Y%m%d'
        self.log_file_suffix = '-{timestamp}-{sequence}.log'
        self.log_file_sequence = 1
        self.gen_next_time()
        self.log()
        self.logger = None

    #----------------------------------------------------------------------
    def gen_next_time(self):
        """"""
        month = self.crontab._month if self.crontab._month else 0
        day = self.crontab._day if self.crontab._day else 0
        week = self.crontab._week if self.crontab._week else 0
        if (month > 0 or day > 0) and week > 0:
            raise Exception('week month or day')
        hour = self.crontab._hour if self.crontab._hour else 0
        minute = self.crontab._minute if self.crontab._minute else 0
        second = self.crontab._second if self.crontab._second else 0
        self.next_time = self.next_time if self.next_time else self.start_time
        
        if self.crontab._method == 'every':
            self.next_time = self.next_time \
                + timedelta(weeks = week) \
                + timedelta(days = day) \
                + timedelta(hours = hour) \
                + timedelta(minutes = minute) \
                + timedelta(seconds = second)
            
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
                self.next_time += timedelta(hours = 1)
            if month == self.next_time.month and day == self.next_time.day and hour == self.next_time.hour \
                 and minute == self.next_time.minute and second < self.next_time.second:
                self.next_time += timedelta(minutes = 1)

        # 计算日志大小
        if os.path.getsize(self.log_file) > self.log_size*1024*1024:
            self.log_file_sequence += 1
        
    #----------------------------------------------------------------------
    def log(self, path = None, prefix = None, size = None):
        """"""
        if path:
            self.log_path = path
        else:
            self.log_path = os.path.join(current_path, 'log')
            if not os.path.exists(self.log_path):
                os.mkdir(self.log_path)
            
        if prefix:
            self.log_file = os.path.join(self.log_path, str(prefix) + self.log_file_suffix)
        else:
            self.log_file = os.path.join(self.log_path,
                                         os.path.splitext(os.path.basename(self.script))[0] + self.log_file_suffix)               
            
        if size:
            self.log_size = size
        else:
            self.log_size = 10
            
            
    #----------------------------------------------------------------------
    def __lt__(self, other):
        """"""
        return self.next_time < other.next_time

    #----------------------------------------------------------------------
    def _logger(self, debug=False):
        """"""
        now_str = datetime.now().strftime(self.log_file_timestamp)
        log_file = self.log_file.format(timestamp=now_str, sequence=self.log_file_sequence)

        logger = logging.getLogger(log_file)
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            filehandler = logging.FileHandler(log_file, encoding='utf-8')
            filehandler.setLevel(logging.DEBUG)
    
            consolehandler = logging.StreamHandler()
            consolehandler.setLevel(logging.DEBUG if debug else logging.ERROR)

            formatter = logging.Formatter("%(asctime)s - %(filename)s - %(module)s - %(levelname)s - %(message)s")
    
            filehandler.setFormatter(formatter)
            consolehandler.setFormatter(formatter)
    
            logger.addHandler(filehandler)
            logger.addHandler(consolehandler)


        return logger

    #----------------------------------------------------------------------
    def run(self):
        """"""
        self.logger = self._logger()
        self.logger.info('begin running script: {}'.format(self.script))
        try:
            cmd = 'python ' + self.script
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
            out, err = p.communicate()
            if err and p.returncode != 0:
                self.logger.error(
                    "The command finished with error: " 
                    + err.decode(decode).replace('\r','').rstrip('\n')
                )
        except Exception as e:
            self.logger.error(
                "The command finished with error: " + e.args[0] + e.args[1]
            )
        else:
            self.logger.info(
                "The stdout of the command: " \
                + out.decode(decode).replace('\r','').rstrip('\n') \
                + ("\n" + err.decode(decode).replace('\r','').rstrip('\n')).rstrip('\n')
            )
        finally:
            self.logger.info('finish running script: {}'.format(self.script))


    def __str__(self):
        return '<Job %r, method %r, next_time %s>' % (self.script, self.crontab._method, self.next_time)
        

########################################################################
class Crontab(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self._method = 'every'
        self._month = None
        self._day =  None
        self._week = None
        self._hour = None
        self._minute = None
        self._second = None
        self._jobs = []
        
    #----------------------------------------------------------------------
    def every(self):
        """"""
        self._method = 'every'
        return self
    
    #----------------------------------------------------------------------
    def at(self):
        """"""
        self._method = 'at'
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
        if not os.path.exists(script):
            raise Exception('The script not found: {}'.format(script))
        j = Job(script, self)
        self._jobs.append(j)
        
    
    #----------------------------------------------------------------------
    def loop(self, queue, debug):
        """"""
        print(debug)
        while True:
            self.run_batch_id = uuid.uuid1().hex
            now = datetime.now().replace(microsecond=0)
            for j in sorted(self._jobs):
                if debug:
                    j._logger(debug).info("{} now:{}".format(str(j), str(now)))

                # 两秒钟的时间窗口，避免因为job太多导致错过部分job
                if (j.next_time + timedelta(seconds=2)) >= now >= j.next_time and self.run_batch_id != j.run_batch_id:
                    j.run_batch_id = self.run_batch_id
                    if debug:
                        j._logger(debug).info("put job into queue: {}".format(str(j)))
                    queue.put(j)
                    j.gen_next_time()
                    
            time.sleep(1)

                
def first_runner(queue):
    while True:
        j = queue.get()
        j.run()

def second_runner(queue):
    while True:
        j = queue.get()
        j.run()


crontab = Crontab()

def crontab_run(debug=False):
    freeze_support()
    queue = Queue()
    ps = []
    p1 = Process(target=crontab.loop, args=(queue, debug))
    ps.append(p1)
    p2 = Process(target=first_runner, args=(queue,))
    ps.append(p2)
    p3 = Process(target=second_runner, args=(queue,))
    ps.append(p3)

    for p in ps:
        p.start()
    
    for p in ps:
        p.join()



if __name__ == '__main__':
    crontab_run()
    
